#' @importFrom magrittr %>%
#'@export
#'@title Process downloaded PIT tag data
#'
#'@description The function waits for new datafiles to arrive in folder \code{path} and imports them into the database specified by \code{db}.

#'@param db The pathname to the Microsoft Access database to insert downloaded files into.
#'@param path The pathname to the folder where data files wait to be processed. Usually placed there my Outlook or some other mailer. Use C:/foo/bar/blah Unix forward
#'   slash notaion.
#'@param report_path Full pathname to folder where knitted import reports are to be stored. Defaults to \code{path/../Import records}
#'@param logfile_path Full pathname of file to receive logging information. Defaults to \code{dirname(db)/import_log.txt}.
#'@param log_level One of \code{logging::loglevels}. E.g. \code{ c("NOTSET", "FINEST", "FINER", "FINE", "DEBUG", "INFO", "WARN", "ERROR", "CRITICAL", "FATAL") }
#'   Default is \code{"INFO"}
#'@param compare_full_pathanme (Default \code{FALSE}). If \code{TRUE} then comparing filenames in \code{path} to those already imported into \code{db} (in tblImports)
#'   only considers the filename and ignores the full pathname.
#'@param start_time Time of day to start looking for new files. Useful to avoid wasting time looking for new files during parts of the day when they
#'   are not expected to arrive. Default \code{14:00:00 noon}
#'@param end_time Time of day to stop looking for new files. Useful to avoid wasting time looking for new files during parts of the day when they
#'   are not expected to arrive. Default \code{14:30:00}
#'@param sleep_time Seconds between checks for new files. Default 30 * 60 (ie. 30 minutes).

#'@details
#'
#'@return Doesn't normally return, but -1 on error.
#'@section Author: Dave Fifield
pitdb_process_data_downloads <- function(db = NULL,
                                         path = NULL,
                                         report_path = NULL,
                                         logfile_path = NULL,
                                         log_level = "INFO",
                                         compare_full_pathname = FALSE,
                                         start_time = NULL,
                                         end_time = NULL,
                                         sleep_time = 30 * 60 ){


  logging::basicConfig(level = log_level)
  if (!is.null(logfile_path)){
    logging::addHandler(logging::writeToFile, file = logfile_path, level = log_level)
  } else if (!is.null(db)) {
    logging::addHandler(logging::writeToFile, file = file.path(dirname(db), "import_log.txt"), level='DEBUG')
  } # else only logging to console....

  logging::loginfo("Starting import server....pid = %d", Sys.getpid())

  # Check args
  if (is.null(db)) {
    logging::logerror("Parameter db is missing from pitdb_process_data_downloads.")
    logging::loginfo("Shutting down import server.")
    return(-1)
  }

  if (is.null(path)) {
    logging::logerror("Parameter path is missing from pitdb_process_data_downloads.")
    logging::loginfo("Shutting down import server.")
    return(-1)
  }

  logging::loginfo(sprintf("Using database: %s", db))
  logging::loginfo(sprintf("Reading downloaded data files from: %s", path))

  if(is.null(report_path))
    report_path <- file.path(dirname(path), "Import records")

  # get initial snapshot of existing files in download folder and keep only ".txt" files
  prev <- fileSnapshot(path)
  new_files <- filter_unwanted(rownames(prev$info), db, compare_full_pathname)
  if(!is.character(new_files)) {
    logging::loginfo("Shutting down import server.")
    return(-1)
  }

  logging::logdebug("New files: %s", new_files)

  # Process new_files (if any), sleep, check for new files, process new files, ad nauseum...
  while(TRUE) {
    if (is.character(new_files) && length(new_files) != 0) {
      logging::loginfo("Processing new files: %s", paste(new_files, collapse = ", "))
      pitdb_do_import(db = db, files = file.path(prev$path, new_files, fsep = "\\"), report_path = report_path)
    }

    # sleep until next check
    logging::logdebug("Sleeping until %s (%d secs)...", lubridate::now() + sleep_time, sleep_time)
    Sys.sleep(sleep_time)
    logging::logdebug("Yawn... waking up at %s", lubridate::now())

    # Get a new snapshot and compare
    curr <- fileSnapshot(path)
    new_files <- filter_unwanted(changedFiles(prev, curr)$added, db, compare_full_pathname)
    if(!is.character(new_files)) {
      logging::logerror("Skipping this round of imports. Hoping database will be available next time...")
      next
    }

    # got here so new_files is OK
    logging::logdebug("New files: %s", new_files)
    prev <- curr

    # keep tidy. necessary?
    #gc()
 }
}

filter_unwanted <- function(files, db, compare_full_pathname){
  files <- files[substr(files,nchar(files)-3, nchar(files)) == ".txt"]

  # Open database
  if ((dbh <- pitdb_open(db)) == -1){
    logging::logerror("Failed to open database: %s", db)
    return(-1)
  }

  # Get list of already imported files.
  tblImports <- RODBC::sqlFetch(dbh, "tblImports", as.is = T) %>% ensure_data_is_returned
  pitdb_close(dbh)

  # return vector of new files
  basename(if (compare_full_pathname) {
      dplyr::setdiff(file.path(prev$path, files, fsep = "\\"), tblImports$Filename)
    } else {
      dplyr::setdiff(files, basename(tblImports$Filename))
    }
  )
}
