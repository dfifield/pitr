#' @importFrom magrittr %>%
#'@export
#'@title Process downloaded PIT tag data
#'
#'@description The function waits for new datafiles to arrive in folder \code{path} and imports them into the database specified by \code{db}.

#'@param db The pathname to the Microsoft Access database to insert downloaded files into.
#'@param path The pathname to the folder where data files wait to be processed. Usually
#'   placed there git pull. Use C:/foo/bar/blah Unix forward slash notaion.
#'@param report_path Full pathname to folder where knitted import reports are to be stored. Defaults to \code{path/../Import records}
#'@param logfile_path Full pathname of file to receive logging information. Defaults to \code{dirname(db)/import_log.txt}.
#'@param log_level One of \code{logging::loglevels}. E.g. \code{ c("NOTSET", "FINEST", "FINER", "FINE", "DEBUG", "INFO", "WARN", "ERROR", "CRITICAL", "FATAL") }
#'   Default is \code{"INFO"}
#'@param compare_full_pathanme (Default \code{FALSE}). If \code{TRUE} then comparing filenames
#'in \code{path} to those already imported into \code{db} (in tblImports)
#'   considers the full pathname, otherwise just considers the filename.
#'@param start_time Time of day to start looking for new files. Useful to avoid wasting time looking for new files during parts of the day when they
#'   are not expected to arrive. NOT IMPLEMENTED.
#'@param end_time Time of day to stop looking for new files. Useful to avoid wasting time looking for new files during parts of the day when they
#'   are not expected to arrive. NOT IMPLEMENTED
#'@param sleep_time If \code{NULL} (default), return after importing any new files.
#'   Otherwise, either number of seconds between checks for new files or a
#'   character string of form "H:M:S" defining the time to wake each day.

#'@details
#'
#'@return Doesn't normally return, but -1 on error.
#'@section Author: Dave Fifield
#'
pitdb_process_data_downloads <- function(db = NULL,
                                         path = NULL,
                                         report_path = NULL,
                                         logfile_path = NULL,
                                         log_level = "INFO",
                                         compare_full_pathname = FALSE,
                                         start_time = NULL,
                                         end_time = NULL,
                                         sleep_time = NULL){


  logging::basicConfig(level = log_level)
  if (!is.null(logfile_path)){
    logging::addHandler(logging::writeToFile, file = logfile_path, level = log_level)
  } else if (!is.null(db)) {
    logging::addHandler(logging::writeToFile, file = file.path(dirname(db),
                                            "import_log.txt"), level='DEBUG')
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

  # Note path could be a vector
  logging::loginfo(sprintf("Reading downloaded data files from: %s",
                           paste(path, collapse = "\n\t")))

  # Need unique since path may contain multiple values of folders to search
  # for new files. It will only work if they all have a common parent in
  # which to place the Import records folder.
  if(is.null(report_path))
    report_path <- unique(file.path(dirname(path), "Import records"))

  if(length(report_path) > 1) {
    logging::logerror(sprintf("Report_path has length %d: (%s)",
                              length(report_path),
                              paste(report_path, collapse = ", ")))
    logging::logerror("All folders in 'path' must have a common parent in which to place the 'Import records' folder.")
    logging::loginfo("Shutting down import server.")
    return(-1)
  }

  # get initial snapshot of existing files in download folder and keep only ".txt" files
  files <- fileSnapshot(path, full.names = TRUE)

  # remove any that have already been imported into the db.
  new_files <- filter_unwanted(rownames(files$info), db, compare_full_pathname)
  if(!is.character(new_files) || length(new_files) == 0) {
    logging::loginfo("No new files to process...Shutting down import server.")
    return(-1)
  }

  logging::logdebug("New files: %s", new_files)

  # Init
  sleep_time_secs <- NA

  # Process new_files (if any), sleep, check for new files, process new files, ad nauseum...
  while(TRUE) {
    if (is.character(new_files) && length(new_files) != 0) {
      logging::loginfo("Processing new files: %s", paste(new_files,
                                                         collapse = ", "))
      # pitdb_do_import wants full pathnames.

      pitdb_do_import(db = db, files = new_files, report_path = report_path)
    }

    # Go around again or just a one-time import?
    if (is.null(sleep_time)) {
      logging::loginfo("No sleep_time set...Shutting down import server.")
      return(-1)
    }

    # Calculate sleep time
    if (is.character(sleep_time)) {
      # In this case sleep_time is of the form "H:M:S" to wake up each day.
      # Need to check and see how long until that time. If difference is negative, then
      # wake tomorrow at that time.
      n <- now()
      normal_wake_time <- as.POSIXct(sprintf("%d:%d:%d", lubridate::year(n),lubridate::month(n),
                                             lubridate::day(n)),
                                     format = "%Y:%m:%d") + lubridate::hms(sleep_time)
      dff <- as.integer(difftime(normal_wake_time, n, units = "secs"))

      if (dff > 0) {
        # coming up today some time
        sleep_time_secs <- dff
      } else {
        # already passed today
        sleep_time_secs <- 86400 + dff
      }
    } else if (is.numeric(sleep_time)) {
      sleep_time_secs <- sleep_time
    } else {
      logging::logerror("sleep_time must be character or numeric.")
      return(-1)
    }

    # Panarnoia check
    if (is.na(sleep_time_secs)) {
      logging::logerror("Sleep_time_secs is NA. Shutting down import server.")
      return(-1)
    }

    # sleep until next check
    logging::logdebug("Sleeping until %s (%d secs)...", lubridate::now() +
                        sleep_time_secs, sleep_time_secs)
    Sys.sleep(sleep_time_secs)
    sleep_time_secs <- NA # reset
    logging::logdebug("Yawn... waking up at %s", lubridate::now())

    # Get a new snapshot and compare.
    # Can't just used changedFiles() b/c import records may have been removed
    # in the db and so we need to re-import those files.
    files <- fileSnapshot(path)
    new_files <- filter_unwanted(row.names(files$info), db, compare_full_pathname)
    if(!is.character(new_files)) {
      logging::logerror("Skipping this round of imports. Hoping database will be available next time...")
      next
    }

    # got here so new_files is OK
    logging::logdebug("New files: %s", new_files)

    # keep tidy. necessary?
    #gc()
 }
}


# Remove any files that have already been imported to db.
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

  # return vector of new files. "files" will always have full pathnames
  if (compare_full_pathname) {
    dplyr::setdiff(files, tblImports$Filename)
  } else {
    # if not comparing full pathnames then look for each filename in the list
    # of already imported files from the db and create a vector of TRUE/FALSE
    # to choose which files to import. in this way we preserve the full
    # pathnames of the returned vector of filenames even though we did the
    # comparison without them.
    lgl_vec <- basename(files) %>%
      purrr::map_lgl(function(looking_for, looking_in) {
        looking_for %>%
          stringr::str_detect(looking_in) %>%
          sum %>%
          magrittr::equals(0)
      }, looking_in = basename(tblImports$Filename))
    files[lgl_vec]
  }
}
