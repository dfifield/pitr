#'@export
#'@title Process downloaded PIT tag data
#'
#'@description The function waits for new datafiles to arrive in folder \code{path} and imports them into the database specified by \code{db}.

#'@param db The pathname to the Microsoft Access database to insert downloaded files into.
#'@param path The pathname to the folder where data files wait to be processed. Usually placed there my Outlook or some other mailer. Use C:/foo/bar/blah Unix forward
#'   slash notaion.
#'@param report_path Full pathname to folder where knitted import reports are to be stored. Defaults to \code{path/../Import records}
#'@param compare_full_pathanme (Default \code{FALSE}). If \code{TRUE} then comparing filenames in \code{path} to those already imported into \code{db} (in tblImports)
#'   only considers the filename and ignores the full pathname.
#'@param start_time Time of day to start looking for new files. Useful to avoid wasting time looking for new files during parts of the day when they
#'   are not expected to arrive. Default \code{12:00:00 noon}
#'@param end_time Time of day to stop looking for new files. Useful to avoid wasting time looking for new files during parts of the day when they
#'   are not expected to arrive. Default \code{14:00:00}
#'@param sleep_time Seconds between checks for new files. Default 30 * 60 (ie. 30 minutes).

#'@details
#'
#'@return Doesn't normally return.
#'@section Author: Dave Fifield
pitdb_process_data_downloads <- function(db = NULL, path = NULL, report_path = NULL, compare_full_pathname = FALSE, start_time = NULL, end_time = NULL, sleep_time = 30 * 60 ){


  # Check args
  !is.null(db) || stop("Parameter db is missing from pitdb_process_data_downloads.")
  !is.null(path) || stop("Parameter path is missing from pitdb_process_data_downloads.")

  !is.null(report_path) || (report_path <- file.path(dirname(path), "Import records"))

  # Open database
  (dbh <- pitdb_open(db)) != -1 || stop(paste0("Failed to open database: ", db))

  # get initial snapshot of existing files in download folder.
  prev <- fileSnapshot(path)
  files <- rownames(prev$info)

  # Get list of already imported files.
  tblImports <- RODBC::sqlFetch(dbh, "tblImports", as.is = T) %>% ensure_data_is_returned
  pitdb_close(dbh)

  # Check if filename in list of imported files with or without full path.
  new_files <- basename(if (compare_full_pathname) {
      dplyr::setdiff(file.path(prev$path, files), tblImports$Filename)
    } else {
      dplyr::setdiff(files, basename(tblImports$Filename))
    }
  )

  # Process new_files (if any), sleep, check for new files, process new files, ad nauseum...
  while(True) {
    if (length(new_files) != 0) {
      print(paste0("Processing new files: ", paste(new_files, collapse = ",")))
      do_import(files = file.path(prev$path, files), report_path = report_path)
    }

    # sleep for until next check
    Sys.sleep(sleep_time)

    # Get a new snapshot and compare
    curr <- fileSnapshot(path)
    new_files <- changedFiles(prev, curr)$added
    prev <- curr

    # keep tidy
    gc()
 }
}

