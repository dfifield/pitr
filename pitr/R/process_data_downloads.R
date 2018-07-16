#'@export
#'@title Process downloaded PIT tag data
#'
#'@description The function waits for new datafiles to arrive in folder \code{path} and then executes and then calls other functions to insert data
#'into database
#'@param db The pathname to the Microsoft Access database to insert downloaded files into.
#'@param path The pathname for the folder where files will magically appear waiting to be processed. Usually placed there my Outlook or some other mailer.
#'@param start_time Time of day to start looking for new files. Useful to avoid wasting time looking for new files during parts of the day when they
#'   are not expected to arrive. Default \code{12:00:00 noon}
#'@param end_time Time of day to stop looking for new files. Useful to avoid wasting time looking for new files during parts of the day when they
#'   are not expected to arrive. Default \code{14:00:00}
#'@param sleep_time Minutes between checks for new files. Default 10 minutes.

#'@details
#'
#'@return Doesn't normally return.
#'@section Author: Dave Fifield
#'
#'
pitdb_process_data_downloads <- function(db = NULL, path = NULL, start_time = XXXX, end_time = XXXX, sleep_time = 10 ){

  # check args

  # Should ch be passed in or opened in here from a given path. Perhaps there should be a pitdb_open_db()?
  !is.null(db) || stop("Parameter db is missing from pitdb_process_data_downloads.")
  !is.null(path) || stop("Parameter path is missing from pitdb_process_data_downloads.")


  # endlessly process files.
  while(True) {
    # any new files to process? How to determine?
    #
    # build a list of filenames and modification times of all files ever processed. ... no, that's just duplicating what's in tblImports.
    # could just check each file to see if it's in tblImports. Grab a new copy of tblImports each time we wake up and go from there...
    # # Check if file already imorted. Rudimentary check which only considers filename

    # here's some similar code from load_file.R....
    # tblImports <- RODBC::sqlFetch(ch, "tblImports", as.is = T) %>%
    #   ensure_data_is_returned
    #
    # # Check if filename in list of imported files with or without full path.
    # res <- if (compare_full_pathname) {
    #     dplyr::filter(tblImports, Filename == filename)
    #   } else {
    #     dplyr::filter(tblImports, basename(Filename) == basename(filename))
    #   }
    #
    # # Print warning (and quit) if nrow(res) is not 0, ie if the file has already been imported
    # ensure_not_already_imported(res) %>% nrow() == 0 || return(FALSE)

    if (any_new_files to process) {
      # duplicate functionality of ...\software\data processing\R\Process download data.R that is currently called by Outlook from VBA.
    } else {
      # sleep for until next check
    }
  }
}
