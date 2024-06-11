#' @importFrom magrittr %>%
#'@export
#'@title Load a PIT tag datafile into a database.
#'
#'@description The function will load a datafile downloaded from a PIT tag
#'  monitor board into a Microsoft Access database.
#'@param ch Channel to open database returned from \code{pitdb_open}.
#'@param filename Path to file containing data dumped from a monitor board.
#'@param date_ Date on which the data was downloaded. Normally this is parsed
#'  from the \code{filename} but it can be over-ridden with this argument.Used to find all boards
#'  deployed on this date and find boards not reporting any data. See \code{display_non_reporters}.
#'@param from_date (default = NUll) minimum date to accept in data from file. Data with dates outside the range
#' \code{from_date to  to_date} will not be imported into the database.
#'@param to_date (default = NULL) maximum date to accept in data from file. \code{from_date/to_date} override
#'  \code{limit_to_deploy_dates} if supplied.
#'@param limit_to_deploy_dates (default = \code{TRUE}). Only data within the range of dates that the board was deployed will
#'  be imported. This is usefule to exclude data recorded before or after deployment (e.g. at the office).
#'@param limit_to_known_prefix (defualt \code{FALSE}) If \code{TRUE} only tag IDs with a known prefix (see \code{lkpSpecialTags} in the database)
#'  will be inserted into the database. This is useful to ignore ghost tag reads.
#'  This is likely no longer needed since implementing duty cycle filtering of
#'  the clock signal from MLX90109 chip.
#'@param fetch_type How the data was fetched from the board. Valid values are
#'  "WiFi" and "CableConnect".
#'#'@param ignore_test_board Should data from the test board (normally #1) be ignored. Default = \code{TRUE}.
#'@param test_board_ID The board ID of the test board (normally 1). Used to ignore testing data.
#'@param record_non_reporters (default \code{TRUE}) Should boards not reporting data in file be recorded in database?
#'  Useful to get a list of boards not reporting in via WIFI each day.
#'  Only makes sense when \code{fetch_type} is "WiFi" and gives an error if this is not the case.
#'  Non-reporting boards will be added to tbl_NonReport.
#'@param display_non_reporters Produce warning message for any deployed boards
#'  not reporting data in \code{filename} when \code{record_non_reporters}.
#'@param parse_summary Print a summary of the parsed data file (default = \code{FALSE})
#'@param ignore_insert_errors (default \code{FALSE}). If \code{TRUE}, then ignore errors when attempting to insert records
#'into tag_reads, statuses, uploads, and bad_recs and print a warning instead. If \code{FALSE} then an error message is printed and execution terminates.
#'Setting this to \code{TRUE} can be useful when loading a file downloaded from a board via
#'direct \code{CableConnect} to ensure that no data (previously inserted via "WiFi" downloaded file) were missed.
#'@param compare_full_pathname (default \code{FALSE}) If true then compare entire pathname when checking to see if \code{filename}
#'has been previously imported (wehn comparing against \code{Filename} field in tblImports). Normally only the filename
#'itself is compared (vi \code{basename()}). Normally, (for years >= 2017) each downloaded filename contains a
#'unique timestamp so comparing \code{basename(filename)} is sufficient and desirable since it allows data files
#'to be moved to a new folder and still detect that they have been previously imported. However, in 2016 boards were downloaded
#'(sometimes) multiple times with names like \code{P2_44.txt} (for burrow 2, board 44) and placed in different folders. Setting
#'this option to TRUE will allow for importing of such data.
#'@param verbose (default \code{FALSE}) show inserted data, and warning messages upon attempting to insert a duplicate
#'tag_read, status, upload, or bad_rec record when \code{ignore_insert_errors} is \code{TRUE}.
#'
#'@details This function reads a text file of records that have been extracted
#'  from a monitor board and imports them into the database indicated by
#'  \code{channel}. These files are typically downloaded via direct cable
#'  connection or received from a Raspberry Pi base station.
#'
#'  In most cases \code{filename} will contain info from multiple monitor
#'  boards, e.g., the daily files from a Raspberry Pi base station. In
#'  this case, it is helpful to know which deployed boards failed to send their
#'  data that day. If  \code{detect_non_reporters} = \code{TRUE},
#'  \code{pitdb_load_file} will issue a warning message listing any boards that
#'  are currently deployed (as defined in tblBoardDeploy in the Access database)
#'  but did not provide any data in \code{filename}. The meaning of "currently"
#'  in the last sentence means "on the date that the data was download" which
#'  comes from the filename but which can be overridded by \code{date}.
#'
#'  What it does in the database......
#'
#'  inserts records one by one in case an insert fails, prints warning regarding
#'  failed inserts.
#'
#'  talk about format of filename somewhere
#'
#'  deal with different data formats in 2016 and 2017
#'
#'  deal with conversion of voltages and MCU temperature
#'
#'  prints a summary of what was done (consider how this is done: jsut via print
#'  or does it return a summary object which has a print method...probably just
#'  print it - easier).
#'
#'  Decide what to do about unknown tag IDs (legitimate and ghost reads) since
#'  the database currently requires all tags in tblTagRead table to exist
#'  apriori in tblTags. Should they go in some other table, or just be
#'  discarded. Put them in failed load table.
#'
#'  talk about creation of a tblImports record.
#'
#'  deal with "Bad record" rows
#'
#'@section Sanity checks:
#'
#'  \itemize{ \item detects any board not reporting if
#'  \code{detect_non_reporters} = TRUE. \item issues a warning (and fails to add
#'  record) if an attempt is made to import a record that already exists. \item
#'  issues a warning (and fails to add record) if a tag_read record refers to an
#'  unknown tag ID. This could be either a legitimate tag that has not been
#'  added to the database or a so-called \emph{ghost read}. Ghost reads
#'  were previously generated b/c the noise data coming from the MLX90109 chip when
#'  no tag was present was continuously read and interpreted as a potential EM4102
#'   data stream.
#'  As of rfid board software version 1.8,
#'  Ghost reads are (should be?) entirely eliminated, b/c the data stream is only
#'  treated as valid when the duty cycle is approximately 50% (40%-60% in practice)
#'  indicating that a tag is present in the antenna.
#'
#'  A previous fix as a stop-gap measure, implemented in version 1.1, involved
#'  only recording tags with specific known prefixes (hardcoded into software)
#'   of existing tags.
#'  However, it was still remotely
#'  possible that a ghost read with a known prefix could occur. The number of
#'  reads field is always 1 (??) in a ghost read, but rarely so for a true tag
#'  read. Setting \code{detect_ghost_reads} = \code{TRUE} will issue a warning
#'  for any reads of valid tag IDs where the number of reads is 1. }
#'@return Returns TRUE on success and FALSE on error.
#'@section Author: Dave Fifield
#'
# debugging
# ch <- pitdb_open("C:/Users/fifieldd/Documents/Offline/Projects/Burrow_Logger/Data/Burrow logger master0.1.accdb")
# filename <- "C:\\Users\\fifieldd\\Documents\\Offline\\Projects\\Burrow_Logger\\Data\\Test data\\gipi1_report_2017_08_08_13_00_09_test_board1.txt"
# filename <- "C:\\Users\\fifieldd\\Documents\\Offline\\Projects\\Burrow_Logger\\Data\\2017\\Downloads\\gipi1_report_2017_06_30_13_52_31.txt"
# date_ <- NULL
# fetch_type <- "WiFi"
# ignore_test_board <- TRUE
# test_board_ID <- 1
# display_non_reporters <- TRUE
# detect_ghost_reads <- TRUE
# parse_summary <- FALSE
# verbose <- FALSE

pitdb_load_file <- function(ch = NULL,
                            filename = NULL,
                            date_ = NULL,
                            fetch_type = "WiFi",
                            from_date = NULL,
                            to_date = NULL,
                            limit_to_deploy_dates = TRUE,
                            limit_to_known_prefix = FALSE, # was previously TRUE
                            ignore_test_board = TRUE,
                            test_board_ID = 1,
                            record_non_reporters = TRUE,
                            display_non_reporters = TRUE,
                            parse_summary = FALSE,
                            ignore_insert_errors = FALSE,
                            compare_full_pathname = FALSE,
                            verbose = FALSE,
                            debug = FALSE)
{
  if (debug) browser()

  cat(paste0("\n######################################\nProcessing ", basename(filename), "\n"))

  !is.null(ch) || stop("parameter ch is missing.")
  !is.null(filename) || stop("parameter filename is missing.")
  !(record_non_reporters && fetch_type != "WiFi") || stop("'record_non_reporters' is only valid when fetch_type == 'WiFi'.")
  invisible(ensurer::ensure_that(list(from_date, to_date),
                      (is.null(.[[1]]) && is.null(.[[2]])) ||
                      (is.not.null(.[[1]]) && is.not.null(.[[2]])),
                      err_desc = "from_date and to_date must both either be supplied or NULL" ))

  # Check if file already imported. Rudimentary check which only considers filename
  tblImports <- RODBC::sqlFetch(ch, "tblImports", as.is = T) %>% ensure_data_is_returned

  # Check if filename in list of imported files with or without full path.
  res <- if (compare_full_pathname) {
      dplyr::filter(tblImports, Filename == filename)
    } else {
      dplyr::filter(tblImports, basename(Filename) == basename(filename))
    }

  # Print warning (and quit) if nrow(res) is not 0, ie if the file has already been imported
  ensure_not_already_imported(res) %>% nrow() == 0 || return(FALSE)

  # Parse file and give summary
  dat <- dat_orig <- pitdb_parse_bird_report_file(filename, fetch_type = fetch_type, ignore_test_board = ignore_test_board,
                                      test_board_ID = test_board_ID, verbose = T)

  tot_recs <- purrr::map_if(dat, is.not.null, nrow) %>% unlist
  cat(sprintf("%d rows read from file: \n", tot_recs %>% sum))
  print(tot_recs)

  # Remove data outside specified date range and override limit_to_deploy_dates
  # if from_date and to_date are given. Note we've already made sure both of
  # from_date and to_date are supplied if either of them is.
  if( is.not.null(from_date)) {
    limit_to_deploy_dates = FALSE

    # remove data outside of specified date range
    dat <- dat %>%
      purrr::map_if(.p = is.not.null, .f = function(dt) {dplyr::filter(dt, as.Date(dt$dateTime) >= from_date, as.Date(dt$dateTime) <= to_date)})

    cat(sprintf("%d records retained after date filtering.\n",
                purrr::map_if(dat, is.not.null, nrow) %>% unlist %>% sum))
  }

  # Nullify any table that has no records left
  dat %<>% make_empty_tibbles_null

  # Filter out data outside deployment dates.
  if (limit_to_deploy_dates) {
    strsql <- paste0("SELECT tblBoardDeploy.BoardID, tblBoardDeploy.FromDate, tblBoardDeploy.ToDate ",
        "FROM tblBoardDeploy;")
    depl <- RODBC::sqlQuery(ch, strsql) %>% ensure_data_is_returned

    # filter each of tag_reads, statuses, etc in turn.
    dat <- dat %>% purrr::map_if(is.not.null, per_dataclass_filter, depl)

    # Nullify any table that has no records left
    dat %<>% make_empty_tibbles_null

    # get remaining numbers of records
    rem_recs <- purrr::map_if(dat, is.not.null, nrow) %>% unlist
    cat(sprintf("%d records retained after deployment date filtering:\n",  rem_recs %>% sum))
    print(rem_recs)

    # show what records were rejected.
    num_filt <- sum(tot_recs - rem_recs)
    if (num_filt > 0) {
      cat(sprintf("\nThe following %d records were filtered out due to deployment date filtering:", num_filt))
      diffs <- purrr::map2(dat_orig, dat, function(x, y) dplyr::setdiff(x, y))
      purrr::walk2(diffs, names(diffs), print_recs)
    }
  }

  # Look up fetch_type
  strsql <- paste0("SELECT lkpFetchType.FetchTypeID, lkpFetchType.FetchTypeText FROM lkpFetchType WHERE ",
                   "(((lkpFetchType.FetchTypeText)='", fetch_type, "'));");
  res <- RODBC::sqlQuery(ch, strsql) %>% ensure_data_is_returned %>% ensure_one_row_returned
  fetch_type_ID <- res$FetchTypeID

  #### create a tblImports record ####
  cat("\tCreating tblImports record...")
  strsql <- paste0("INSERT INTO tblImports ( [DateTime], [Filename], [FetchType]) SELECT ",
                            "#", format(lubridate::now(), format = "%Y-%b-%d %H:%M:%S"), "# AS Expr1, ",
                            "'", filename, "' AS Expr2, ",
                            fetch_type_ID, " AS Expr3;")
  res <- RODBC::sqlQuery(ch, strsql) %>% ensure_insert_success
  import_ID <- get_sql_ID(ch)
  cat(paste0("import ID = ", import_ID, "..."))
  cat("done\n")

  # Check if any data left to process
  if(dat %>% purrr::map(is.null) %>% purrr::flatten_lgl() %>% all){
    cat("No data to load. Quiting...\n")
    return(TRUE)
  }

  #### data summary ####
  if (parse_summary)
    dat %>% pitdb_summarize_parsed_file(ch = ch, verbose = F)

  ##### handle non_reporters ####
  handle_non_reporters(ch, import_ID, record_non_reporters, display_non_reporters, date_, dat, filename)

  #################################################
  ####### Insert each type of record from dat #####
  #################################################

  # get special tags
  sp_tags <- RODBC::sqlFetch(ch, "lkpSpecialTags", as.is = T) %>% ensure_data_is_returned

  # create special tag indicators
  test_tags <- dplyr::filter(sp_tags, Typ == "test_tag")$Val
  known_prefix <- stringr::str_c("^", dplyr::filter(sp_tags, Typ == "known_prefix")$Val)
  web_prefix <- dplyr::filter(sp_tags, Typ == "web_prefix")$Val

  ##### Insert tags known to be deployed on birds ----

  cat("\tInserting bird tag reads...")
  if (!is.null(dat$tag_reads)) {

    if (limit_to_known_prefix) {
      orig <- nrow(dat$tag_reads)
      dat$tag_reads <- dat$tag_reads %>%
        dplyr::filter(purrr::map(known_prefix, ~stringr::str_detect(dat$tag_reads$tagID, .)) %>%
                  purrr::reduce(`|`))
      cat(sprintf("Removed %d records with unknown prefix...", orig - nrow(dat$tag_reads)))
    }

    known_tags <- RODBC::sqlFetch(ch, "tblTags", as.is = T) %>% ensure_data_is_returned
    known_tag_reads <- dat$tag_reads %>% dplyr::filter(tagID %in% known_tags$TagID)
    insert_results <- known_tag_reads %>% insert_table_data(ch = ch, whch_table = "tblBirdTagRead", import_ID = import_ID,
                       ignore_errors = ignore_insert_errors, verbose = verbose)
    cat(sprintf("%d inserted/%d rejected\n", sum(insert_results), sum(insert_results == FALSE)))
    if (verbose) print_tibble(known_tag_reads[insert_results,])

    #### Insert web tag reads ----

    cat("\tInserting web tag reads...")
    web_tag_reads <- dat$tag_reads %>% dplyr::filter(substr(tagID, 1, 4) == web_prefix)
    insert_results <- web_tag_reads %>% insert_table_data(ch = ch, whch_table = "tblOtherTagRead", import_ID = import_ID,
                        read_type = "Web", ignore_errors = ignore_insert_errors, verbose = verbose)
    cat(sprintf("%d inserted/%d rejected\n", sum(insert_results), sum(insert_results == FALSE)))
    if (verbose) print_tibble(web_tag_reads[insert_results,])

    #### Insert test tag reads ----

    cat("\tInserting test tag reads...")
    test_tag_reads <- dat$tag_reads %>% dplyr::filter(tagID %in% test_tags)
    insert_results <- test_tag_reads %>% insert_table_data(ch = ch, whch_table = "tblOtherTagRead", import_ID = import_ID,
                      read_type = "Test", ignore_errors = ignore_insert_errors, verbose = verbose)
    cat(sprintf("%d inserted/%d rejected\n", sum(insert_results), sum(insert_results == FALSE)))
    if (verbose) print_tibble(test_tag_reads[insert_results,])

    #### Insert unknown tag reads ----

    cat("\tInserting unknown tag reads...")
    unkn_tag_reads <-
      dat$tag_reads %>%
      dplyr::setdiff(dplyr::bind_rows(known_tag_reads, web_tag_reads, test_tag_reads)) %>%
      dplyr::distinct()
    insert_results <- unkn_tag_reads %>% insert_table_data(ch = ch, whch_table = "tblOtherTagRead", import_ID = import_ID,
                    read_type = "Unknown", ignore_errors = ignore_insert_errors, verbose = verbose)
    cat(sprintf("%d inserted/%d rejected\n", sum(insert_results), sum(insert_results == FALSE)))
    if (verbose) print_tibble(unkn_tag_reads)

    #### Check to make sure all tag_reads were used ----

    dat$tag_reads %>%
      dplyr::setdiff(dplyr::bind_rows(known_tag_reads, web_tag_reads, test_tag_reads, unkn_tag_reads)) %>% warn_tag_reads_not_inserted
  } else {
    cat("no tag data to insert\n")
  }

  ##### Insert statuses ####

    cat("\tInserting statuses...")
  if(!is.null(dat$statuses)){
    insert_results <- dat$statuses %>%
      insert_table_data(ch = ch, whch_table = "tblStatus", import_ID = import_ID,
          ignore_errors = ignore_insert_errors, verbose = verbose)
    cat(sprintf("%d inserted/%d rejected\n", sum(insert_results),
                sum(insert_results == FALSE)))
    if (verbose) print_tibble(dat$statuses[insert_results,])
  } else {
    cat("no status data to insert\n")
  }

  ##### Insert uploads ####

  cat("\tInserting uploads...")
  if(!is.null(dat$uploads)){
    insert_results <- dat$uploads %>% insert_table_data(ch = ch, whch_table = "tblUpload", import_ID = import_ID,
                       ignore_errors = ignore_insert_errors, verbose = verbose)
    cat(sprintf("%d inserted/%d rejected\n", sum(insert_results), sum(insert_results == FALSE)))
    if (verbose) print_tibble(dat$uploads[insert_results,])
  } else {
    cat("no upload data to insert\n")
  }

  ##### Insert bad records ####

  cat("\tInserting bad records...")
  if(!is.null(dat$bad_recs)){
    insert_results <- dat$bad_recs %>% insert_table_data(ch = ch, whch_table = "tblBadRecord", import_ID = import_ID,
                       ignore_errors = ignore_insert_errors, verbose = verbose)
    cat(sprintf("%d inserted/%d rejected\n", sum(insert_results), sum(insert_results == FALSE)))
    if (verbose) print_tibble(dat$bad_recs[insert_results,])
  } else {
    cat("no bad records to insert\n")
  }

  cat("Done.\n")
  TRUE
}


#'@export
#'@title Load multiple PIT tag datafiles into a database.
#'
#'@description Repeatedly calls \link{pitdb_load_file} to load multiple files
#'  into a Microsoft Access database.
#'@param ch (required) Channel to open database returned from \link{pitdb_open}.
#'@param filenames (required) Character vector of full pathnames to files
#'  containing data from a monitor board.
#'@param ... Other args passed to \link{pitdb_load_file}
#'
#'@details This calls \link{pitdb_load_file} once for each element of \code{filenames}
#'  in order to read a text file of records that have been extracted
#'  from a monitor board and import them into the database indicated by
#'  \code{channel}. These files are typically downloaded via direct cable
#'  connection or received from a Raspberry Pi base station.
#'
#'@return Nothing
#'@section Author: Dave Fifield
#'
pitdb_load_files <- function(ch, filenames, ...){
  filenames %>%
    purrr::walk(function(.x) {pitdb_load_file(ch = ch, filename = .x, ...)})
}
