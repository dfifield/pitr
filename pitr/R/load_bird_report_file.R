
#'@export
#'@title Parse data from a Raspberry Pi report file
#'
#'@description The function will parse a bird report datafile.
#'
#'@param filename Path to file containing data dumped from a monitor board.
#'@param fetch_type Either "WiFi" or "CableConnect". This is really a proxy for
#'  the type of info to expect in the file since "WiFi" downloads have fetch
#'  date and times and WiFi module ids in the data file, whereas "CableConnect"
#'  files have fetch date and time encoded in the filename. The date encoded in
#'  a "WiFi" downloaded file refers to the time it was received at the RPi base
#'  station.
#'@param ignore_test_board Should data from the test board (normally #1) be
#'  ignored. Default = \code{TRUE}.
#'@param test_board_ID The board ID of the test board (normally 1). Used to
#'  ignore testing data.
#'@param verbose If \code{TRUE}, print a banner for each processed file.
#'
#'@details This function reads a text file of records that have been extracted
#'  from a monitor board. These files are typically downloaded via direct cable
#'  connection or via email from a Raspberry Pi base station.
#'
#'  In most cases \code{filename} will contain info from multiple monitor
#'  boards, e.g., the daily files emailed from a Raspberry Pi base station.
#'
#'  Does not attempt to further split tag_reads into normal reads, webserver
#'  tags, ghost tags, etc. since this requires info on known tags/prefixes, etc
#'  from the database and it is desirable to have this function work whether
#'  database is available or not.
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
#'  deal with "Bad record" rows
#'
#'@return Returns a list with elements \code{tag_reads, statuses, uploads,
#'  bad_recs}. Individual elements of the list will be null if \code{filename}
#'  doesn't contain any records of that type.
#'@section Author: Dave Fifield
#'
pitdb_parse_bird_report_file <- function(filename, fetch_type, ignore_test_board = TRUE, test_board_ID = 1,verbose = FALSE) {

  # init
  retval <- list(tag_reads = NULL, statuses = NULL, uploads = NULL, bad_recs = NULL)

  # If file is from direct dowload from CableConnect, then parse download date
  # and time as FDate and FTime (fetch date and time). Fetch times are encoded in each data line for "WiFi" fetch_type.
  if(fetch_type == "CableConnect") {
    parts <- stringr::str_split(basename(filename), "_")
    fetchDateTime <- as.POSIXct(strptime(paste0(parts[[1]][2], " ", parts[[1]][3]), format = "%Y%m%d %H%M%S"))
  }

  dat <- as.data.frame(readLines(con = filename), stringsAsFactors = F)
  names(dat) <- "string"

  # Any lines in file? Nothing else to do?
  if (nrow(dat) == 0)
    return(retval)

  # Detect no updates. This is a report file with no data. Should only happen with "WIFI" downloads.
  if (nrow(dat) == 1 && (grep("no updates", dat[1,]) == 1)){
    if (verbose) cat('File contains "no updates".\n')
    return(retval)
  }

  # Detect bad records. These were due to a bug in mqtt upload that send invalid data. Fixed in V1.2 of rfid_logger.
  bad_recs_idx <- grep("Bad record", dat$string)
  if (length(bad_recs_idx) != 0) {
    bad_recs <- tidyr::separate(dplyr::slice(dat, bad_recs_idx), "string", sep = "[ ]+",
                                into = c("FDate", "FTime", "WiFiID",  "BoardID"), extra = "drop", convert = T)
    bad_recs$fetchDateTime <- as.POSIXct(strptime(paste0(bad_recs$FDate, " ", bad_recs$FTime), format = "%Y-%m-%d %H:%M:%S"))
    bad_recs <- dplyr::select(bad_recs, -FDate, -FTime)
    retval$bad_recs <- bad_recs
  }

  # Extract statuses
  statuses <- as.data.frame(sub(" S ", " ", dat[,1]))
  names(statuses) <- "string"

  # note this line figures out which rows to keep in the slice by looking for
  # the initial "S " in dat, since it has already been removed from statuses.
  # There was a reason for this but I can't remember now....
  statuses <- tidyr::separate(dplyr::slice(statuses, grep(" S ", dat$string)), "string",
                    into = switch(fetch_type, WiFi = c("FDate", "FTime", "WiFiID",  "BoardID", "Date", "Time", "VCoin", "VIn", "MCUTemp", "Freq"),
                      CableConnect = c("BoardID", "Date", "Time", "VCoin", "VIn", "MCUTemp", "Freq"),
                      "error"),
                   sep = "[ ]+",
                   convert = T)
  if(nrow(statuses) > 0) {
    statuses$dateTime <- as.POSIXct(strptime(paste0(statuses$Date, " ", statuses$Time), format = "%Y-%m-%d %H:%M:%S"))
    statuses$Date <- NULL
    statuses$Time <- NULL
    if(fetch_type == "WiFi") {
      statuses$fetchDateTime <- as.POSIXct(strptime(paste0(statuses$FDate, " ", statuses$FTime), format = "%Y-%m-%d %H:%M:%S"))
      statuses$FDate <- NULL
      statuses$FTime <- NULL
    } else {
      statuses$fetchDateTime <- fetchDateTime
    }
    retval$statuses <- statuses
  }

  #extract tag reads
  tag_reads <- as.data.frame(sub(" T ", " ", dat[,1]))
  names(tag_reads) <- "string"

  tag_reads <- tidyr::separate(dplyr::slice(tag_reads, grep(" T ", dat$string)), "string",
                     into =  switch(fetch_type,
                                    WiFi = c("FDate", "FTime", "WiFiID",  "BoardID", "Date", "Time", "numread", "tagID"),
                                    CableConnect = c("BoardID", "Date", "Time", "numread", "tagID"),
                                    "error"),
                     sep = "[ ]+",
                     convert = T)
  if(nrow(tag_reads) > 0) {
    tag_reads$dateTime <- as.POSIXct(strptime(paste0(tag_reads$Date, " ", tag_reads$Time), format = "%Y-%m-%d %H:%M:%S"))
    tag_reads$Date <- NULL
    tag_reads$Time <- NULL
    if(fetch_type == "WiFi") {
      tag_reads$fetchDateTime <- as.POSIXct(strptime(paste0(tag_reads$FDate, " ", tag_reads$FTime), format = "%Y-%m-%d %H:%M:%S"))
      tag_reads$FDate <- NULL
      tag_reads$FTime <- NULL
    } else {
      tag_reads$fetchDateTime <- fetchDateTime
    }
    retval$tag_reads <- tag_reads
  }

  #extract mark_upload records
  uploads <- as.data.frame(sub(" M ", " ", dat[,1]))
  names(uploads) <- "string"

  uploads <- tidyr::separate(dplyr::slice(uploads, grep(" M ", dat$string)), "string",
                     into =  switch(fetch_type,
                                    WiFi = c("FDate", "FTime", "WiFiID",  "BoardID", "Date", "Time", "prev_index"),
                                    CableConnect = c("BoardID", "Date", "Time", "prev_index"),
                                    "error"),
                     sep = "[  ]+",
                     convert = T)
  if (nrow(uploads) > 0){
    uploads$dateTime <- as.POSIXct(strptime(paste0(uploads$Date, " ", uploads$Time), format = "%Y-%m-%d %H:%M:%S"))
    uploads$Date <- NULL
    uploads$Time <- NULL
    if(fetch_type == "WiFi") {
      uploads$fetchDateTime <- as.POSIXct(strptime(paste0(uploads$FDate, " ", uploads$FTime), format = "%Y-%m-%d %H:%M:%S"))
      uploads$FDate <- NULL
      uploads$FTime <- NULL
    } else {
      uploads$fetchDateTime <- fetchDateTime
    }
    retval$uploads <- uploads
  }


  # Optinally remove data coming from the test board. This prevents accidental
  # insertion of info from the test board that was in the RPi dbase when it was
  # deployed in the field.
  if (ignore_test_board) {
    origlen <- retval %>% purrr::map(nrow) # original counts of rows in each dataframe
    retval <- retval %>% purrr::map_if(is.not.null, ~ dplyr::filter(., BoardID != test_board_ID)) # remove rows from test board
    newlen <- retval %>% purrr::map(nrow) # new counts of rows

    # Print message for any where rows were removed
    purrr::map2(origlen, newlen, ~ .x - .y) %>% # get difference in counts
      purrr::keep(. != 0) %>% # keep ones where count is differnt
      purrr::walk2(names(.), ~ warning(paste0("Warning: ", .x, " ", .y, " rows ignored from test board ", test_board_ID,"\n"),
                                       immediate. = TRUE))
  }


  # return value
  retval
}


#'@export
#'@title Print a summary of data parsed from a Raspberry Pi report file
#'
#'@description The function summarizes data parsed from a bird report datafile.
#'
#'@param dat List (typically returned from \code{pitdb_parse_bird_report_file})
#'  containing elements \code{tag_reads, statuses, uploads, bad_recs}.
#'@param ch  an open connection to the database. If not null, tag_read records are further categorized
#'  as known_tags, webserver tags, test tags, and unknown tags.
#'@param verbose If \code{TRUE}, print details of each record.
#'
#'@details This function takes the output from \code{pitdb_parse_bird_report_file}
#'  and summarizes the numbers of each type of info contained in the parsed file.
#'
#'@section Author: Dave Fifield
#'
pitdb_summarize_parsed_file <- function(dat, ch = NULL, verbose = FALSE){

  ##### Overall summary ####
  boards <- unique(unlist(dat %>% purrr::discard(is.null) %>% purrr::map("BoardID")))

  if (is.null(boards)) {
    cat("No data to summarize.")
    return()
  }

  cat(paste0(length(boards), " boards with data: ", paste0(boards <- boards[order(boards)], collapse = ", "), "\n"))

  #####  All tag_read records #####
  if (!is.null(dat$tag_reads)) {
    tags <- unique(dat$tag_reads$tagID)
    tags <- tags[order(tags)]
    boards <- unique(dat$tag_reads$BoardID)
    boards <- boards[order(boards)]

    # extract tags with one detection as they passed through the coil. This is
    # unusual and close to non-detection so I'm curious about them.
    one_reads <- dplyr::filter(dat$tag_reads, numread == 1)
    if(nrow(one_reads) > 0){
      one_read_tags <- unique(one_reads$tagID)
      one_read_tags <- one_read_tags[order(one_read_tags)]
    }

    cat(paste0(nrow(dat$tag_reads), " tag reads from ", length(tags), " tags"))
    print(table(dat$tag_reads$tagID))

    cat(paste0("\n\tfrom ", length(boards), " boards (",
        paste0(boards, collapse = ", "),  ")\n\tspanning dates ",
        paste0(range(dat$tag_reads$dateTime), collapse = " to "), "\n",
        "\t", nrow(one_reads), " tag records involved a single read of the tag."))
    print(table(one_reads$tagID))
    if (verbose) {
      cat("Tag read records:")
      dat$tag_reads %>% print(n = nrow(.))
    }

    ######## Categorize tag reads #####
    # Dig deeper and chategorize tag reads according to info in the database.
    if (!is.null(ch)) {
      sp_tags <- RODBC::sqlFetch(ch, "lkpSpecialTags", as.is = T)

      # Split out special tags
      test_tags <- dplyr::filter(sp_tags, Typ == "test_tag")$Val
      known_prefix <- dplyr::filter(sp_tags, Typ == "known_prefix")$Val
      web_prefix <- dplyr::filter(sp_tags, Typ == "web_prefix")$Val

      # get known tags - ones that have been deployed on birds
      known_tags <- RODBC::sqlFetch(ch, "tblTags", as.is = T)

      ################### Known tag reads #######
      known_tag_reads <- dplyr::filter(dat$tag_reads,  tagID %in% known_tags$TagID)

      if (nrow(known_tag_reads) > 0) {
        tags <- unique(known_tag_reads$tagID)
        tags <- tags[order(tags)]
        boards <- unique(known_tag_reads$BoardID)
        boards <- boards[order(boards)]

        # do same as above for all tag reqads
        one_reads <- dplyr::filter(known_tag_reads, numread == 1)
        if(nrow(one_reads) > 0){
          one_read_tags <- unique(one_reads$tagID)
          one_read_tags <- one_read_tags[order(one_read_tags)]
        }

        cat(paste0("\n\t", nrow(known_tag_reads), " of these reads were from known tags, n = ", length(tags), " tags"))
        print(table(known_tag_reads$tagID))

        cat(paste0("\n\tfrom ", length(boards), " boards (",
            paste0(boards, collapse = ", "),  ")\n\tspanning dates ",
            paste0(range(known_tag_reads$dateTime), collapse = " to "), "\n",
            "\t", nrow(one_reads), " known tag records involved a single read of the tag."))
        print(table(one_reads$tagID))
        if (verbose) {
          cat("Known tag read records:")
          known_tag_reads %>% print(n = nrow(.))
        }
      } else {
        cat("0 known-tag reads.")
      }

      ################### Webserver tag reads #######
      web <- dplyr::filter(dat$tag_reads, substr(tagID, 1, 4) == web_prefix)
      if (nrow(web) > 0) {
        cat(paste0("\n", nrow(web), " webserver mode tags\n"))
        print(table(web$tagID))
      } else {
        cat("\n0 webserver tag reads.")
      }


      ################### test tag reads #######
      test <- dplyr::filter(dat$tag_reads, tagID %in% test_tags)
      if (nrow(test) > 0) {
        cat(paste0("\n", nrow(test), " test tags\n"))
        print(table(test$tagID))
      } else {
        cat("\n0 test tag reads.")
      }

      ################### unknown tag reads #######
      ukn <- dat$tag_reads %>%
        dplyr::setdiff(dplyr::bind_rows(known_tag_reads, web, test)) %>%
        dplyr::distinct()
      if (nrow(ukn) > 0) {
        tags <- unique(ukn$tagID)
        tags <- tags[order(tags)]
        boards <- unique(ukn$BoardID)
        boards <- boards[order(boards)]

        cat(paste0("\n", nrow(ukn), " unknown tag reads from ", length(tags)," tags"))
        print(table(ukn$tagID))
        cat(paste0("\n\tfrom ", length(boards), " boards (",
            paste0(boards, collapse = ", "),  ")\n\tspanning dates ",
            paste0(range(ukn$dateTime), collapse = " to "), "\n"))

        if (verbose) {
          ukn %>% print(n = nrow(.))
          cat("Rows where num_read == 1 may be ghost reads.")
        }
      } else {
        cat("\n0 unknown tag reads.")
      }
    } # end tag categorization
  } else {
    cat("No tag_read records.")
  }


  #### status entries #####
  if (!is.null(dat$statuses)) {
    boards <- unique(dat$statuses$BoardID)
    boards <- boards[order(boards)]
    cat(paste0("\n\n", nrow(dat$statuses), " status entries from ", length(boards), " boards (",
        paste0(boards, collapse = ", "), ")\n\tspanning dates ",
        paste0(range(dat$statuses$dateTime), collapse = " to "), "\n"))
    if (verbose) dat$statuses %>% print(n = nrow(.))
  } else cat("\nNo status records.")


  ##### uploads #####
  if (!is.null(dat$uploads)) {
    boards <- unique(dat$uploads$BoardID)
    boards <- boards[order(boards)]
    cat(paste0("\n", nrow(dat$uploads), " upload entries from ", length(boards), " boards (",
        paste0(boards, collapse = ", "), ")\n\tspanning dates ",
        paste0(range(dat$uploads$dateTime), collapse = " to "), "\n"))
    if (verbose) dat$uploads %>% print(n = nrow(.))
  } else cat("\nNo uploads")

  #### Bad records #####
  if (!is.null(dat$bad_recs)) {
    boards <- unique(dat$bad_recs$BoardID)
    boards <- boards[order(boards)]
    cat(paste0("\n", nrow(dat$bad_recs), " bad records from ", length(boards), " boards (",
        paste0(boards, collapse = ", "), ")\n\tspanning fetch dates ",
        paste0(range(dat$bad_recs$fetchDateTime), collapse = " to "), "\n"))
    if (verbose) dat$bad_recs %>% print(n = nrow(.))
  } else cat("\nNo bad records")

  invisible(dat)
}



#'@export
#'@title Load a PIT tag datafile into a database.
#'
#'@description The function will load a datafile downloaded from a PIT tag
#'  monitor board into a Microsoft Access database.
#'@param ch Channel to open database returned from \code{pitdb.open}.
#'@param filename Path to file containing data dumped from a monitor board.
#'@param date_ Date on which the data was downloaded. Normally this is parsed
#'  from the \code{filename} but it can be over-ridden with this argument.Used to find all boards
#'  deployed on this date and find boards not reporting any data. See \code{display_non_reporters}.
#'@param fetch_type How the data was fetched from the board. Valid values are
#'  "WiFi" and "CableConnect".
#'@param ignore_test_board Should data from the test board (normally #1) be ignored. Default = \code{TRUE}.
#'@param test_board_ID The board ID of the test board (normally 1). Used to ignore testing data.
#'@param record_non_reporters (default \code{TRUE}) Should boards not reporting data in file be recorded in database?
#'  Only makes sense when \code{fetch_type} is "WiFi" and gives an error if this is not the case.
#'  Non-reporting boards will be added to tbl_NonReport.
#'@param display_non_reporters Produce warning message for any deployed boards
#'  not reporting data in \code{filename} when \code{record_non_reporters}.
#'@param parse_summary Print a summary of the parsed data file (default = \code{FALSE})
#'@param ignore_insert_errors (default \code{FALSE}). If \code{TRUE}, then ignore errors when attempting to insert records
#'into tag_reads, statuses, uploads, and bad_recs. This can be useful when loading a file downloaded from a board via
#'direct \code{CableConnect} to ensure that no data (previously inserted via "WiFi" downloaded file) were missed.
#'@param verbose (default \code{FALSE}) show inserted data when insert_summary is \code{TRUE}, and warning messages upon
#'attempting to insert a duplicate tag_read, status, upload, or bad_rec record when \code{ignore_insert_errors} is \code{TRUE}.
#'
#'@details This function reads a text file of records that have been extracted
#'  from a monitor board and imports them into the database indicated by
#'  \code{channel}. These files are typically downloaded via direct cable
#'  connection or via email from a Raspberry Pi base station.
#'
#'  In most cases \code{filename} will contain info from multiple monitor
#'  boards, e.g., the daily files emailed from a Raspberry Pi base station. In
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
#'  added to the database or a so-called \emph{ghost read}. Ghost reads are
#'  almost entirely eliminated as of rfid board software version 1.1, since only
#'  tags with known prefixes (compiled in) are recorded. However, it is remotely
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

pitdb_load_file <- function(ch = NULL, filename = NULL, date_ = NULL, fetch_type = "WiFi", ignore_test_board = TRUE, test_board_ID = 1,
                            record_non_reporters = TRUE, display_non_reporters = TRUE, parse_summary = FALSE, ignore_insert_errors = FALSE,
                            verbose = FALSE){

  cat(paste0("\n######################################\nProcessing ", basename(filename), "\n"))

  !is.null(ch) || stop("parameter ch is missing.")
  !is.null(filename) || stop("parameter filename is missing.")
  !(record_non_reporters && fetch_type != "WiFi") || stop("'record_non_reporters' is only valid when fetch_type == 'WiFi'.")

  # Check if file already imorted. Rudimentary check which only considers filename
  RODBC::sqlFetch(ch, "tblImports", as.is = T) %>%
    ensure_data_is_returned %>%
    dplyr::filter(basename(Filename) == basename(filename)) %>%
    ensure_not_already_imported %>%
    nrow() == 0 || return()

  # Parse file and give summary
  dat <- pitdb_parse_bird_report_file(filename, fetch_type = fetch_type, ignore_test_board = ignore_test_board,
                                      test_board_ID = test_board_ID, verbose = T)

  # Check if any data to process
  if(dat %>% purrr::map(is.null) %>% purrr::flatten_lgl() %>% all){
    cat("No data in file to load. Qutting...\n")
    return(TRUE)
  }

  if(verbose) cat(sprintf("%d records read.\n", nrow(dat)))

  #### data summary ####
  if (parse_summary)
    dat %>% pitdb_summarize_parsed_file(ch = ch, verbose = F)

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
  cat("done\n")

  # get tblImport record ID
  import_ID <- get_sql_ID(ch)

  ##### handle non_reporters ####
  # This only makes sense to do for datafiles from base stations (ie. fetch_type == WiFi).
  # this is ensured by check at start of function.
  if (record_non_reporters) {
    # get fetch date
    date_ <- date_ %||% parse_date_from_string(filename)

    # try to figure out what plot this data comes from
    plotID <- parse_plot_from_name(filename)

    # get all boards active in this plot on this date
    strsql <- paste0("SELECT tblPlot.PlotID, tblBoardDeploy.BoardID, tblBoardDeploy.BurrowID, ",
                    " tblBoardDeploy.FromDate, tblBoardDeploy.ToDate FROM tblPlot INNER JOIN ",
                    " (tblBurrow INNER JOIN (tblBoard INNER JOIN tblBoardDeploy ON tblBoard.BoardID ",
                    " = tblBoardDeploy.BoardID) ON tblBurrow.BurrowID = tblBoardDeploy.BurrowID) ON ",
                    " tblPlot.PlotID = tblBurrow.Plot ",
                    " WHERE (((tblPlot.PlotID)=", plotID, ") AND ",
                    " ((tblBoardDeploy.FromDate)<=#", format(date_, format = "%Y-%b-%d"), "#) AND ",
                    " ((tblBoardDeploy.ToDate)>=#", format(date_, format = "%Y-%b-%d"), "# Or (tblBoardDeploy.ToDate) Is Null));")
    all_plot_brds <- RODBC::sqlQuery(ch, strsql) %>% ensure_data_is_returned
    active_brds <- all_plot_brds$BoardID %>% unique %>% ensurer::ensure_that(length(.) != 0, err_desc = "No active boards on that date!")

    # get all boards reporting and non-reporting
    reporting <- dat %>%
      purrr::map("BoardID") %>%
      purrr::flatten_int() %>%
      unique %>%
      sort
    non_report <- dplyr::setdiff(active_brds, reporting) %>%
      sort

    # deal with non-reporting boards
    if (length(non_report) != 0){
      # display message if any missing boards
      if(display_non_reporters){
        dplyr::filter(all_plot_brds, BoardID %in% non_report) %>%
          dplyr::mutate(brd_bur = paste0(.$BoardID, " (", .$BurrowID, ")")) %>%
          `[[`("brd_bur") %>% # there3's gotta be a better way
          paste0(collapse = ", ") %>%
          warn_non_reporting
      }

      # Insert a record into tblNonReport
      cat("\tInserting non-reporting board records...")
      non_report  %>% purrr::walk(function(brd) {
        strsql <- paste0("INSERT INTO tblNonReport ( [BoardID], [Date_], [ImportID]) SELECT ",
                        brd, " AS Expr1, ",
                        "#", format(date_, format = "%Y-%b-%d"), "# AS Expr2, ",
                        import_ID, " AS Expr3;")
        res <- RODBC::sqlQuery(ch, strsql) %>% ensure_insert_success
      })
      cat(sprintf("%d inserted\n", length(non_report)))
    }
  }

  ####### Insert each type of record from dat #####

  # get special tags
  sp_tags <- RODBC::sqlFetch(ch, "lkpSpecialTags", as.is = T) %>% ensure_data_is_returned

  # create special tag indicators
  test_tags <- dplyr::filter(sp_tags, Typ == "test_tag")$Val
  known_prefix <- dplyr::filter(sp_tags, Typ == "known_prefix")$Val
  web_prefix <- dplyr::filter(sp_tags, Typ == "web_prefix")$Val

  ##### Insert tags known to be deployed on birds ----
  cat("\tInserting bird tag reads...")
  if (!is.null(dat$tag_reads)) {
    known_tags <- RODBC::sqlFetch(ch, "tblTags", as.is = T) %>% ensure_data_is_returned
    known_tag_reads <- dat$tag_reads %>%
      dplyr::filter(tagID %in% known_tags$TagID) %>%
      insert_tag_reads(ch = ch, whch_table = "tblBirdTagRead", import_ID = import_ID,
                       ignore_errors = ignore_insert_errors, verbose = verbose)
    cat(sprintf("%d inserted\n", nrow(known_tag_reads)))
    if (verbose) print_tibble(known_tag_reads)

    #### Insert web tag reads ----
    cat("\tInserting web tag reads...")
    web_tag_reads <- dat$tag_reads %>%
      dplyr::filter(substr(tagID, 1, 4) == web_prefix) %>%
      insert_tag_reads(ch = ch, whch_table = "tblOtherTagRead", import_ID = import_ID, read_type = "Web",
                       ignore_errors = ignore_insert_errors, verbose = verbose)
    cat(sprintf("%d inserted\n", nrow(web_tag_reads)))
    if (verbose) print_tibble(web_tag_reads)

    #### Insert test tag reads ----
    cat("\tInserting test tag reads...")
    test_tag_reads <- dat$tag_reads %>%
      dplyr::filter(tagID %in% test_tags) %>%
      insert_tag_reads(ch = ch, whch_table = "tblOtherTagRead", import_ID = import_ID, read_type = "Test",
                       ignore_errors = ignore_insert_errors, verbose = verbose)
    cat(sprintf("%d inserted\n", nrow(test_tag_reads)))
    if (verbose) print_tibble(test_tag_reads)

    #### Insert unknown tag reads ----
    cat("\tInserting unknown tag reads...")
    unkn_tag_reads <-
      dat$tag_reads %>%
      dplyr::setdiff(dplyr::bind_rows(known_tag_reads, web_tag_reads, test_tag_reads)) %>%
      dplyr::distinct() %>%
      insert_tag_reads(ch = ch, whch_table = "tblOtherTagRead", import_ID = import_ID, read_type = "Unknown",
                       ignore_errors = ignore_insert_errors, verbose = verbose)
    cat(sprintf("%d inserted\n", nrow(unkn_tag_reads)))
    if (verbose) print_tibble(unkn_tag_reads)

    #### Check to make sure all tag_reads were used ----
    dat$tag_reads %>%
      dplyr::setdiff(dplyr::bind_rows(known_tag_reads, web_tag_reads, test_tag_reads, unkn_tag_reads)) %>% warn_tag_reads_not_inserted
  } else {
    cat("no tag data to insert\n")
  }

  ##### Insert statuses ####
  # Empirical value derived from measuring voltage and observing value of vin at same time
  VIn_to_volts <- 0.013342
  VCoin_to_volts <- 0.001881
  MCUTemp_to_temp <- 1 # XXX havent figure out how to convert yet

  cat("\tInserting statuses...")
  if(!is.null(dat$statuses)){
    dat$statuses %>%
      tibble::rowid_to_column() %>%
      split(.$rowid) %>%
      purrr::walk(function(dt) {
      strsql <- paste0("INSERT INTO tblStatus ( [BoardID], [DateTime], [FetchDateTime], [WIFIID], [Freq], ",
                        "[Vin], [VCoin], [MCUTemp], [ImportID] ) SELECT ",
                        dt$BoardID, " AS Expr1, ",
                        "#", format(dt$dateTime, format = "%Y-%b-%d %H:%M:%S"), "# AS Expr2, ",
                        "#", format(dt$fetchDateTime, format = "%Y-%b-%d %H:%M:%S"), "# AS Expr3, ",
                        "'", dt$WiFiID, "' AS Expr4, ",
                        dt$Freq, " AS Expr5, ",
                        round(dt$VIn * VIn_to_volts, 2), " AS Expr6, ",
                        round(dt$VCoin * VCoin_to_volts, 2), " AS Expr7, ",
                        round(dt$MCUTemp * MCUTemp_to_temp, 1), " AS Expr8, ",
                        import_ID, " AS Expr9;")
      res <- RODBC::sqlQuery(ch, strsql)

      # process errors
      if(!ignore_insert_errors)
        res %>% ensure_insert_success
      else if (verbose)
        res %>% warn_insert_success
    })
    cat(sprintf("%d inserted\n", nrow(dat$statuses)))
    if (verbose) print_tibble(dat$statuses)
  } else {
    cat("no status data to insert\n")
  }

  ##### Insert uploads ####
  cat("\tInserting uploads...")
  if(!is.null(dat$uploads)){
    dat$uploads %>%
      tibble::rowid_to_column() %>%
      split(.$rowid) %>%
      purrr::walk(function(dt) {
      strsql <- paste0("INSERT INTO tblUpload ( [BoardID], [DateTime], [FetchDateTime], [WIFIID], [PrevIndex], [ImportID] ) SELECT ",
                        dt$BoardID, " AS Expr1, ",
                        "#", format(dt$dateTime, format = "%Y-%b-%d %H:%M:%S"), "# AS Expr2, ",
                        "#", format(dt$fetchDateTime, format = "%Y-%b-%d %H:%M:%S"), "# AS Expr3, ",
                        "'", dt$WiFiID, "' AS Expr4, ",
                        dt$prev_index, " AS Expr5, ",
                        import_ID, " AS Expr6;")
      res <- RODBC::sqlQuery(ch, strsql)

      # process errors
      if(!ignore_insert_errors)
        res %>% ensure_insert_success
      else if (verbose)
        res %>% warn_insert_success

    })
    cat(sprintf("%d inserted\n", nrow(dat$uploads)))
    if (verbose) print_tibble(dat$uploads)
  } else {
    cat("no upload data to insert\n")
  }

  ##### Insert bad records ####
  cat("\tInserting bad records...")
  if(!is.null(dat$bad_recs)){
    dat$bad_recs %>%
      tibble::rowid_to_column() %>%
      split(.$rowid) %>%
      purrr::walk(function(dt) {
      strsql <- paste0("INSERT INTO tblBadRecord ( [BoardID], [FetchDateTime], [WIFIID], [ImportID] ) SELECT ",
                        dt$BoardID, " AS Expr1, ",
                        "#", format(dt$fetchDateTime, format = "%Y-%b-%d %H:%M:%S"), "# AS Expr3, ",
                        "'", dt$WiFiID, "' AS Expr4, ",
                        import_ID, " AS Expr5;")
      res <- RODBC::sqlQuery(ch, strsql)

      # process errors
      if(!ignore_insert_errors)
        res %>% ensure_insert_success
      else if (verbose)
        res %>% warn_insert_success
    })
    cat(sprintf("%d inserted\n", nrow(dat$bad_recs)))
    if (verbose) print_tibble(dat$bad_recs)
  } else {
    cat("no bad records to insert\n")
  }

  cat("Done.\n")
  TRUE
}

#####
# Insert a tag read into one of the tag read tables
#
# ch - open RODBC channel
# import_ID - ID from tblImports
# read_type - either "Web", "Test", "Unknown" or "Test_board"
#     only used if whch_table == "tblOtherTagRead"
# ignore_errors - ignore insertion errors (which normally cause a fatal error).
# verbose - If TRUE,print warning message if insertion error occurs and ignore_errors is TRUE.
#
# returns tag_dat invisible so it can be used in pipes
#
insert_tag_reads <- function(tag_dat = NULL, ch = NULL, whch_table = NULL, import_ID = NULL, read_type = NULL, ignore_errors = FALSE,
                             verbose = FALSE) {

  ensurer::ensure_that(tag_dat, is.data.frame(.))
  ensurer::ensure_that(whch_table, is.character(.))
  ensurer::ensure_that(ch, class(ch) == "RODBC")

  if (nrow(tag_dat) == 0) {
    #warning(sprintf("Tag_dat for read_type '%s' is empty. Not inserting anything.", read_type), immediate. = TRUE)
    return(invisible(tag_dat))
  }

  tag_dat  %>%
    tibble::rowid_to_column() %>%
    split(.$rowid) %>%
    purrr::walk(function(dt) {

    if (whch_table == "tblOtherTagRead") {
      ensurer::ensure_that(read_type, is.character(.))

      read_types <- RODBC::sqlFetch(ch, "lkpTagReadType", as.is = TRUE) %>% ensure_data_is_returned
      read_type_ID <- dplyr::filter(read_types, ReadType == read_type)$ReadID %>% ensurer::ensure_that(length(.) > 0)

      strsql <- paste0("INSERT INTO ", whch_table," ( [BoardID], [DateTime], [FetchDateTime], [WIFIID], [TagID], ",
                      "[NumReads], [ImportID], [ReadType] ) SELECT ",
                      dt$BoardID, " AS Expr1, ",
                      "#", format(dt$dateTime, format = "%Y-%b-%d %H:%M:%S"), "# AS Expr2, ",
                      "#", format(dt$fetchDateTime, format = "%Y-%b-%d %H:%M:%S"), "# AS Expr3, ",
                      "'", ifelse("WiFiID" %in% colnames(dt), dt$WiFiID, "CableConnect"), "' AS Expr4, ",
                      "'", dt$tagID, "' AS Expr5, ",
                      dt$numread, " AS Expr6, ",
                      import_ID, " AS Expr7, ",
                      read_type_ID, " AS Expr8;")
    } else {
      strsql <- paste0("INSERT INTO ", whch_table," ( [BoardID], [DateTime], [FetchDateTime], [WIFIID], [TagID], ",
                      "[NumReads], [ImportID] ) SELECT ",
                      dt$BoardID, " AS Expr1, ",
                      "#", format(dt$dateTime, format = "%Y-%b-%d %H:%M:%S"), "# AS Expr2, ",
                      "#", format(dt$fetchDateTime, format = "%Y-%b-%d %H:%M:%S"), "# AS Expr3, ",
                      "'", ifelse("WiFiID" %in% colnames(dt), dt$WiFiID, "CableConnect"), "' AS Expr4, ",
                      "'", dt$tagID, "' AS Expr5, ",
                      dt$numread, " AS Expr6, ",
                      import_ID, " AS Expr7;")
    }

    res <- RODBC::sqlQuery(ch, strsql)
    if (!ignore_errors)
        res %>% ensure_insert_success
    else if (verbose)
        res %>% warn_insert_success
  })
  invisible(tag_dat)
}
