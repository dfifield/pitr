
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
#'@param verbose If \code{TRUE}, print a message if file contains "no updates"
#'
#'@details This function reads a text file of records that have been extracted
#'  from a monitor board. These files are typically downloaded via direct cable
#'  connection or via email from a Raspberry Pi base station.
#'
#'  In most cases \code{filename} will contain info from multiple monitor
#'  boards, e.g., the daily files emailed from a Raspberry Pi base station.
#'
#'  All \code{tagID}'s are converted to lowercase.
#'  Does not attempt to further split tag_reads into normal reads, webserver
#'  tags, ghost tags, etc. since this requires info on known tags/prefixes, etc
#'  from the database and it is desirable to have this function work whether
#'  database is available or not.
#'
#'  talk about format of filename somewhere
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
  fetchDateTime <- NA
  if(fetch_type == "CableConnect") {
    parts <- stringr::str_split(basename(filename), "_")
    fetchDateTime <- as.POSIXct(strptime(paste0(parts[[1]][2], " ", parts[[1]][3]), format = "%Y%m%d %H%M%S"))
  }

  if(is.na(fetchDateTime)) fetchDateTime = lubridate::now()

  dat <- as.data.frame(readLines(con = filename), stringsAsFactors = F)
  names(dat) <- "string"

  # Any lines in file? Nothing else to do?
  if (nrow(dat) == 0)
    return(retval)

  # Detect no updates. This is a report file with no data. Should only happen with "WIFI" downloads.
  if (nrow(dat) == 1 && str_detect(dat[1,], "no updates")){
    if (verbose) cat('File contains "no updates".\n')
    return(retval)
  }

  # Detect bad records. These were due to a bug in mqtt upload that send invalid data. Fixed in V1.2 of rfid_logger.
  bad_recs_idx <- grep("Bad record", dat$string)
  if (length(bad_recs_idx) != 0) {
    bad_recs <- tidyr::separate(dplyr::slice(dat, bad_recs_idx), "string", sep = "[ ]+",
                                into = c("FDate", "FTime", "WiFiID",  "BoardID"), extra = "drop")
    bad_recs$BoardID <- as.integer(bad_recs$BoardID)
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

  # Cannout use "convert = T" here because if the tagID column only contains tags with digits 0-9 they will be converted to integers
  # and if any of them start with some number of 0's these will be lost.
  tag_reads <- tidyr::separate(dplyr::slice(tag_reads, grep(" T ", dat$string)), "string",
                     into =  switch(fetch_type,
                                    WiFi = c("FDate", "FTime", "WiFiID",  "BoardID", "Date", "Time", "numread", "tagID"),
                                    CableConnect = c("BoardID", "Date", "Time", "numread", "tagID"),
                                    "error"),
                     sep = "[ ]+")
  if(nrow(tag_reads) > 0) {
    tag_reads$BoardID <- as.integer(tag_reads$BoardID)
    tag_reads$numread <- as.integer(tag_reads$numread)
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

    tag_reads$tagID <- stringr::str_to_lower(tag_reads$tagID)
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


  # Optionally remove data coming from the test board. This prevents accidental
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
