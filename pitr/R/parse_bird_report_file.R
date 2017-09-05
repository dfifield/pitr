#'@export
#'@title Parse data from a Raspberry Pi report file
#'
#'@description The function will parse a bird report datafile.
#'
#'@param filename Path to file containing data dumped from a monitor board.
#'@param sp_tags A dataframe that indicates the identity of test tags, known
#'  (i.e., deployed) tag prefixes, and webserver_mode tag prefixes.
#'
#'  The dataframe must contain at least two columns: "Typ" and "Val", Where Typ has entries for at least
#'  \code{test_tag, web_prefix, known_prefix}.
#'
#'@param verbose Print information regarding numbers of status records, tag read
#'  records, mark_upload records, # of boards reporting, and ghost reads.
#'
#'@details This function reads a text file of records that have been extracted
#'  from a monitor board. These files are typically downloaded via direct cable
#'  connection or via email from a Raspberry Pi base station.
#'
#'  In most cases \code{inputfile} will contain info from multiple monitor
#'  boards, e.g., the daily files emailed from a Raspberry Pi base station.
#'
#'  talk about format of inputfile somewhere
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
#'@return Returns TRUE on success and FALSE on error.
#'@section Author: Dave Fifield
#'
pitdb_process_bird_report_file <- function(filename, sp_tags = NULL, verbose = F) {


  # initialize. XXX Note the awful mixing of variable naming schemes.
  bad_recs <- NA
  fakeGhost <- NA

  if (is.null(sp_tags))
    stop("Argument sp_tags has not been provided.")

  # Split out special tags
  test_tags <- dplyr::filter(sp_tags, Typ == "test_tag")$Val
  known_prefix <- dplyr::filter(sp_tags, Typ == "known_prefix")$Val
  web_prefix <- dplyr::filter(sp_tags, Typ == "web_prefix")$Val

  cat(paste0("==========================================================================================",
             "\n\nProcessing ", basename(filename), "\n"))

  dat <- as.data.frame(readLines(con = filename), stringsAsFactors = F)
  names(dat) <- "string"

  cat(paste0(nrow(dat), " rows read\n"))

  # Nothing else to do
  if (nrow(dat) == 0)
    return(-1)

  # Detect no updates. This is a report file with no data.
  if (nrow(dat) == 1 && (grep("no updates", dat[1,]) == 1)){
    cat('File contains "no updates".\n')
    return(NULL)
  }

  # Detect bad records. These were due to a bug in mqtt upload that send invalid data. Fixed in V1.2 of rfid_logger.
  bad_recs_idx <- grep("Bad record", dat$string)
  if (length(bad_recs_idx) != 0) {
    bad_recs <- dat[bad_recs_idx,]
    if(verbose){
      cat(paste0(length(bad_recs_idx), " bad records.\n"))
      print(bad_recs)
    }
  }

  # Extract statuses
  sdat <- as.data.frame(sub(" S ", " ", dat[,1]))
  names(sdat) <- "string"

  # note this line figures out which rows to keep in the slice by looking for
  # the initial "S " in dat, since it has already been removed from sdat.
  sdat <- tidyr::separate(dplyr::slice(sdat, grep(" S ", dat$string)), "string",
                   into = c("FDate", "FTime", "WiFiID",  "BoardID", "Date", "Time", "VCoin", "Vin", "MCUTemp", "Freq"), sep = "[  ]+",
                   convert = T)

  sdat$dateTime <- as.POSIXct(strptime(paste0(sdat$Date, " ", sdat$Time), format = "%Y-%m-%d %H:%M:%S"))
  sdat$fetchDateTime <- as.POSIXct(strptime(paste0(sdat$FDate, " ", sdat$FTime), format = "%Y-%m-%d %H:%M:%S"))
  sdat$Date <- NULL
  sdat$Time <- NULL
  sdat$FDate <- NULL
  sdat$FTime <- NULL

  boards <- unique(sdat$BoardID)
  boards <- boards[order(boards)]
  if (verbose){
    cat(paste0(nrow(sdat), " status entries from  ",
             paste0(range(sdat$dateTime), collapse = " to "), "\n",
             paste0("\tfrom ",  length(boards), " boards: ", paste0(boards[order(boards)], collapse = ", "), "\n")))
  }

  #extract tag reads
  tagDat <- as.data.frame(sub(" T ", " ", dat[,1]))
  names(tagDat) <- "string"

  tagDat <- tidyr::separate(dplyr::slice(tagDat, grep(" T ", dat$string)), "string",
                     into = c("FDate", "FTime", "WiFiID",  "BoardID", "Date", "Time", "numread", "tagID"), sep = "[  ]+",
                     convert = T)
  tagDat$dateTime <- as.POSIXct(strptime(paste0(tagDat$Date, " ", tagDat$Time), format = "%Y-%m-%d %H:%M:%S"))
  tagDat$fetchDateTime <- as.POSIXct(strptime(paste0(tagDat$FDate, " ", tagDat$FTime), format = "%Y-%m-%d %H:%M:%S"))
  tagDat$Date <- NULL
  tagDat$Time <- NULL
  tagDat$FDate <- NULL
  tagDat$FTime <- NULL

  # normal reads. This will not include web tags or test_tags
  reads <- dplyr::filter(tagDat, substr(tagID, 1, 5) %in% known_prefix, !(tagID %in% test_tags))
  tags <- unique(reads$tagID)
  tags <- tags[order(tags)]

  if (verbose && nrow(reads) > 0){
    cat(paste0("\n", nrow(reads), " reads from ", length(tags), " tags (",
               paste0(tags, collapse = ","), ") with prefix(es) ",
               paste0(known_prefix, collapse = ", "),  " spanning dates ",
               paste0(range(reads$dateTime), collapse = " to "), ".\n"))
    print(reads)
  }

  # report on 1-read detections reads
  one_read <- dplyr::filter(tagDat, numread == 1)
  if (verbose) cat(paste0(nrow(one_read), " apparent ghost reads out of ", nrow(tagDat), "\n"))

  # Ghost reads
  if (nrow(one_read) > 0) {
    fakeGhost <- dplyr::filter(tagDat, numread == 1, substr(tagID, 1, 5) %in% known_prefix)
    if (verbose) {
      cat(paste0(nrow(fakeGhost), " of these may be real tags with 1 read each.\n"))
      if(nrow(fakeGhost) > 0) print(fakeGhost)
    }
  }

  # test tag reads
  test <- dplyr::filter(tagDat, tagID %in% test_tags)
  if (verbose) {
    cat(paste0("\n", nrow(test), " test tag reads.\n"))
    if(nrow(test) > 0) print(test)
  }

  # web tag reads
  web <- dplyr::filter(tagDat, numread == 1, substr(tagID, 1, 4) == web_prefix)
  if (verbose){
    cat(paste0(nrow(web), " webserver mode tags\n"))
    if(nrow(web) > 0) print(web)
  }

  #extract mark_upload records
  markDat <- as.data.frame(sub(" M ", " ", dat[,1]))
  names(markDat) <- "string"

  markDat <- tidyr::separate(dplyr::slice(markDat, grep(" M ", dat$string)), "string",
                     into = c("FDate", "FTime", "WiFiID",  "BoardID", "Date", "Time", "prev_index"), sep = "[  ]+",
                     convert = T)
  markDat$dateTime <- as.POSIXct(strptime(paste0(markDat$Date, " ", markDat$Time), format = "%Y-%m-%d %H:%M:%S"))
  markDat$fetchDateTime <- as.POSIXct(strptime(paste0(markDat$FDate, " ", markDat$FTime), format = "%Y-%m-%d %H:%M:%S"))
  markDat$Date <- NULL
  markDat$Time <- NULL
  markDat$FDate <- NULL
  markDat$FTime <- NULL

  if (verbose){
    cat(paste0(nrow(markDat), " upload records\n"))
    if(nrow(markDat) > 0) print(markDat)
  }

  # return value
  return(list(boards = boards, status = sdat, uploads = markDat, tag_reads = reads, web_tags = web, test_tags = test,
              poss_ghost = one_read, fake_ghost = fakeGhost, bad_recs = bad_recs))
}

