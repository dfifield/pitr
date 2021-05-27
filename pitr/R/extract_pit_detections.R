#'@importFrom magrittr %>%
#'@export
#'@title Extract tag reads from PIT tag database.
#'
#'@description This function will connect to the Access database, create a
#'  series of queries and import the desired information in a data frame.
#'
#'@param pitdb.path \[character:\sQuote{NULL}]\cr Full path name to the PIT tag
#'database.
#'
#'@param years \[integer:\sQuote{NULL}]\cr Optional. Either a single year or a
#'  vector of two years denoting "from" and "to" (inclusive).
#'
#'@details
#'@section Author: Dave Fifield


# debugging
# pitdb.path <- "C:\\Users\\fifieldd\\Documents\\Offline\\Projects\\Burrow_Logger\\Data\\Burrow logger master0.1.accdb"
# years <- 2019

pitdb_extract_detections <- function(pitdb.path = NULL,
                            years = NULL,
                            verbose = FALSE)
{

  coll = checkmate::makeAssertCollection()

  checkmate::assert(
    checkmate::check_null(years),
    checkmate::check_integerish(years, any.missing = FALSE, len = 1),
    checkmate::check_integerish(years, any.missing = FALSE, len = 2, sorted = TRUE),
    add = coll
  )

  checkmate::assert(
    checkmate::check_file_exists(pitdb.path),
    add = coll
  )

  checkmate::reportAssertions(coll)

  # test for 32-bit architecture
  if (Sys.getenv("R_ARCH") != "/i386")
    stop("You are not running a 32-bit R session. You must run ECSAS.extract in a 32-bit R session due to limitations in the RODBC Access driver.")

  # initialize
  year.selection <- ""

  # setwd and open connection
  channel1 <- RODBC::odbcConnectAccess2007(pitdb.path, uid = "")

  # write SQL selection for year
  if (!missing(years)) {
    if (length(years) == 1)
      year.selection <- paste0("AND ((Year([DateTime])) = ", years, ")")
    else if (length(years) == 2)
      year.selection <- paste0("AND ((Year([DateTime])) Between ", years[1], " And ", years[2], ")")
    else
      stop("Years must be either a single number or a vector of two numbers.")
  }


  query <- paste0("SELECT tblBurrow.BurrowID, tblBirds.BandNo, tblBirdTagRead.DateTime, tblBoardDeploy.FromDate,",
                 "tblBoardDeploy.ToDate, tblBoardDeploy.BoardID, tblBoardDeploy.BrdDeplID, tblBirds.AgeAtBanding,",
                 "tblBirdTagRead.TagID, tblBurrow.Plot, tblBirdTagRead.ImportID, tblBirds.ID AS BirdID, ",
                 "Year([DateTime]) AS [year], tblBirdTagRead.NumReads, tblBirdTagRead.TagReadD, ",
                 "[NumReads]*0.065 AS DetctnTime ",
                 "FROM (tblBirds INNER JOIN tblCapture ON tblBirds.ID = tblCapture.BirdID) INNER JOIN ",
                 "(tblBurrow INNER JOIN ((tblBoard INNER JOIN tblBoardDeploy ON ",
                 "tblBoard.BoardID = tblBoardDeploy.BoardID) INNER JOIN ",
                 "(tblTagDeployment INNER JOIN tblBirdTagRead ON tblTagDeployment.TagID = tblBirdTagRead.TagID) ",
                 "ON tblBoard.BoardID = tblBirdTagRead.BoardID) ON tblBurrow.BurrowID = tblBoardDeploy.BurrowID) ",
                 "ON tblCapture.ID = tblTagDeployment.DeployCap ",
                 "WHERE (((tblBirdTagRead.DateTime) >= [FromDate]) And ",
                 "((tblBirdTagRead.DateTime) <= IIf(IsNull([ToDate]),#1/1/2200#,[ToDate])) ",
                 year.selection,
                 ")")

  dat <- RODBC::sqlQuery(channel1, query) %>% ensure_data_is_returned

  # close connection
  RODBC::odbcClose(channel1)

  dat
}
