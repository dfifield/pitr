#'@importFrom magrittr %>%
#'@export
#'@title Extract capture events PIT tag database.
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
#'@section Author: Dave Fifield

# debugging
# pitdb.path <- "C:/Users/fifieldd/OneDrive - EC-EC/Projects/Burrow_Logger/Data/Burrow logger master0.1.accdb"
#
# years <- 2019
# x <- pitdb_extract_captures(pitdb.path, years = years)

pitdb_extract_captures <- function(pitdb.path = NULL,
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

  # initialize
  year.selection <- ""

  # Setup dummy where clause
  where.start <-  "WHERE ((1=1)"
  where.end <- ")"

  # setwd and open connection
  channel1 <- RODBC::odbcConnectAccess2007(pitdb.path, uid = "")

  # write SQL selection for year
  if (!missing(years)) {
    if (length(years) == 1)
      year.selection <- paste0("AND ((Year([tblCapture].[Date])) = ", years, ")")
    else if (length(years) == 2)
      year.selection <- paste0("AND ((Year([tblCapture].[Date])) Between ", years[1], " And ", years[2], ")")
    else
      stop("Years must be either a single number or a vector of two numbers.")
  }


  query = paste0("SELECT tblBirds.BandNo, tblBurrow.Plot, tblCapture.BurrowID, ",
                 "tblCapture.Date AS [DateTime], tblTagDeployment.TagID, ",
                 "Year([DateTime]) AS [year], tblBirds.AgeAtBanding ",
                 "FROM tblBirds INNER JOIN ((tblBurrow INNER JOIN tblCapture ",
                 "ON tblBurrow.BurrowID = tblCapture.BurrowID) ",
                 "INNER JOIN tblTagDeployment ",
                 "ON tblCapture.ID = tblTagDeployment.DeployCap) ",
                 "ON tblBirds.ID = tblCapture.BirdID ",
                 paste(where.start, year.selection, where.end, sep = " ")
                 )

  dat <- RODBC::sqlQuery(channel1, query) %>% ensure_data_is_returned

  # close connection
  RODBC::odbcClose(channel1)

  dat
}
