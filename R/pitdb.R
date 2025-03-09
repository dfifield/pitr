#'@export
#'@title Open a pit tag database
#'
#'@description The function will connect to the Access PIT tag database via ODBC
#'  and return a handle to the open database object.
#'@param pathname Full pathname of the Access PIT tag database to be opened.
#'@details The function will open the Access database specified by
#'  \code{pathname}.
#'@return A channel to the open database object or -1 on error.
#'@section Author: Dave Fifield
#'
pitdb_open <- function(pathname){
  ### open connection. This should work on both 32-bit R and 64-bit R as long as
  # user has both 32-bit and 64-bit Access drivers installed. See
  # https://stackoverflow.com/questions/45064057/reading-data-from-32-bit-access-db-using-64-bit-r
  # and https://www.microsoft.com/en-US/download/details.aspx?id=13255

  # RODBC::odbcConnectAccess(pathname)
  # RODBC::odbcDriverConnect(paste0("Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=", pathname))

  # Handle windows shortcuts
  if (tools::file_ext(pathname) == "lnk") {
    # Windows shortcut - follow to get real path

  } else
    RODBC::odbcConnectAccess2007(pathname, uid="")
}

#'@export
#'@title Close a pit tag database
#'
#'@description The function will close the connection to the Access PIT tag database via ODBC.
#'@param ch A open ODBC channel.
#'@return Nothing
#'@section Author: Dave Fifield
#'
pitdb_close <- function(ch){
  RODBC::odbcClose(ch)
}
