#'@export
#'@title Open a pit tag database
#'
#'@description The function will connect to the Access PIT tag database via ODBC
#'  and return a handle to the open database object.
#'@param pathname Full pathname of the Access PIT tag database to be opened.
#'@details The function will open the Access database specified by
#'  \code{pathname}. Note that it only works in a 32-bit R session since the
#'  only 32-bit Access drivers are available.
#'@return A channel to the open database object or -1 on error.
#'@section Author: Dave Fifield
#'
pitdb_open <- function(pathname){

  if (Sys.getenv("R_ARCH") != "/i386")
    stop("You are not running a 32-bit R session. You must run pitdb_open in a 32-bit R session due to limitations in the RODBC Access driver.")


  #RODBC::odbcConnectAccess(pathname)
  RODBC::odbcDriverConnect(paste0("Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=", pathname))
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
