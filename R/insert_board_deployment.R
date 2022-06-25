#'@export
#' @importFrom magrittr %>%
#'@title Insert board deployment data
#'
#'@description The function inserts a single board deployment record into the
#' database.
#'
#'@param dt A dataframe containing at least \code{BoardID, Burrow, FromDate}.
#'  Also may contain \code{ToDate, Revision, FlashSize, Status,
#'  Plot, Habitat, BurrowNotes} - see below for details.
#'@param ch Open RODBC channel connecting to the database
#'
#'@details Creates appropriate entries in tblBoardDeployment, tblBoard,
#'  and tblBurrow to record a board being deployed in a burrow from \code{FromDate}
#'  to \code{ToDate} (which may be NA).
#'
#'  Date is the date and time of the deployment of the board in the burrow. It must
#'  be a \code{POSIXct}.
#'
#'  If \code{BoardID} does not exist in \code{tblBoard} it is added but this requires
#'  fields for \code{Revision, FlashSize,} and \code{Status}.
#'
#'  if \code{Burrow} does not exist in \code{tblBurrow} it will be added but
#'  this requires fields for \code{Plot} and \code{Habitat} and optionally
#'  \code{BurrowNotes}. See table \code{lkpHabitat} for habitat details.
#'
#'@return Nothing.
#'@section Author: Dave Fifield

pitdb_insert_board_deployment <- function(dt, ch){

  nrow(dt) == 1 || stop(
    sprintf("pitdb_insert_board_deployment: dt contains %d rows, should be 1", nrow(dt)))

  !is.null(ch) || stop("parameter ch is missing.")

  if (with(dt, any(is.na(BoardID), is.na(Burrow), is.na(FromDate)))) {
    message("ERROR: pitdb_insert_board_deployment: ",
            sprintf("At least one of BoardID (%s), Burrow (%s), or FromDate (%s) is NA",
                    as.character(dt$BoardID), as.character(dt$Burrow),
                    as.character(dt$FromDate)),
            "\nNot entering this record!")
    return()
  }

  message(sprintf("Inserting board deployment record for %s in %s from %s...\n",
                  as.character(dt$BoardID), as.character(dt$Burrow),
                  as.character(dt$FromDate)))

  # Deal with Board
  create_board_if_needed(dt, ch)

  # Deal with Burrow
  create_burrow_if_needed(dt, ch)

  # Insert board deployment record
  message("\tCreating tblBoardDeploy record...")
  strsql <- paste0("INSERT INTO tblBoardDeploy ( [BoardID], [BurrowID], [FromDate], ",
                   "[ToDate] ) SELECT ",
                   dt$BoardID, " AS e1, ",
                   "'", dt$Burrow, "' AS e2, ",
                   "#", format(dt$FromDate, format = "%Y-%b-%d %H:%M:%S"), "# AS e3, ",
                   do_na(val_or_NA(dt, "ToDate")), " AS e4;")
  if (!try_insert(ch, strsql)) return()
  message("done\n")
}
