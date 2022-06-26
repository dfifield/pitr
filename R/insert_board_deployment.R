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
#'  Fails if this deployment would conflict with an existing deployment of this
#'  board.
#'
#'@return \code{TRUE} on success, else \code{FALSE}.
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
            "\nNot entering this record!\n")
    return()
  }

  message(sprintf("Inserting board deployment record for %s in %s from %s...\n",
                  as.character(dt$BoardID), as.character(dt$Burrow),
                  as.character(dt$FromDate)))

  # First check to see if this deployment would overlap with any other existing
  # deployment for this board.
  strsql <- paste0("SELECT tblBoardDeploy.BrdDeplID, tblBoardDeploy.BoardID, ",
                   "tblBoardDeploy.BurrowID, tblBoardDeploy.FromDate, ",
                   "tblBoardDeploy.ToDate, tblBoardDeploy.Note ",
                   "FROM tblBoardDeploy ",
                   "WHERE (((tblBoardDeploy.BoardID)=", dt$BoardID, "));")
  tblBrdDepl <- RODBC::sqlQuery(ch, strsql) %>% ensure_data_is_returned

  # All records should have a from and to date but lets be paranoid
  if (any(is.na(tblBrdDepl$FromDate))) {
    message(paste0("ERROR: pitdb_insert_board_deployment: ",
                 "There is at least one deployment of that board with a missing FromDate.",
                 "This shouldn't happen:"))
    print(tblBrdDepl)
    return(FALSE)
  }

  # Check for any active deployment of this board (ie. with NULL ToDate)
  if (nrow(tblBrdDepl) > 0) {
    if (any(is.na(tblBrdDepl$ToDate))) {
      message(paste0("ERROR: pitdb_insert_board_deployment: ",
                   "There is at least one currently active deployment of that board.",
                   "\nNot entering this record!:\n"))
      print(filter(tblBrdDepl, is.na(ToDate)))
      message("\n")
      return(FALSE)
    }

    # Ok, so remove active deployment and then check overlaps
    # Check for overlapping dates - 2 cases:
    #   - either dt$FromDate or dt$ToDate is between FromDate and ToDate of some
    #    record in tblBrdDepl, where 'between' includes the endpoints.
    tblBrdDepl <- filter(tblBrdDepl, !is.na(ToDate))
    res <- tblBrdDepl %>%
      split(1:nrow(.)) %>%
      map_lgl(~ between(dt$FromDate, .$FromDate, .$ToDate))

    if (any(res)) {
      message(paste0("ERROR: pitdb_insert_board_deployment: ",
                   "This deployment overlaps in time with existing deployment.",
                   "\nNot entering this record:\n"))
      print(tblBrdDepl[res,])
      message("\n")
      return(FALSE)
    }
  }

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
  TRUE
}
