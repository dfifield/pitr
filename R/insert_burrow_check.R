#'@export
#' @importFrom magrittr %>%
#'@title Insert burrow check data
#'
#'@description The function inserts a single burrow check record into the
#' database.
#'
#'@param dt A dataframe containing at least \code{Burrow, Status, DateTime}.
#'  May also contain \code{BandNo, CheckNote}. See below for more details.
#'@param ch Open RODBC channel connecting to the database
#'
#'@details Creates appropriate entries in tblBurrowCheck, tblBurrow,
#'  and tblBird to record the status of a burrow on \code{DateTime}.
#'
#'  DateTime is the date and time of the check. It must
#'  be a \code{POSIXct}.
#'
#'  See table \code{lkpBurrowStatus} for valied \code{Status} values.
#'
#'  If \code{Burrow} does not exist in \code{tblBurrow} it will be added but
#'  this requires fields for \code{Plot} and \code{Habitat} and optionally
#'  \code{BurrowNotes}. If the burrow does already exist, these fields are ignored
#'  if provided. See table \code{lkpHabitat} for habitat details.
#'
#'  If a bird is found in the burrow, it is indicated by \code{BandNo}.
#'  If this bird is not already in \code{tblBirds}), then a new record is
#'  created in \code{tblBirds} but this
#'  requires fields for \code{AgeAtBanding, Species, and Colony} and optionally
#'  \code{Sex} and \code{BirdNote}. If the bird is already in \code{tblBirds} then
#'  these fields are ignored.
#'@return Nothing.
#'@section Author: Dave Fifield

pitdb_insert_burrow_check <- function(dt, ch){

  nrow(dt) == 1 || stop(
    sprintf("pitdb_insert_board_deployment: dt contains %d rows, should be 1", nrow(dt)))

  !is.null(ch) || stop("parameter ch is missing.")

  if (with(dt, any(is.na(Burrow), is.na(Status), is.na(DateTime)))) {
    message("ERROR: pitdb_insert_burrow_check: ",
            sprintf("At least one of Burrow (%s), Status (%s), or DateTime (%s) is NA",
                    as.character(dt$Burrow), as.character(dt$Status),
                    as.character(dt$DateTime)),
            "\nNot entering this record!")
    return()
  }

  message(sprintf("Inserting burrow check record for %s with status %s on %s...\n",
                  as.character(dt$Burrow), as.character(dt$Status),
                  as.character(dt$DateTime)))

  # Deal with bird
  birdID <- find_or_create_bird(dt, ch)

  # Deal with burrow
  create_burrow_if_needed(dt, ch)

  # Insert burrow check record
  message("\tCreating tblBurrowCheck record...")
  strsql <- paste0("INSERT INTO tblBurrowCheck ( [BurrowID], [Status], [BirdID], ",
                   "[DateTime], [Note] ) SELECT ",
                   "'", dt$Burrow, "' AS e1, ",
                   "'", dt$Status, "' AS e2, ",
                   birdID, " AS e3, ",
                   "#", format(dt$DateTime, format = "%Y-%b-%d %H:%M:%S"), "# AS e4, ",
                   do_na(val_or_NA(dt, "CheckNote")), " AS e5;")
  if (!try_insert(ch, strsql)) return()
  message("done\n")
}
