#'@export
#' @importFrom magrittr %>%
#'@title Insert capture data
#'
#'@description The function inserts a single capture record into the database.
#'
#'@param dt A dataframe containing at least \code{Band, Burrow, DateTime} and
#'  optionally \code{Colony, Species, Age, TotW, BagW, Tarsus, Wing, BloodCard,
#'  BloodVial, BloodVialAmount, CloSwab, PharSwab, Feathers, SampleNote,
#'  Behaviour, PIT_deployed, PIT_removed}.
#'@param ch Open RODBC channel connecting to the database
#'
#'@details Creates or updates appropriate entries in tblBirds, tblBands, tblTags,
#'  tblNestAttempt, tblCapture, tblMorpho, and tblTagDeployment.
#'
#'  If this bird is not already in \code{tblBirds}, then a new record is
#'  created in \code{tblBirds} but this
#'  requires fields for \code{AgeAtBanding, Species, and Colony} and optionally
#'  \code{Sex} and \code{BirdNote}. If the bird is already in \code{tblBirds} then
#'  these fields are ignored.
#'
#'  If \code{Burrow} does not exist then it is created in \code{tblBurrow} which
#'  requires field \code{Plot} and optionally \code{Habitat} and \code{BurrowNotes}.
#'
#'  If \code{Species} or \code{Colony} are missing, they default to "Gull Island"
#'  and "Leach's Storm-Petrel" respectively. See tables \code{tblSpeciesInfo} and
#'  \code{lkpColony} respectively for acceptable values.
#'
#'  If \code{PIT_deployed} tag does not already
#'  exist in the database, then \code{dt} must contain fields \code{Manufac}
#'  and \code{TagType} - see tables \code{lkpManufacturers} and \code{lkpTagTypes}
#'  in the database for acceptable values.
#'
#'  If \code{PIT_removed} is supplied then it should exist in
#'  \code{tblTagDeployment}, otherwise a warning is displayed and no removal
#'  is recorded.
#'
#'  \code{Behaviour} refers to behaviour when arm is inserted into burrow and includes
#'  things like "run to back", "bite once", "bite multiple times", "vocalize",
#'  "calm/docile/neutral"
#'@return Nothing.
#'@section Author: Dave Fifield

pitdb_insert_capture <- function(dt, ch){

  nrow(dt) == 1 || stop(
    sprintf("pitdb_insert_capture: dt contains %d rows, should be 1", nrow(dt)))

  !is.null(ch) || stop("parameter ch is missing.")

  if (with(dt, any(is.na(Band), is.na(Burrow), is.na(DateTime)))) {
    message("ERROR: pitdb_insert_capture: ",
            sprintf("At least one of Band (%s), Burrow (%s), or DateTime (%s) is NA",
                    as.character(dt$Band), as.character(dt$Burrow),
                    as.character(dt$DateTime)),
            "\nNot entering this record!\n")
    return()
  }

  message(sprintf("Inserting capture record for %s in %s on %s...\n",
                  as.character(dt$Band), as.character(dt$Burrow),
                  as.character(dt$DateTime)))

  # Deal with bird
  birdID <- find_or_create_bird(dt, ch)

  # Deal with burrow
  create_burrow_if_needed(dt, ch)

  # Get capture type - default "In burrow by hand"
  if (not_exist_or_na(dt, "CaptureType")) {
    strCapType <- "In burrow by hand"
  } else {
    strCapType <- dt$CaptureType
  }

  # Lookup capture type
  strsql <- paste0("SELECT lkpCaptureType.ID FROM lkpCaptureType ",
                   "WHERE (((lkpCaptureType.CaptureType)='", strCapType, "'));")
  res <- RODBC::sqlQuery(ch, strsql) %>% ensure_data_is_returned %>%
    ensure_one_row_returned
  capID <- res$ID

  # Deal with missing or NA columns
  dt <- dplyr::mutate(dt,

                      TotW = val_or_NA(dt, "TotW"),
                      BagW = val_or_NA(dt, "BagW"),
                      Wing = val_or_NA(dt, "Wing"),
                      Tarsus = val_or_NA(dt, "Tarsus"),
                      Note = val_or_NA(dt, "Note"),
                      Behaviour = val_or_NA(dt, "Behaviour"),
                      BloodCard = val_or_NA(dt, "BloodCard"),
                      BloodVial = val_or_NA(dt, "BloodVial"),
                      BloodVialAmount = val_or_NA(dt, "BloodVialAmount"),
                      CloSwab = val_or_NA(dt, "CloSwab"),
                      PharSwab = val_or_NA(dt, "PharSwab"),
                      Feathers = val_or_NA(dt, "Feathers"),
                      SampleNote = val_or_NA(dt, "SampleNote")
                      )

  #### create a tblCapture record ####
  message("\tCreating tblCapture record...")
  strsql <- with(dt, paste0("INSERT INTO tblCapture ( [BirdID], [Date], ",
                  "[BurrowID], [CaptureType], [Note]) SELECT ",
                  birdID, " AS e1, ",
                  "#", format(DateTime, format = "%Y-%b-%d"), "# AS e2, ",
                  "'", Burrow, "' As e3, ",
                  capID, " AS e4, ",
                  do_note(Note), " AS e5;"))
  if (!try_insert(ch, strsql)) return()
  message("done")

  # get Capture ID
  captureID <- get_sql_ID(ch)

  # Samples
  if (!with(dt, all(is.na(BloodCard),
                      is.na(BloodVial),
                      is.na(BloodVialAmount),
                      is.na(CloSwab),
                      is.na(PharSwab),
                      is.na(Feathers),
                      is.na(SampleNote))
    )) {
    #### create a tblSamples record ####
    message("\tCreating tblSamples record...")
    strsql <- with(dt, paste0("INSERT INTO tblSamples ( [CaptureID], [BldCard],",
                              " [BldVial], [BldAmount], [Feathers], [CloacalSwab], ",
                              " [PharyngealSwab], [Note] ) SELECT ",
                              captureID, " AS e1, ",
                              do_na(BloodCard), " AS e2, ",
                              do_na(BloodVial), " AS e3, ",
                              do_na(BloodVialAmount), " AS e4, ",
                              do_na(CloSwab), " AS e5, ",
                              do_na(PharSwab), " AS e6, ",
                              do_na(Feathers), " AS e7, ",
                              do_na(SampleNote), " AS e8;"
                   ))
    if (!try_insert(ch, strsql)) return()

    message("done\n")
  }

  # Morphology: Only create morphology record if there is some...
  if (!with(dt, all(is.na(Tarsus), is.na(TotW), is.na(BagW), is.na(Wing)))) {
    #### create a tblMorpho record ####
    message("\tCreating tblMorph record...")
    strsql <- with(dt, paste0("INSERT INTO tblMorpho ( [CaptureID], [BirdPlusBagWt],",
                              " [BagWt], [BirdWt], [Wing], [Tarsus]) SELECT ",
                    captureID, " AS e1, ",
                    do_na(TotW), " AS e2, ",
                    do_na(BagW), " AS e3, ",
                    do_na(TotW) - BagW, " AS e4, ",
                    do_na(Wing), " AS e5, ",
                    do_na(Tarsus), " AS e6;"))
    if (!try_insert(ch, strsql)) return()
    message("done\n")
  }

  #### create a tblTagDeployment record ####
  if(!not_exist_or_na(dt, "PIT_deployed")) {

    # Make sure PIT tag exists in database
    create_tag_if_needed(dt, ch)

    message("\tCreating tblTagDeployment record...")
    strsql <- with(dt, paste0("INSERT INTO tblTagDeployment ( [TagID], [DeployCap]) SELECT ",
                    "'", PIT_deployed, "' AS e1, ",
                    captureID, " AS e2;"))
    if (!try_insert(ch, strsql)) return()
    message("done\n")
  } else {
    message("\tNo PIT tag deployed.\n")
  }

  #### Deal with PIT tag removal
  if(!not_exist_or_na(dt, "PIT_removed")) {
    # Look up the tag deployment record
    strsql <- paste0("SELECT tblTagDeployment.ID FROM tblTagDeployment WHERE ",
      "(((tblTagDeployment.TagID)='", dt$PIT_removed, "'));")
    res <- RODBC::sqlQuery(ch, strsql) %>% ensure_data_is_returned
    deplID <- res$ID

    if (nrow(res) == 0) {
      message("Trying to remove PIT tag ", PIT_removed, ", but cannot find a ",
              "record of it's deployment! PIT tag not removed!")
    } else {
      # set the RemoveCap field to captureID
      strsql <- paste0("UPDATE tblTagDeployment SET tblTagDeployment.RemoveCap = ",
                       captureID, " WHERE (((tblTagDeployment.ID)=", deplID, "));")
      if (!try_insert(ch, strsql)) return()
    }
  } else {
    message("\tNo PIT tag removed.\n")
  }
}
