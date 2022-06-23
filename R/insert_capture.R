#'@export
#' @importFrom magrittr %>%
#'@title Insert capture data
#'
#'@description The function inserts a single capture record into the database.
#'
#'@param dt A dataframe containing at least \code{Band, Burrow, Date} and
#'  optionally \code{Colony, Species, Age, TotW, BagW, Tarsus, PIT_deployed,
#'  PIT_removed}.
#'@param ch Open RODBC channel connecting to the database
#'
#'@details Creates or updates appropriate entries in tblBirds, tblBands, tblTags,
#'  tblNestAttempt, tblCapture, tblMorpho, and tblTagDeployment.
#'
#'  If this is a recapture of a previously banded bird (ie. it is already in
#'  \code{tblBirds}), then a new record is not created in \code{tblBirds} and thus
#'  \code{Age, Sex, Species, and Colony} are ignored.
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
#'@return Nothing.
#'@section Author: Dave Fifield

pitdb_insert_capture <- function(dt, ch){

  nrow(dt) == 1 || stop(
    sprintf("pitdb_insert_capture: dt contains %d rows, should be 1", nrow(dt)))

  !is.null(ch) || stop("parameter ch is missing.")

  message(sprintf("Inserting record for %s in  %s...\n", dt$Band, dt$Burrow))


  # Check if band exists
  strsql <- paste0("SELECT tblBands.BandNo FROM tblBands WHERE ",
                   "(((tblBands.BandNo)='", dt$Band, "'));")
  res <- RODBC::sqlQuery(ch, strsql) %>% ensure_data_is_returned
  if (nrow(res) == 0) {
    # Create tblBands record.
    message("\tCreating tblBands record...")
    strsql <- paste0("INSERT INTO tblBands ( [BandNo]) SELECT ",
                     "'", dt$Band, "' AS e1;")
    res <- RODBC::sqlQuery(ch, strsql) %>% ensure_insert_success
    message("done\n")
  }


  # Check if bird already exists. It may not exist even if a tblBands record exists.
  # Also, birds can be captured multiple times, so the bird record could already exist
  strsql <- paste0("SELECT tblBirds.BandNo, tblBirds.ID FROM tblBirds WHERE ",
    "(((tblBirds.BandNo)='", dt$Band, "'));")
  res <- RODBC::sqlQuery(ch, strsql) %>% ensure_data_is_returned
  if (nrow(res) == 0) {

    # create a tblBirds record
    message("\tCreating tblBirds record...")

    # Get species ID
    if (not_exist_or_na(dt, "Species")) {
      strSpec <- "Leach's Storm-Petrel"
    } else {
      strSpec <- dt$Species
    }

    # Escape single quotes in species name with two single quotes
    strSpec <- stringr::str_replace_all(strSpec, "'", "''")

    # Lookup species
    strsql <- paste0("SELECT tblSpeciesInfo.SpecInfoID FROM tblSpeciesInfo ",
                     "WHERE (((tblSpeciesInfo.English)='", strSpec, "'));")
    res <- RODBC::sqlQuery(ch, strsql) %>% ensure_data_is_returned %>%
      ensure_one_row_returned
    specID <- res$SpecInfoID

    # Get colony
    if (not_exist_or_na(dt, "Colony")) {
      strColony <- "Gull Island"
    } else {
      strColony <- dt$Colony
    }

    # Escape single quotes in species name with two single quotes
    strColony <- stringr::str_replace_all(strColony, "'", "''")

    # Lookup colony
    strsql <- paste0("SELECT lkpColony.ID FROM lkpColony ",
                     "WHERE (((lkpColony.ColonyName)='", strColony, "'));")
    res <- RODBC::sqlQuery(ch, strsql) %>% ensure_data_is_returned %>%
      ensure_one_row_returned
    colID <- res$ID

    # Get sex
    if (not_exist_or_na(dt, "Sex")) {
      strSex <- "U"
    } else {
      strSex <- dt$Sex
    }

    # Get Age - assume adult by default
    if (not_exist_or_na(dt, "Age")) {
      strAge <- "A"
    } else {
      strAge <- dt$Age
    }

    # Insert tblBirds record
    strsql <- paste0("INSERT INTO tblBirds ( [BandNo], [Species], [Sex], ",
        "[Colony], [AgeAtBanding]) SELECT ",
        "'", dt$Band, "' AS e1, ",
        specID, " AS e2, ",
        "'", strSex, "' AS e3, ",
        colID, " AS e4, ",
        "'", strAge, "' AS e5;")
    res <- RODBC::sqlQuery(ch, strsql) %>% ensure_insert_success
    message("done\n")

    # get bird ID
    birdID <- get_sql_ID(ch)

    # Deal with filling in nest attempt record for a chick.
    # Not sure if whole nest attempt thing should go away.
    if (dt$Age == "C") {
      # Fill in chick ID of the nest attempt
      #### look for nest attempt record ####
      message("\tBird is a chick...finding tblNestAttempt record...")
      strsql <- with(dt, paste0("SELECT tblNestAttempt.NestAttmpID FROM ",
                                "tblNestAttempt WHERE (((tblNestAttempt.BurrowID) = '", Burrow,
                                "') AND ((tblNestAttempt.Yr) = ", lubridate::year(Date),"));"))
      res <- RODBC::sqlQuery(ch, strsql) %>% ensure_data_is_returned %>%
        warn_no_nest_attempt()
      message("done\n")

      # did we find a nesting attempt?
      # If so fill in the chick id
      if (is.data.frame(res)){
        message("\tUpdating chick ID in tblNestAttempt record...")
        strsql <- with(dt, paste0("UPDATE tblNestAttempt SET tblNestAttempt.Chick = ", birdID,
                                  " WHERE (((tblNestAttempt.BurrowID) = '", Burrow, "'));"))
        res <- RODBC::sqlQuery(ch, strsql) %>% ensure_insert_success
        message("done\n")
      } else {
        message("\tNo tblNestAttempt record found.")
      }
    }
  } else {
    # Bird already exists
    birdID <- res$ID
  }

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
                      Note = val_or_NA(dt, "Note"))

  #### create a tblCapture record ####
  message("\tCreating tblCapture record...")
  strsql <- with(dt, paste0("INSERT INTO tblCapture ( [BirdID], [Date], ",
                  "[BurrowID], [CaptureType], [Note]) SELECT ",
                  birdID, " AS e1, ",
                  "#", format(Date, format = "%Y-%b-%d"), "# AS e2, ",
                  "'", Burrow, "' As e3, ",
                  capID, " AS e4, ",
                  do_note(strNote), " AS e5;"))
  res <- RODBC::sqlQuery(ch, strsql) %>% ensure_insert_success
  message("done\n")

  # get Capture ID
  captureID <- get_sql_ID(ch)

  # Only create morphology record if there is some...
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
    res <- RODBC::sqlQuery(ch, strsql) %>% ensure_insert_success
    message("done\n")
  }

  #### Look up tblTag record ####
  strsql <- paste0("SELECT tblTags.TagID FROM tblTags WHERE (((tblTags.TagID)='",
                   dt$PIT_deployed, "'));")
  res <- RODBC::sqlQuery(ch, strsql) %>% ensure_data_is_returned

  if (nrow(res) == 0) {
    message("\tCreating tblTags record...")

    strManufac <- val_or_NA(dt, "Manufac")
    strTagType <- val_or_NA(dt, "TagType")

    # Make sure manufacturer and TagType are provided
    if (is.na(strManufac) || is.na(strTagType)) {
      message("One or both of Manufacurer or TagType is missing! ",
        "Cannot add PIT tag to tblTags nor deploy on bird.")
    } else {
      # Look up manufacturer ID
      strsql <- paste0("SELECT lkpManufacturers.ID FROM lkpManufacturers ",
                       "WHERE (((lkpManufacturers.ManufName)='", strManufac, "'));")
      res <- RODBC::sqlQuery(ch, strsql) %>% ensure_data_is_returned %>%
        ensure_one_row_returned
      manufacID <- res$ID

      # Look up tagtype
      strsql <- paste0("SELECT lkpTagTypes.ID FROM lkpTagTypes ",
                       "WHERE (((lkpTagTypes.TagType)='", strTagType, "'));")
      res <- RODBC::sqlQuery(ch, strsql) %>% ensure_data_is_returned %>%
        ensure_one_row_returned
      tagtypeID <- res$ID

      # insert tag into database - quick and dirty should be looked up.
      strsql <- paste0("INSERT INTO tblTags ( [TagID], [Manufac], [Type]) SELECT ",
                    "'", dt$PIT_deployed, "' AS e1,",
                    manufacID, " AS e2, ",
                    tagtypeID, " AS e3; "
                    )
      res <- RODBC::sqlQuery(ch, strsql) %>% ensure_insert_success
      message("done\n")
    }
  }

  #### create a tblTagDeployment record ####
  if(!not_exist_or_na(dt, "PIT_deployed")) {
    message("\tCreating tblTagDeployment record...")
    strsql <- with(dt, paste0("INSERT INTO tblTagDeployment ( [TagID], [DeployCap]) SELECT ",
                    "'", PIT_deployed, "' AS e1, ",
                    captureID, " AS e2;"))
    res <- RODBC::sqlQuery(ch, strsql) %>% ensure_insert_success
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
      res <- RODBC::sqlQuery(ch, strsql) %>% ensure_insert_success
    }
  } else {
    message("\tNo PIT tag removed.\n")
  }
}
