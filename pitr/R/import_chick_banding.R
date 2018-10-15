#'@export
#' @importFrom magrittr %>%
#'@title Import chick banding
#'
#'@description The function import a single chick banding record to the database.
#'
#'@param dt A dataframe containing at least fields: \code{Band, Burrow, Date, TotW, BagW, Tarsus, PIT, Contents}
#'@param ch Open RODBC channel connecting to the database
#'
#'@details Creates appropriate entries in tblBirds, tblNestAttempt, tblCapture, tblMorpho, tblTagDeployment, tblBurrowCheck
#'@note Currently assumes that all birds are at Gull Island, tags starting with 062 are Cyntag 10mm tags, otherwise they are CoreRFID 12mm tags.
#'@return Nothing.
#'@section Author: Dave Fifield

pitdb_insert_chick_banding <- function(dt, ch){

 !is.null(ch) || stop("parameter ch is missing.")

  cat(sprintf("Inserting chick banding record for %s in  %s...\n", dt$Band, dt$Burrow))

  #### create a tblBirds record ####
  if (!is.na(dt$Band)) {
    #### check if bird already exists ####
    strsql <- paste0("SELECT tblBirds.BandNo FROM tblBirds WHERE (((tblBirds.BandNo)='", dt$Band, "'));")
    res <- RODBC::sqlQuery(ch, strsql) %>% ensure_data_is_returned

    if (nrow(res) != 0){
      warning("This bird is already in database. Skipping.", immediate. = TRUE)
      return()
    }

    # This shouldn't really be done here... bands should exist in the database apriori
    cat("\tCreating tblBands record...")
    strsql <- paste0("INSERT INTO tblBands ( [BandNo]) SELECT ",
                    "'", dt$Band, "' AS e1;")
    res <- RODBC::sqlQuery(ch, strsql) %>% ensure_insert_success
    cat("done\n")


    #### create a tblBirds record ####
    cat("\tCreating tblBirds record...")
    strsql <- paste0("INSERT INTO tblBirds ( [BandNo], [Species], [Sex], [Colony], [AgeAtBanding]) SELECT ",
                    "'", dt$Band, "' AS e1, ",
                    1462, " AS e2, ",
                    "'U' AS e3, ",
                    "2 AS e4, ", # XXXGull Island. quick and dirty should really look this up.
                    "'C' AS e5;")
    res <- RODBC::sqlQuery(ch, strsql) %>% ensure_insert_success
    cat("done\n")

    # get bird ID
    birdID <- get_sql_ID(ch)

    # Fill in chick ID of the nest attempt
    #### look for nest attempt record ####
    cat("\tFinding tblNestAttempt record...")
    strsql <- with(dt, paste0("SELECT tblNestAttempt.NestAttmpID FROM tblNestAttempt WHERE (((tblNestAttempt.BurrowID) = '", Burrow,
                              "') AND ((tblNestAttempt.Yr) = ", lubridate::year(Date),"));"))
    res <- RODBC::sqlQuery(ch, strsql) %>% ensure_data_is_returned %>% warn_no_nest_attempt()
    cat("done\n")

    # did we find a nesting attempt?
    # If so fill in the chick id
    if (is.data.frame(res)){
      cat("\tUpdating chick ID in tblNestAttempt record...")
      strsql <- with(dt, paste0("UPDATE tblNestAttempt SET tblNestAttempt.Chick = ", birdID,
                        " WHERE (((tblNestAttempt.BurrowID) = '", Burrow, "'));"))
      res <- RODBC::sqlQuery(ch, strsql) %>% ensure_insert_success
      cat("done\n")
    }

    #### create a tblCapture record ####
    cat("\tCreating tblCapture record...")
    strsql <- with(dt, paste0("INSERT INTO tblCapture ( [BirdID], [Date], [CaptureType], [Note]) SELECT ",
                    birdID, " AS e1, ",
                    "#", format(Date, format = "%Y-%b-%d"), "# AS e2, ",
                    1, " AS e3, ",
                    do_note(Note, "Chick banding"), " AS e4;"))
    res <- RODBC::sqlQuery(ch, strsql) %>% ensure_insert_success
    cat("done\n")

    # get Capture ID
    captureID <- get_sql_ID(ch)

    #### create a tblMorpho record ####
    cat("\tCreating tblMorph record...")
    strsql <- with(dt, paste0("INSERT INTO tblMorpho ( [CaptureID], [BirdPlusBagWt], [BagWt], [BirdWt], [Wing], [Tarsus]) SELECT ",
                    captureID, " AS e1, ",
                    do_na(TotW), " AS e2, ",
                    do_na(BagW), " AS e3, ",
                    do_na(TotW) - BagW, " AS e4, ",
                    do_na(Wing), " AS e5, ",
                    do_na(Tarsus), " AS e6;"))
    res <- RODBC::sqlQuery(ch, strsql) %>% ensure_insert_success
    cat("done\n")

    #### create a tblTag record ####
    strsql <- paste0("SELECT tblTags.TagID FROM tblTags WHERE (((tblTags.TagID)='", dt$PIT, "'));")
    res <- RODBC::sqlQuery(ch, strsql) %>% ensure_data_is_returned

    if (nrow(res) == 0) {
      cat("\tCreating tblTags record...")

      # insert tag into database - quick and dirty should be looked up.
      strsql <- paste0("INSERT INTO tblTags ( [TagID], [Manufac], [Type]) SELECT ",
                    "'", dt$PIT, "' AS e1,",
                    ifelse(dat$PIT[1:3] == "062", 2, -1293520891L), " AS e2, ",
                    ifelse(dat$PIT[1:3] == "062", 2, -940491536L), " AS e3; "
                    )
      res <- RODBC::sqlQuery(ch, strsql) %>% ensure_insert_success
      cat("done\n")
    }


    #### create a tblTagDeployment record ####
    if(!is.na(dt$PIT)) {
      cat("\tCreating tblTagDeployment record...")
      strsql <- with(dt, paste0("INSERT INTO tblTagDeployment ( [TagID], [DeployCap]) SELECT ",
                      "'", PIT, "' AS e1, ",
                      captureID, " AS e2;"))
      res <- RODBC::sqlQuery(ch, strsql) %>% ensure_insert_success
      cat("done\n")
    } else {
      cat("\tNo PIT tag deployed.\n")
    }
  } else { # no band number - this is just a burrow check record
    cat("\tNo band number just filling in burrow check.\n")
  }


  #### check if nest check record already exists. This can happen if script was run on same data before and failed part way through####
  strsql <- paste0("SELECT tblBurrowCheck.BurrowID, tblBurrowCheck.Status, tblBurrowCheck.DateTime FROM tblBurrowCheck",
              " WHERE (((tblBurrowCheck.BurrowID)='", dt$Burrow, "') AND ((tblBurrowCheck.DateTime)=#",
              format(dt$Date, format = "%Y-%b-%d"),"#));")
  res <- RODBC::sqlQuery(ch, strsql) %>% ensure_data_is_returned

  if (nrow(res) != 0){
    warning("This burrow check is already in database. Skipping.", immediate. = TRUE)
    return()
  }

  #### create a tblBurrowCheck record ####
  cat("\tCreating tblBurrowCheck record...")
  strsql <- with(dt, paste0("INSERT INTO tblBurrowCheck ( [BurrowID], [Status], [DateTime], [Note]) SELECT ",
            "'", Burrow, "' AS e1, ",
            "'", Contents, "' AS e2, ",
            "#", format(dt$Date, format = "%Y-%b-%d"), "# AS e3, ",
            do_note(NA), " AS e4;"))
  res <- RODBC::sqlQuery(ch, strsql) %>% ensure_insert_success
  cat("done\nDone\n")
}


# auxilliary function to deal with input fields that can be NA
do_na <- function(dat) {
  if(is.na(dat)){
    "Null"
  } else {
    dat
  }
}
