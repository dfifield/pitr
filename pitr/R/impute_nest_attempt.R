
#'@export
#'@title Parse data from a Raspberry Pi report file
#'
#'@description The function will create and/or fill in nest attempt records from info on band no, date and burrow.
#'
#'@param dt A dtaframe containing at least fields: \code{Band, Burrow, Date}
#'@param ch Open RODBC channel connecting to the database
#'
#'@details If no record currently exists in tblNestAttempt for this bird/burrow combination in this year, then a new record is created.
#'If a record already exists for this bird/year for another burrow then an error message is printed and the function returns.
#'Otherwise if a record already exists for this bird/year and the \code{Bird1} field is not already assigned to this bird
#' (which would produce an error) and the \code{Bird2} field is empty then the \code{Bird2} field is filled in with this bird's ID.
#'@return Nothing.
#'@section Author: Dave Fifield
#'
pitdb_impute_nest_attempt <- function(dt, ch) {

  !is.null(ch) || stop("parameter ch is missing.")

  cat(sprintf("Inserting nest attempt for %s...\n", dt$Band))

  # get birdID
  strsql <- with(dt, paste0("SELECT tblBirds.ID FROM tblBirds WHERE (((tblBirds.BandNo) = '", Band, "'));"))
  res <- RODBC::sqlQuery(ch, strsql) %>% ensure_data_is_returned %>% ensure_one_row_returned()
  birdID <- res$ID

  # Find any existing records for this year and bird in some other burrow. (Shouldn't happen - just being paranoid)
  strsql <- with(dt, paste0("SELECT tblNestAttempt.Bird1, tblNestAttempt.Yr, tblNestAttempt.Bird2, tblNestAttempt.BurrowID",
        " FROM tblNestAttempt",
        " WHERE (((tblNestAttempt.Bird1)=", birdID, ") ",
            " AND ((tblNestAttempt.Yr)=", lubridate::year(Date), ")",
            " AND ((tblNestAttempt.BurrowID)<>'", Burrow, "'))",
        " OR (((tblNestAttempt.Yr)=", lubridate::year(Date), ") ",
            " AND ((tblNestAttempt.Bird2)=", birdID, ") ",
            " AND ((tblNestAttempt.BurrowID)<>'", Burrow,"'));"
    )
  )
  res <- RODBC::sqlQuery(ch, strsql) %>% ensure_data_is_returned %>% warn_no_other_nest_attempt

  # abort if this bird already in a nestattempt in some other burrow this year
  if (nrow(res) > 0) return()

  cat(paste0("\tFinding tblNestAttempt record for ", dt$Band, "(ID = ", birdID,") in ", dt$Burrow, "..."))
  strsql <- with(dt, paste0("SELECT tblNestAttempt.* FROM tblNestAttempt WHERE (((tblNestAttempt.BurrowID) = '", Burrow,
                            "') AND ((tblNestAttempt.Yr) = ", lubridate::year(Date),"));"))
  res <- RODBC::sqlQuery(ch, strsql) %>% ensure_data_is_returned
  cat("done.\n")

  # no existing record?
  if (nrow(res) == 0){
    cat("\tCreating tblNestAttempt record...")
    strsql <- with(dt, paste0("INSERT INTO tblNestAttempt ( [BurrowID], [Bird1], [Yr]) SELECT ",
                    "'", Burrow, "' AS e1, ",
                    birdID, " AS e2, ",
                    lubridate::year(Date), " AS e3;"))
    res <- RODBC::sqlQuery(ch, strsql) %>% ensure_insert_success
    cat("done\n")
  } else { # record exists
    # Make sure existing bird2 is empty and bird1 is not already filed in with me.
    if (res$Bird1 != birdID && is.na(res$Bird2)) {
      cat("\tInserting into 'Bird2' slot ...")
      strsql <- paste0("UPDATE tblNestAttempt SET tblNestAttempt.Bird2 = ", birdID,
                        " WHERE (((tblNestAttempt.NestAttmpID) = ", res$NestAttmpID,"));")
      res <- RODBC::sqlQuery(ch, strsql) %>% ensure_insert_success
      cat("done.\n")
    } else {
      cat(sprintf("\tSomething is wrong. Not inserting. NestAttmpID %d: bird1 = %d, bird2 = %d\n", res$NestAttmpID, res$Bird1, res$Bird2))
    }
  }
  cat("Done\n")
}
