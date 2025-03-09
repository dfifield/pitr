
`%||%` <- function(a, b) if (!is.null(a)) a else b
is.not.null <- function(x) ! is.null(x)


# Parse datafile date.
#
#Parses a data download filename and extracts the date.
#param filename Text string giving filename with or without full pathname.
#param ... Extra arguments passed to \code{lubridate::ymd}.
# Used to extract the data download date from a filename with the following form:  \bold{gipi1_report_2017_06_30_13_52_31.txt}.
#returns a date in \code{POSIXct} format.
#
parse_date_from_string <- function(filename, ...){

  # pattern to find a date and time anywhere in string
  pat <- ".*((?:19|20)[0-9][0-9])[-_/.](0[1-9]|1[012])[-_/.](0[1-9]|[12][0-9]|3[01]).*"

  grepl(pat, filename) || stop(paste0("Could not extract date from: '", filename,"'"))

  lubridate::ymd(gsub(pat, "\\1_\\2_\\3", filename, perl = T), ...)
}


# Extract plot number from filename if possible
#
#
parse_plot_from_name <- function(filename, ...){

  # pattern to find plot number
  pat <- "^gipi([0-9]+).*"

  grepl(pat, basename(filename)) || stop(paste0("Could not extract plot number from: '", filename,"'"))

  as.integer(gsub(pat, "\\1", basename(filename)))
}

# get the autonumber field ID of last inserted record
get_sql_ID <- function(ch){
  ret <- RODBC::sqlQuery(ch, "SELECT @@IDENTITY;") %>% ensure_data_is_returned %>% ensure_one_row_returned
  ret[1,1]
}

# print out a complete tibble
print_tibble <- function(tb){

  nrow(tb) == 0 && return()

  cat("\n")
  dplyr::as_tibble(tb) %>%
    print(n = nrow(tb), width = Inf)
  cat("\n")
}


# encode note field
do_note <- function(s, def = NULL) {

  if (is.na(s)){
    if (is.null(def))
      "Null"
    else
      paste0("'", def, "'")
  } else if (!is.null(def)) {
      paste0("'", def, "; ", gsub("'", "_", s), "'")
  } else
      paste0("'", gsub("'", "_", s), "'")
}


# Filter out (ie keep) any rows (tag detections) that occur during a deployment
# of this board. Deployments that have not yet ended (ie ToDate is NA) are also
# handled. called once from per_dataclass_filter for each board in data with
# brd_df containing all rows for that board. my depl is table of all board
# deployments. Note the use of as.character() to avoid any funky timezone
# conversions when as.Date is called with a POSIXct date. In that case,
# dateTimes with time > 21:30 were being converted to the following day.
per_board_filter <- function(brd_df, mydepl, debug = FALSE){

  # make sure there is a column named "dateTime" (bad_recs don't have one). Just
  # return dataframe if not
  "dateTime" %in% colnames(brd_df) || return(brd_df)

  # get deployments for this board
  mydepl <- dplyr::filter(mydepl, BoardID == unique(brd_df$BoardID))

  if(debug) {
    print(paste0("Calling per_board_filter for board ", unique(brd_df$BoardID)))
    print("mydepl =")
    print(mydepl)
    print("Candidate rows to keep:")
    dplyr::as_tibble(brd_df) %>%
      print(n = nrow(brd_df))
  }
  # for each deployment, filter on dates
  filt <- mydepl %>%
    tibble::rowid_to_column() %>%
    split(.$rowid) %>%
    purrr::map_dfr(~ dplyr::filter(brd_df, dplyr::between(dateTime, .$FromDate,  .$ToDate) |
                                    (is.na(.$ToDate) & .$FromDate <= dateTime)
                      )
    )

  ensurer::ensure_that(filt, nrow(.) <= nrow(brd_df), err_desc = "More records after filtering than before!")

  if (debug) {
    print("returning:")
    dplyr::as_tibble(filt) %>%
      print(n = nrow(filt))
  }

  filt
}

# called from pitdb_load_file once for each element of the data list (ie.
# tag_reads, statuses, uploads, bad_recs) mydepl is table of all board
# deployments
per_dataclass_filter <- function(df, mydepl) {
  df %>%
  split(.$BoardID) %>%
  purrr::map_dfr(per_board_filter, mydepl = mydepl)
}


# print out records from a download file with the name of the record type (tag_read, status, etc) if recs is not empty or NULL
print_recs <- function(recs, nm, debug = FALSE) {
  if (debug) browser()
  if(is.not.null(recs) && nrow(recs) > 0) {
    cat(sprintf("\n%s:\n", nm))
    print(recs)
    cat("\n")
  }
}


# auxilliary function to deal with input fields that can be NA
do_na <- function(dat) {
  if(is.na(dat))
    "Null"
  else if (is.character(dat))
    paste0("'", dat, "'")
  else if (inherits(dat, "POSIXct"))
    paste0("#", format(dat, format = "%Y-%b-%d %H:%M:%S"), "#")
  else
    dat
}

# Return true if a dataframe column doesn't exist or is NA
not_exist_or_na <- function(dat, colnm) {
  if (!(colnm %in% colnames(dat)) || is.na(dat[, colnm])) TRUE
  else FALSE
}

# return value of a dataframe column or NA if it either doesn't exist or is NA
val_or_NA <- function(dat, colnm) {
  if (not_exist_or_na(dat, colnm)) NA
  else dplyr::pull(dat, colnm)
}


# Either find a bird record in tblBirds or create one from data in dt.
# Return the tblBirds record ID
#
# If this bird is not already in \code{tblBirds}), then a new record is
# created in \code{tblBirds} but this
# requires fields for \code{AgeAtBanding, Species, and Colony} and optionally
# \code{Sex} and \code{BirdNote}. If the bird is already in \code{tblBirds} then
# these fields are ignored.
find_or_create_bird <- function(dt, ch) {

  # Check if band exists
  strsql <- paste0("SELECT tblBands.BandNo FROM tblBands WHERE ",
                   "(((tblBands.BandNo)='", dt$Band, "'));")
  res <- RODBC::sqlQuery(ch, strsql) %>% ensure_data_is_returned
  if (nrow(res) == 0) {
    # Create tblBands record.
    message("\tCreating tblBands record...")
    strsql <- paste0("INSERT INTO tblBands ( [BandNo]) SELECT ",
                     "'", dt$Band, "' AS e1;")
    if (!try_insert(ch, strsql)) return()
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
    if (!try_insert(ch, strsql)) return()
    message("done\n")

    # get bird ID
    birdID <- get_sql_ID(ch)

    # Deal with filling in nest attempt record for a chick.
    # Not sure if whole nest attempt thing should go away.
    if (!not_exist_or_na(dt, "Age") && dt$Age == "C") {
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

  birdID
}

# Check if a burrow record exists and if not create it from fields in dt.
create_burrow_if_needed <- function(dt, ch) {
  # Deal with Burrow
  # Check if burrow exists
  strsql <- paste0("SELECT tblBurrow.BurrowID FROM tblBurrow WHERE ",
                   "(((tblBurrow.BurrowID)='", dt$Burrow, "'));")
  res <- RODBC::sqlQuery(ch, strsql) %>% ensure_data_is_returned
  if (nrow(res) == 0) {
    # Create tblBurrow record.
    message("\tCreating tblBurrow record...")
    strsql <- paste0("INSERT INTO tblBurrow ( [BurrowID], [Plot], [Habitat], [Notes])",
                     " SELECT ",
                     "'", dt$Burrow, "' AS e1,",
                     dt$Plot, " AS e2,",
                     do_na(val_or_NA(dt, "Habitat")), " AS e3, ",
                     do_na(val_or_NA(dt, "BurrowNotes")), " AS e4;"
    )
    if (!try_insert(ch, strsql)) return()
    message("done\n")
  }
}


# Check if a board record exists and if not create it from fields in dt.
create_board_if_needed <- function(dt, ch) {

  # Check if board exists
  strsql <- paste0("SELECT tblBoard.BoardID FROM tblBoard WHERE ",
                   "(((tblBoard.BoardID)=", dt$BoardID, "));")
  res <- RODBC::sqlQuery(ch, strsql) %>% ensure_data_is_returned
  if (nrow(res) == 0) {
    # get FlashSize ID
    strsql <- paste0("SELECT tblFlashSize.ID FROM tblFlashSize ",
                     "WHERE (((tblFlashSize.FlashSize)=", dt$FlashSize, "));")
    res <- RODBC::sqlQuery(ch, strsql) %>% ensure_data_is_returned %>%
      ensure_one_row_returned
    FlashSizeID <- res$ID

    # Create tblBoard record.
    message("\tCreating tblBoard record...")
    strsql <- paste0("INSERT INTO tblBoard ( [BoardID], [Revision], [FlashSize])",
                     " [Status] SELECT ",
                     dt$BoardID, " AS e1,",
                     dt$Revision, " AS e2,",
                     FlashSizeID, " AS e3,",
                     "'", dt$Status, "' AS e4;",
    )
    if (!try_insert(ch, strsql)) return()
    message("done\n")
  }
}

# Check if dt$PIPT_deployed exists in tblTags and create if not.
create_tag_if_needed <- function(dt, ch) {
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
      if (!try_insert(ch, strsql)) return()
      message("done\n")
    }
  }
}


try_insert <- function(ch, strsql) {
  res <- result <- try({
    RODBC::sqlQuery(ch, strsql) %>% ensure_insert_success
  }, silent = TRUE)

  if (class(result) == "try-error") {
    err <- geterrmessage()
    message("Inserting record failed with following message:\n", err)
    FALSE
  } else
    TRUE
}

# Set any tibbles in list dat with zero records left to NULL
make_empty_tibbles_null <- function(dat) {
  dat %>% purrr::map(function(dfr) {
    if (!is.null(dfr) && nrow(dfr) == 0) NULL
    else dfr
  })
}
