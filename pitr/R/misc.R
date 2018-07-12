
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
  print(tb, n = nrow(tb), width = Inf)
  cat("\n")
}


# encode note field
do_note <- function(s, def = NULL) {

  if (is.na(s)){
    if (is.null(def)) {
      "Null"
    } else {
      paste0("'", def, "'")
    }
  } else {
      paste0("'", def, "; ", gsub("'", "_", s), "'")
  }
}


# Filter out (ie keep) any rows (tag detections) that occur during a deployment of this board. Deployments
# that have not yet ended (ie ToDate is NA) are also handled.
# called once from per_dataclass_filter for each board in data with brd_df containing all rows for that board.
# my depl is table of all board deployments.
# Note the use of as.character() to avoid any funky timezone conversions when as.Date is called with a POSIXct date.
# In that case, dateTimes with time > 21:30 were being converted to the following day.
per_board_filter <- function(brd_df, mydepl, debug = FALSE){
  # get deployments for this board
  mydepl <- filter(mydepl, BoardID == unique(brd_df$BoardID))

  if(debug) {
    print(paste0("Calling per_board_filter for board ", unique(brd_df$BoardID)))
    print("mydepl =")
    print(mydepl)
    print("Candidate rows to keep:")
    print(brd_df, n = nrow(brd_df))
  }
  # for each deployment, filter on dates
  x <- mydepl %>%
    rowid_to_column() %>%
    split(.$rowid) %>%
    map_dfr(~ dplyr::filter(brd_df, between(as.Date(as.character(dateTime)),
                                            as.Date(as.character(.$FromDate)),
                                            as.Date(as.character(.$ToDate))) ||
                                    (is.na(.$ToDate) && as.Date(as.character(.$FromDate)) <= as.Date(as.character(dateTime)))
                      )
              )
  if (debug) {
    print("returning:")
    print(x, n = nrow(x))
  }

  x
}

# called from pitdb_load_file once for each element of the data list (ie. tag_reads, statuses, uploads, bad_recs)
# my depl is table of all board deployments
per_dataclass_filter <- function(df, mydepl) {
  df %>%
  split(.$BoardID) %>%
  map_dfr(per_board_filter, mydepl = mydepl)
}


