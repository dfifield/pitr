
`%||%` <- function(a, b) if (!is.null(a)) a else b


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
