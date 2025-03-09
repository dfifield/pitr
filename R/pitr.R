#' pitr: A package for manipulating data from PIT tag monitors.
#'
#'
#' blah blah blah
#'
#' Data formats for PIT tag monitor boards.
#'
#'  In most cases \code{inputfile} will contain info from multiple monitor
#'  boards, e.g., the daily files emailed from a Raspberry Pi base station.
#'
#'  talk about format of inputfile somewhere
#'
#'  deal with different data formats in 2016 and 2017
#'
#' @section Author: Dave Fifield
"_PACKAGE"

# Print message when user executes "library(pitr). Shamelessly borrowed from mgcv.
.onAttach <- function(...) {
  library(help=pitr)$info[[1]] -> version

  if (!is.null(version)) {
    version <- version[pmatch("Version",version)]
    um <- strsplit(version," ")[[1]]
    version <- um[nchar(um)>0][2]
  } else {
    version <- "Unknown version"
  }

  hello <- paste0("This is pitr ", version, ".")
  packageStartupMessage(hello)
}
