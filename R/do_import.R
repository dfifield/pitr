#'@export
#'@title Import data file(s) into database
#'
#'@description Import one or more files into the database by rendering import_template.RMD included with the package.

#'@param db The pathname to the Microsoft Access database to insert downloaded files into.
#'@param files files to import (including full pathname)
#'@param report_path The pathname to the folder where rendered output should be saved.
#'@details This function renders and existing RMD template \code{import_template.RMD} which causes \code{files} to be imported  to \code{db}.
#'It is called by \code{pitdb_process_data_downloads} (which is used by the \code{import server} to regularly import downloaded data.) It can
#'also be called directly.
#'
#'@section Author: Dave Fifield
pitdb_do_import <- function(db = NULL, files = NULL, report_path = NULL) {

  !is.null(db) || stop("pitdb_do_import: no db specified.")
  !is.null(files) || return()
  !is.null(report_path) || stop("pitdb_do_import: no report_path specified.")

  # needed to make render() happy
  Sys.setenv(RSTUDIO_PANDOC = "C:/Program Files/RStudio/bin/quarto/bin/")
  #Sys.setenv(RSTUDIO_PANDOC = "C:/Program Files/RStudio/bin/pandoc/")

  rmarkdown::render(
    input = system.file("import_template.RMD", package = "pitr"),
    output_dir = report_path,
    output_file = gsub("[- :]", "_", sprintf("Automated_data_import_%s.html", Sys.time())),
    params = list(files = files, db = db),
  )
#
#   # wait a random amount of time before sending the email. Previous expereince indicates that sometimes this script runs but the email
#   # does not get sent. At  least for plot 1 on Oct 11.Proceeding on the assumption that two invocations happening close in time
#   # may cause problems...
#   delay <- sample(1:90)[1]
#   print(paste0("Delayng for ", delay, " seconds before sending email."))
#   Sys.sleep(delay)
#
#   # email the output as html and also attach the html file.
#   email <- send.mail(from = "gull.island.nl@gmail.com",
#             to = c("dave.fifield@canada.ca"),
#             subject = sprintf("Import results for %s", basename(files)),
#             body = file.path(output_dir, output_file),
#             html = TRUE,
#             smtp = list(host.name = "smtp.gmail.com", port = 465, user.name = "gull.island.nl", passwd = "petrelpass", ssl = TRUE),
#             attach.files = file.path(output_dir, output_file),
#             authenticate = TRUE,
#             send = TRUE,
#             debug = FALSE)

}
