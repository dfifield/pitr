
# Import one or more files into the database by rendering.
#
# files - files to import (including full pathname)
# report_path - where to store the resulting of of rendering the import .RMD
do_import <- function(files = NULL, report_path = NULL) {



  # XXXXX THIS IS ONLY PART DONE.
  # XXXXX NEED TO EITHER RENDER "Import 2018 download data.Rmd" IN THE DATA PROCESING FOLDER OR PUT IT IN PITR AND GET IT FROM THERE
  #
  # NOTE NEED TO DO THIS IN A WALK (ONE FILE AT A TIME) IN ORDER to get individual output htmls. the RMD template
  # passing it all files (it can handle that) and choose an output file just based on date and time. I think I like that best....
  #
  # see how to include the RMD in the package here:
  #https://stackoverflow.com/questions/30377213/how-to-include-rmarkdown-file-in-r-package


  !is.null(files) || return()
  !is.null(report_pat) || stop("db_import: no report_path specified.")

  # needed to make render() happy
  Sys.setenv(RSTUDIO_PANDOC = "C:/Program Files/RStudio/bin/pandoc/")

  # XXXXX need to fix this if files is a vector or it will give multiple results
  output_file = gsub("[- :]", "_", sprintf("%s_processed_%s.html", substr(basename(files), 1, nchar(basename(files)) - 4), Sys.time()))

  rmarkdown::render(
    input = system.file("import_template.RMD", package = "pitr"),
    output_dir = report_path,
    output_file = output_file,
    params = list(files = files),
    knit_root_dir = "C:/Users/fifieldd/Documents/Offline/Projects/Burrow_Logger/Software/data processing/R/")

  # wait a random amount of time before sending the email. Previous expereince indicates that sometimes this script runs but the email
  # does not get sent. At  least for plot 1 on Oct 11.Proceeding on the assumption that two invocations happening close in time
  # may cause problems...
  delay <- sample(1:90)[1]
  print(paste0("Delayng for ", delay, " seconds before sending email."))
  Sys.sleep(delay)

  # email the output as html and also attach the html file.
  email <- send.mail(from = "gull.island.nl@gmail.com",
            to = c("dave.fifield@canada.ca"),
            subject = sprintf("Import results for %s", basename(files)),
            body = file.path(output_dir, output_file),
            html = TRUE,
            smtp = list(host.name = "smtp.gmail.com", port = 465, user.name = "gull.island.nl", passwd = "petrelpass", ssl = TRUE),
            attach.files = file.path(output_dir, output_file),
            authenticate = TRUE,
            send = TRUE,
            debug = FALSE)

}
