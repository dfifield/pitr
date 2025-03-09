library(tidyverse)
library(pitr)
library(RODBC)
library(magrittr)

# data_sets <- "C:/Users/fifieldd/Documents/Offline/Projects/Burrow_Logger/Data/2017/Downloads"
data_sets <- "C:/Users/fifieldd/Documents/Offline/Projects/Burrow_Logger/Data/2017/Direct_connect_downloads"

files <- "P41_20171120_160558_UTC.txt"
# files = c("gipi1_report_2017_08_28_13_00_03.txt", "gipi2_report_2017_08_28_13_00_04.txt", "gipi3_report_2017_08_28_13_00_04.txt", "gipi4_report_2017_08_28_13_00_02.txt", "gipi5_report_2017_08_28_13_00_16.txt")
files = file.path(data_sets, files)

mych <- pitdb_open("C:/Users/fifieldd/Documents/Offline/Projects/Burrow_Logger/Data/Burrow logger master0.1.accdb")

files %>%
  walk(function(.x, ch = mych) {pitdb_load_file(ch = mych, filename = .x, fetch_type = "CableConnect",
                                                ignore_insert_errors = TRUE,
                                                record_non_reporters = FALSE,
                                                parse_summary = TRUE,
                                                verbose = TRUE)})

  # walk(function(.x, ch = mych) {pitdb_load_file(ch = mych, filename = .x, fetch_type = "CableConnect",
  #                                               to_date = lubridate::ymd("2017-11-17"),
  #                                               from_date = lubridate::ymd("2017-07-11"),
  #                                               ignore_insert_errors = TRUE,
  #                                               record_non_reporters = FALSE,
  #                                               parse_summary = TRUE,
  #                                               verbose = FALSE)})


# sqlQuery(ch, "INSERT INTO tblImports ( [DateTime], Notes, FetchType ) SELECT #9/8/2017# AS Expr1, 'fourth' AS EXPR2, 2 AS Expr3")
# res <- sqlQuery(ch, "SELECT @@IDENTITY")

pitdb_close(mych)
