library(tidyverse)
library(pitr)
library(RODBC)
library(magrittr)

# data_sets <- "C:/Users/fifieldd/Documents/Offline/Projects/Burrow_Logger/Data/2017/Downloads"
data_sets <- "C:/Local files/Burrow_logger/Data/2022/Plot4"

files = c("gipi4_report_2022_06_23_06_10_17.txt")
files = file.path(data_sets, files)

mych <- pitdb_open("C:/Local files/Burrow_logger/Data/Burrow logger master_test0.1.accdb")

files %>%
  walk(function(.x, ch = mych) {pitdb_load_file(ch = mych, filename = .x,
                                                ignore_insert_errors = TRUE,
                                                record_non_reporters = TRUE,
                                                parse_summary = TRUE,
                                                verbose = TRUE)})


pitdb_close(ch)
