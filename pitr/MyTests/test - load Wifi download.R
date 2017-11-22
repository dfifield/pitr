library(tidyverse)
library(pitr)
library(RODBC)
library(magrittr)

data_sets <- "C:/Users/fifieldd/Documents/Offline/Projects/Burrow_Logger/Data/2017/Downloads"

files = c("gipi1_report_2017_08_28_13_00_03.txt")
files = file.path(data_sets, files)

mych <- pitdb_open("C:/Users/fifieldd/Documents/Offline/Projects/Burrow_Logger/Data/Burrow logger master0.1.accdb")

files %>%
  walk(function(.x, ch = mych) {pitdb_load_file(ch = mych, filename = .x,
                                                ignore_insert_errors = TRUE,
                                                record_non_reporters = TRUE,
                                                parse_summary = TRUE,
                                                verbose = TRUE)})


pitdb_close(ch)
