library(pitr)
library(RODBC)

#ch <- pitdb_open("C:/Users/fifieldd/Documents/Offline/Projects/Burrow_Logger/Software/pitr/R/testdb.accdb")
ch <- pitdb_open("C:/Users/fifieldd/Documents/Offline/Projects/Burrow_Logger/Data/Burrow logger master0.1.accdb")

sp_tags <- RODBC::sqlFetch(ch, "lkpSpecialTags", as.is = T)
filename <-  "C:/Users/fifieldd/Documents/Offline/Projects/Burrow_Logger/Data/2017/Downloads/gipi1_report_2017_08_08_13_00_09.txt"
ret <- pitdb_parse_bird_report_file(filename, verbose = T)
RODBC::odbcClose(ch)
