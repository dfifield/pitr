library(pitr)
library(RODBC)

ch <- pitdb_open("C:/Users/fifieldd/Documents/Offline/Projects/Burrow_Logger/Software/pitr/tests/testdb.accdb")
#ch <- pitdb_open("C:/Users/fifieldd/Documents/Offline/Projects/Burrow_Logger/Data/Burrow logger master0.1.accdb")

filename <-  "C:/Users/fifieldd/Documents/Offline/Projects/Burrow_Logger/Data/2017/Downloads/gipi1_report_2017_08_08_13_00_09.txt"
ret <- pitdb_parse_bird_report_file(filename, verbose = T)
pitdb_summarize_parsed_file(ret, ch = ch, verbose = F)



sqlQuery(ch, "INSERT INTO tblImports ( [DateTime], FetchType ) SELECT #9/8/2017# AS Expr1, \"fourth\" AS EXPR2, 2 AS Expr3')

RODBC::odbcClose(ch)
