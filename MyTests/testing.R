library(pitr)
library(RODBC)
library(magrittr)

# data_sets <- "C:/Users/fifieldd/Documents/Offline/Projects/Burrow_Logger/Data/2017/Downloads"
data_sets <- "C:/Users/fifieldd/Documents/Offline/Projects/Burrow_Logger/Data/2017/Direct_connect_downloads"

files <- "P41_20171120_160558_UTC.txt"
# files = c("gipi1_report_2017_08_28_13_00_03.txt", "gipi2_report_2017_08_28_13_00_04.txt", "gipi3_report_2017_08_28_13_00_04.txt", "gipi4_report_2017_08_28_13_00_02.txt", "gipi5_report_2017_08_28_13_00_16.txt")
files = file.path(data_sets, files)

#ch <- pitdb_open("C:/Users/fifieldd/Documents/Offline/Projects/Burrow_Logger/Software/pitr/tests/testdb.accdb")
mych <- pitdb_open("C:/Users/fifieldd/Documents/Offline/Projects/Burrow_Logger/Data/Burrow logger master0.1.accdb")

x <- pitdb_parse_bird_report_file(files[1], fetch_type = "CableConnect")

files %>%
  walk(function(.x, ch = mych) {pitdb_parse_bird_report_file(.x, verbose = T) %>% pitdb_summarize_parsed_file(ch = ch, verbose = F)})


# sqlQuery(ch, "INSERT INTO tblImports ( [DateTime], Notes, FetchType ) SELECT #9/8/2017# AS Expr1, 'fourth' AS EXPR2, 2 AS Expr3")
# res <- sqlQuery(ch, "SELECT @@IDENTITY")

pitdb_close(ch)
