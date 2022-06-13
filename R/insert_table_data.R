
#####
# Insert a tag read into one of the tag read tables
#
# ch - open RODBC channel
# import_ID - ID from tblImports
# read_type - either "Web", "Test", "Unknown" or "Test_board"
#     only used if whch_table == "tblOtherTagRead"
# ignore_errors - ignore insertion errors (which normally cause a fatal error).
# verbose - Passed on to do_insert
#
#
insert_table_data <- function(dat = NULL, ch = NULL, whch_table = NULL, import_ID = NULL, read_type = NULL, ignore_errors = FALSE,
                             verbose = FALSE) {

  ensurer::ensure_that(dat, is.data.frame(.))
  ensurer::ensure_that(whch_table, is.character(.))
  ensurer::ensure_that(ch, class(ch) == "RODBC")

  if (nrow(dat) == 0) {
    #warning(sprintf("Tag_dat for read_type '%s' is empty. Not inserting anything.", read_type), immediate. = TRUE)
    return(invisible(logical()))
  }

  # attempt to insert each record. returns a vector of T/F: T if load succeeded otherwise F, for each record.
  res <- dat  %>%
    tibble::rowid_to_column() %>%
    split(.$rowid) %>%
    purrr::map_lgl(do_insert, ch = ch, strsql = strsql, import_ID = import_ID, whch_table = whch_table, read_type = read_type,
                   ignore_errors = ignore_errors, verbose = verbose)
}
