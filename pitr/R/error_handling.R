sql_failure <- function(e){
  # fetch what sql* returned
  . <- get(".", parent.frame())

  mess <- sprintf("%s\nError message was:\n%s\n", e$message, paste(., collapse = ", "))

  stop(mess)
}

sql_warn <- function(e){
  # fetch what sql* returned
  . <- get(".", parent.frame())

  mess <- sprintf("Warning:\n%s\nError message was:\n%s\n", e$message, paste(., collapse = ", "))

  warning(mess, immediate. = True)
}

too_many_rows <- function(e) {
  # fetch what sql* returned
  . <- get(".", parent.frame())

  dat <- paste0(paste0(capture.output(.), collapes = "\n"), collapse = "")
  mess <- sprintf("%s\nIncorrect number of rows (%d) returned, expecting 1:\n%s", e$message, nrow(.), dat)

  stop(mess)
}

# used to ensure that all tag_reads are inserted into DB
tag_reads_not_inserted_warn <- function(e) {
  # fetch what sql* returned
  . <- get(".", parent.frame())

  dat <- paste0(paste0(capture.output(.), collapse = "\n"), collapse = "")
  mess <- sprintf("%s\nThe following tag_reads were not inserted (%d):\n%s", e$message, nrow(.), dat)

  stop(mess)
}


# print message regarding boards not reporting
boards_not_reporting_warn <- function(e) {
  # fetch the data
  . <- get(".", parent.frame())

  dat <- paste0(., collapse = ", ")
  mess <- sprintf("The following boards did not send data on this day: %s", dat)

  warning(mess, immediate. = TRUE)
}

ensure_data_is_returned <- ensurer::ensures_that(is.data.frame(.), fail_with = sql_failure)
ensure_insert_success <- ensurer::ensures_that(length(.) == 0, fail_with = sql_failure)
ensure_one_row_returned <- ensurer::ensures_that(nrow(.) == 1, fail_with = too_many_rows)
warn_insert_success <- ensurer::ensures_that(length(.) == 0, fail_with = sql_warn)
warn_tag_reads_not_inserted <- ensurer::ensures_that(nrow(.) == 0, fail_with = tag_reads_not_inserted_warn)
warn_non_reporting <- ensurer::ensures_that(length(.) == 0, fail_with = boards_not_reporting_warn)
