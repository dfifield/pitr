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
ensure_data_is_returned <- ensurer::ensures_that(is.data.frame(.), fail_with = sql_failure)
ensure_insert_success <- ensurer::ensures_that(length(.) == 0, fail_with = sql_failure)
ensure_one_row_returned <- ensurer::ensures_that(nrow(.) == 1, fail_with = too_many_rows)
warn_insert_success <- ensurer::ensures_that(length(.) == 0, fail_with = sql_warn)
