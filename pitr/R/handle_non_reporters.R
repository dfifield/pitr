
handle_non_reporters <- function(ch, import_ID, record_non_reporters, display_non_reporters, date_, dat, filename) {

  # This only makes sense to do for datafiles from base stations (ie. fetch_type == WiFi).
  # this is ensured by check at start pitdb_load_file.

  if (record_non_reporters) {
    # get fetch date
    date_ <- date_ %||% parse_date_from_string(filename)

    # try to figure out what plot this data comes from
    plotID <- parse_plot_from_name(filename)

    # get all boards active in this plot on this date
    strsql <- paste0("SELECT tblPlot.PlotID, tblBoardDeploy.BoardID, tblBoardDeploy.BurrowID, ",
                    " tblBoardDeploy.FromDate, tblBoardDeploy.ToDate FROM tblPlot INNER JOIN ",
                    " (tblBurrow INNER JOIN (tblBoard INNER JOIN tblBoardDeploy ON tblBoard.BoardID ",
                    " = tblBoardDeploy.BoardID) ON tblBurrow.BurrowID = tblBoardDeploy.BurrowID) ON ",
                    " tblPlot.PlotID = tblBurrow.Plot ",
                    " WHERE (((tblPlot.PlotID)=", plotID, ") AND ",
                    " ((tblBoardDeploy.FromDate)<=#", format(date_, format = "%Y-%b-%d"), "#) AND ",
                    " ((tblBoardDeploy.ToDate)>=#", format(date_, format = "%Y-%b-%d"), "# Or (tblBoardDeploy.ToDate) Is Null));")
    all_plot_brds <- RODBC::sqlQuery(ch, strsql) %>% ensure_data_is_returned
    active_brds <- all_plot_brds$BoardID %>% unique %>% warn_no_active_boards

    # get all boards reporting and non-reporting
    reporting <- dat %>%
      purrr::map("BoardID") %>%
      purrr::flatten_int() %>%
      unique %>%
      sort
    non_report <- dplyr::setdiff(active_brds, reporting) %>%
      sort

    # deal with non-reporting boards
    if (length(non_report) != 0){
      # display message if any missing boards
      if(display_non_reporters){
        dplyr::filter(all_plot_brds, BoardID %in% non_report) %>%
          dplyr::mutate(brd_bur = paste0(.$BoardID, " (", .$BurrowID, ")")) %>%
          `[[`("brd_bur") %>% # there's gotta be a better way
          paste0(collapse = ", ") %>%
          warn_non_reporting
      }

      # Insert a record into tblNonReport
      cat("\tInserting non-reporting board records...")
      non_report %>%
        purrr::walk(function(brd) {
          strsql <- paste0("INSERT INTO tblNonReport ( [BoardID], [Date_], [ImportID]) SELECT ",
                          brd, " AS Expr1, ",
                          "#", format(date_, format = "%Y-%b-%d"), "# AS Expr2, ",
                          import_ID, " AS Expr3;")
          res <- RODBC::sqlQuery(ch, strsql) %>% ensure_insert_success
        })
      cat(sprintf("%d inserted\n", length(non_report)))
    }
  }
}
