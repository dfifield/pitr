
# Try to insert a single record. Returns T if success, F otherwise.
# If verbose == TRUE and ignore_errors == TRUE, print a warning message when insertion fails.
do_insert <- function(dt, ch, strsql, ignore_errors, whch_table, import_ID, read_type, verbose) {


  if (whch_table == "tblOtherTagRead") {
    ensurer::ensure_that(read_type, is.character(.))

    read_types <- RODBC::sqlFetch(ch, "lkpTagReadType", as.is = TRUE) %>% ensure_data_is_returned
    read_type_ID <- dplyr::filter(read_types, ReadType == read_type)$ReadID %>% ensurer::ensure_that(length(.) > 0)

    strsql <- paste0("INSERT INTO ", whch_table," ( [BoardID], [DateTime], [FetchDateTime], [WIFIID], [TagID], ",
                    "[NumReads], [ImportID], [ReadType] ) SELECT ",
                    dt$BoardID, " AS Expr1, ",
                    "#", format(dt$dateTime, format = "%Y-%b-%d %H:%M:%S"), "# AS Expr2, ",
                    "#", format(dt$fetchDateTime, format = "%Y-%b-%d %H:%M:%S"), "# AS Expr3, ",
                    "'", ifelse("WiFiID" %in% colnames(dt), dt$WiFiID, "CableConnect"), "' AS Expr4, ",
                    "'", dt$tagID, "' AS Expr5, ",
                    dt$numread, " AS Expr6, ",
                    import_ID, " AS Expr7, ",
                    read_type_ID, " AS Expr8;")
  } else if (whch_table == "tblBirdTagRead") {
    strsql <- paste0("INSERT INTO ", whch_table," ( [BoardID], [DateTime], [FetchDateTime], [WIFIID], [TagID], ",
                    "[NumReads], [ImportID] ) SELECT ",
                    dt$BoardID, " AS Expr1, ",
                    "#", format(dt$dateTime, format = "%Y-%b-%d %H:%M:%S"), "# AS Expr2, ",
                    "#", format(dt$fetchDateTime, format = "%Y-%b-%d %H:%M:%S"), "# AS Expr3, ",
                    "'", ifelse("WiFiID" %in% colnames(dt), dt$WiFiID, "CableConnect"), "' AS Expr4, ",
                    "'", dt$tagID, "' AS Expr5, ",
                    dt$numread, " AS Expr6, ",
                    import_ID, " AS Expr7;")
  } else if (whch_table == "tblStatus") {
    # Empirical value derived from measuring voltage and observing value of vin at same time
    VIn_to_volts <- 0.013342
    VCoin_to_volts <- 0.001881
    MCUTemp_to_temp <- 1 # XXX havent figure out how to convert yet

    strsql <- paste0("INSERT INTO tblStatus ( [BoardID], [DateTime], [FetchDateTime], [WIFIID], [Freq], ",
                        "[Vin], [VCoin], [MCUTemp], [ImportID] ) SELECT ",
                        dt$BoardID, " AS Expr1, ",
                        "#", format(dt$dateTime, format = "%Y-%b-%d %H:%M:%S"), "# AS Expr2, ",
                        "#", format(dt$fetchDateTime, format = "%Y-%b-%d %H:%M:%S"), "# AS Expr3, ",
                        "'", ifelse("WiFiID" %in% colnames(dt), dt$WiFiID, "CableConnect"), "' AS Expr4, ",
                        dt$Freq, " AS Expr5, ",
                        round(dt$VIn * VIn_to_volts, 2), " AS Expr6, ",
                        round(dt$VCoin * VCoin_to_volts, 2), " AS Expr7, ",
                        round(dt$MCUTemp * MCUTemp_to_temp, 1), " AS Expr8, ",
                        import_ID, " AS Expr9;")
  } else if (whch_table == "tblUpload") {
      strsql <- paste0("INSERT INTO tblUpload ( [BoardID], [DateTime], [FetchDateTime], [WIFIID], [PrevIndex], [ImportID] ) SELECT ",
                        dt$BoardID, " AS Expr1, ",
                        "#", format(dt$dateTime, format = "%Y-%b-%d %H:%M:%S"), "# AS Expr2, ",
                        "#", format(dt$fetchDateTime, format = "%Y-%b-%d %H:%M:%S"), "# AS Expr3, ",
                        "'", ifelse("WiFiID" %in% colnames(dt), dt$WiFiID, "CableConnect"), "' AS Expr4, ",
                        dt$prev_index, " AS Expr5, ",
                        import_ID, " AS Expr6;")

  } else if (whch_table == "tblBadRecord") {
      strsql <- paste0("INSERT INTO tblBadRecord ( [BoardID], [FetchDateTime], [WIFIID], [ImportID] ) SELECT ",
                        dt$BoardID, " AS Expr1, ",
                        "#", format(dt$fetchDateTime, format = "%Y-%b-%d %H:%M:%S"), "# AS Expr3, ",
                        "'", ifelse("WiFiID" %in% colnames(dt), dt$WiFiID, "CableConnect"), "' AS Expr4, ",
                        import_ID, " AS Expr5;")
  } else {
    stop(sprintf("do_insert: unknown table (%s)", whch_table))
  }


    res <- RODBC::sqlQuery(ch, strsql)

    if (!ignore_errors)
        res %>% ensure_insert_success
    else if (verbose)
        res %>% warn_insert_success

    # this should really be integrated into previous statement...
    if (length(res) == 0)
      TRUE
    else
      FALSE
}
