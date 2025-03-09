#' @importFrom magrittr %>%
#'@export
#'@title Print a summary of data parsed from a Raspberry Pi report file
#'
#'@description The function summarizes data parsed from a bird report datafile.
#'
#'@param dat List (typically returned from \code{pitdb_parse_bird_report_file})
#'  containing elements \code{tag_reads, statuses, uploads, bad_recs}.
#'@param ch  an open connection to the database. If not null, tag_read records are further categorized
#'  as known_tags, webserver tags, test tags, and unknown tags.
#'@param verbose If \code{TRUE}, print details of each record.
#'
#'@details This function takes the output from \code{pitdb_parse_bird_report_file}
#'  and summarizes the numbers of each type of info contained in the parsed file.
#'
#'@section Author: Dave Fifield
#'
pitdb_summarize_parsed_file <- function(dat, ch = NULL, verbose = FALSE){

  ##### Overall summary ####
  boards <- unique(unlist(dat %>% purrr::discard(is.null) %>% purrr::map("BoardID")))

  if (is.null(boards)) {
    cat("No data to summarize.")
    return()
  }

  cat(paste0(length(boards), " boards with data: ", paste0(boards <- boards[order(boards)], collapse = ", "), "\n"))

  #####  All tag_read records #####
  if (!is.null(dat$tag_reads)) {
    tags <- unique(dat$tag_reads$tagID)
    tags <- tags[order(tags)]
    boards <- unique(dat$tag_reads$BoardID)
    boards <- boards[order(boards)]

    # extract tags with one detection as they passed through the coil. This is
    # unusual and close to non-detection so I'm curious about them.
    one_reads <- dplyr::filter(dat$tag_reads, numread == 1)
    if(nrow(one_reads) > 0){
      one_read_tags <- unique(one_reads$tagID)
      one_read_tags <- one_read_tags[order(one_read_tags)]
    }

    cat(paste0(nrow(dat$tag_reads), " tag reads from ", length(tags), " tags"))
    print(table(dat$tag_reads$tagID))

    cat(paste0("\n\tfrom ", length(boards), " boards (",
        paste0(boards, collapse = ", "),  ")\n\tspanning dates ",
        paste0(range(dat$tag_reads$dateTime), collapse = " to "), "\n",
        "\t", nrow(one_reads), " tag records involved a single read of the tag."))
    print(table(one_reads$tagID))
    if (verbose) {
      cat("Tag read records:")
      dat$tag_reads %>%
        dplyr::as_tibble() %>%
        print(n = nrow(.))
    }

    ######## Categorize tag reads #####
    # Dig deeper and chategorize tag reads according to info in the database.
    if (!is.null(ch)) {
      sp_tags <- RODBC::sqlFetch(ch, "lkpSpecialTags", as.is = T)

      # Split out special tags
      test_tags <- dplyr::filter(sp_tags, Typ == "test_tag")$Val
      known_prefix <- dplyr::filter(sp_tags, Typ == "known_prefix")$Val
      web_prefix <- dplyr::filter(sp_tags, Typ == "web_prefix")$Val

      # get known tags - ones that have been deployed on birds
      known_tags <- RODBC::sqlFetch(ch, "tblTags", as.is = T)

      ################### Known tag reads #######
      known_tag_reads <- dplyr::filter(dat$tag_reads,  tagID %in% known_tags$TagID)

      if (nrow(known_tag_reads) > 0) {
        tags <- unique(known_tag_reads$tagID)
        tags <- tags[order(tags)]
        boards <- unique(known_tag_reads$BoardID)
        boards <- boards[order(boards)]

        # do same as above for all tag reqads
        one_reads <- dplyr::filter(known_tag_reads, numread == 1)
        if(nrow(one_reads) > 0){
          one_read_tags <- unique(one_reads$tagID)
          one_read_tags <- one_read_tags[order(one_read_tags)]
        }

        cat(paste0("\n\t", nrow(known_tag_reads), " of these reads were from known tags, n = ", length(tags), " tags"))
        print(table(known_tag_reads$tagID))

        cat(paste0("\n\tfrom ", length(boards), " boards (",
            paste0(boards, collapse = ", "),  ")\n\tspanning dates ",
            paste0(range(known_tag_reads$dateTime), collapse = " to "), "\n",
            "\t", nrow(one_reads), " known tag records involved a single read of the tag."))
        print(table(one_reads$tagID))
        if (verbose) {
          cat("Known tag read records:")
          known_tag_reads %>% print(n = nrow(.))
        }
      } else {
        cat("0 known-tag reads.")
      }

      ################### Webserver tag reads #######
      web <- dplyr::filter(dat$tag_reads, substr(tagID, 1, 4) == web_prefix)
      if (nrow(web) > 0) {
        cat(paste0("\n", nrow(web), " webserver mode tags\n"))
        print(table(web$tagID))
      } else {
        cat("\n0 webserver tag reads.")
      }


      ################### test tag reads #######
      test <- dplyr::filter(dat$tag_reads, tagID %in% test_tags)
      if (nrow(test) > 0) {
        cat(paste0("\n", nrow(test), " test tags\n"))
        print(table(test$tagID))
      } else {
        cat("\n0 test tag reads.")
      }

      ################### unknown tag reads #######
      ukn <- dat$tag_reads %>%
        dplyr::setdiff(dplyr::bind_rows(known_tag_reads, web, test)) %>%
        dplyr::distinct()
      if (nrow(ukn) > 0) {
        tags <- unique(ukn$tagID)
        tags <- tags[order(tags)]
        boards <- unique(ukn$BoardID)
        boards <- boards[order(boards)]

        cat(paste0("\n", nrow(ukn), " unknown tag reads from ", length(tags)," tags"))
        print(table(ukn$tagID))
        cat(paste0("\n\tfrom ", length(boards), " boards (",
            paste0(boards, collapse = ", "),  ")\n\tspanning dates ",
            paste0(range(ukn$dateTime), collapse = " to "), "\n"))

        if (verbose) {
          ukn %>%
            dplyr::as_tibble() %>%
            print(n = nrow(.))
          cat("Rows where num_read == 1 may be ghost reads.")
        }
      } else {
        cat("\n0 unknown tag reads.")
      }
    } # end tag categorization
  } else {
    cat("No tag_read records.")
  }


  #### status entries #####
  if (!is.null(dat$statuses)) {
    boards <- unique(dat$statuses$BoardID)
    boards <- boards[order(boards)]
    cat(paste0("\n\n", nrow(dat$statuses), " status entries from ", length(boards), " boards (",
        paste0(boards, collapse = ", "), ")\n\tspanning dates ",
        paste0(range(dat$statuses$dateTime), collapse = " to "), "\n"))
    if (verbose) dat$statuses %>% dplyr::as_tibble() %>% print(n = nrow(.))
  } else cat("\nNo status records.")


  ##### uploads #####
  if (!is.null(dat$uploads)) {
    boards <- unique(dat$uploads$BoardID)
    boards <- boards[order(boards)]
    cat(paste0("\n", nrow(dat$uploads), " upload entries from ", length(boards), " boards (",
        paste0(boards, collapse = ", "), ")\n\tspanning dates ",
        paste0(range(dat$uploads$dateTime), collapse = " to "), "\n"))
    if (verbose) dat$uploads %>% dplyr::as_tibble() %>% print(n = nrow(.))
  } else cat("\nNo uploads")

  #### Bad records #####
  if (!is.null(dat$bad_recs)) {
    boards <- unique(dat$bad_recs$BoardID)
    boards <- boards[order(boards)]
    cat(paste0("\n", nrow(dat$bad_recs), " bad records from ", length(boards), " boards (",
        paste0(boards, collapse = ", "), ")\n\tspanning fetch dates ",
        paste0(range(dat$bad_recs$fetchDateTime), collapse = " to "), "\n"))
    if (verbose) dat$bad_recs %>% dplyr::as_tibble() %>% print(n = nrow(.))
  } else cat("\nNo bad records\n")

  invisible(dat)
}
