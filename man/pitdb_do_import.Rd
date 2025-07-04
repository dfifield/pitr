% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/do_import.R
\name{pitdb_do_import}
\alias{pitdb_do_import}
\title{Import data file(s) into database}
\usage{
pitdb_do_import(db = NULL, files = NULL, report_path = NULL)
}
\arguments{
\item{db}{The pathname to the Microsoft Access database to insert downloaded files into.}

\item{files}{files to import (including full pathname)}

\item{report_path}{The pathname to the folder where rendered output should be saved.}
}
\description{
Import one or more files into the database by rendering import_template.RMD included with the package.
}
\details{
This function renders and existing RMD template \code{import_template.RMD} which causes \code{files} to be imported  to \code{db}.
It is called by \code{pitdb_process_data_downloads} (which is used by the \code{import server} to regularly import downloaded data.) It can
also be called directly.
}
\section{Author}{
 Dave Fifield
}

