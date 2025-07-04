% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/pitdb.R
\name{pitdb_open}
\alias{pitdb_open}
\title{Open a pit tag database}
\usage{
pitdb_open(pathname)
}
\arguments{
\item{pathname}{Full pathname of the Access PIT tag database to be opened.}
}
\value{
A channel to the open database object or -1 on error.
}
\description{
The function will connect to the Access PIT tag database via ODBC
 and return a handle to the open database object.
}
\details{
The function will open the Access database specified by
 \code{pathname}.
}
\section{Author}{
 Dave Fifield
}

