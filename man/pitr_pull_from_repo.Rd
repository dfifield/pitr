% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/git_handler.R
\name{pitr_pull_from_repo}
\alias{pitr_pull_from_repo}
\title{Pull latest PIT tag data from Git repositories}
\usage{
pitr_pull_from_repo(repos, pub, priv, passphrase)
}
\arguments{
\item{repos}{A character vector of one or more complete pathnames to a local
repository. Repositories must have a remote defined in order for the pull
to succes}

\item{pub}{Full pathname to the file containing the public key to use for
authentication.}

\item{priv}{Full pathname to the file containing the private key to use for
authentication.}

\item{passphrase}{Character string containing the passphrase for the key.}
}
\value{
Nothing.
}
\description{
This function executes a 'git pull' (using \code{git2r::pull()})
 for each repository listed in repos. It uses \code{ssh} key authentication - see
 https://docs.github.com/en/authentication/connecting-to-github-with-ssh/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent for info.
}
\details{
Attempts to pull any updates from the remote for each repository given
 in \code{repos}. Prints a information message for each repo giving the location of
 both the local and remote.
}
\section{Author}{
 Dave Fifield
}

