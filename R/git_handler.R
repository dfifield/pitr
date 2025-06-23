#'@export
#'@title Pull latest PIT tag data from Git repositories
#'
#'@description This function executes a 'git pull' (using \code{git2r::pull()})
#'  for each repository listed in repos.
#'@param repos A character vector of one or more complete pathnames to a local
#'   repository. Repositories must have a remote defined in order for the pull
#'   to succes
#'@param cred Credentials as returned by pitr_setup_git_crds() (default: \code{NULL}).
#'@details Attempts to pull any updates from the remote for each repository given
#'  in \code{repos}. Prints a information message for each repo giving the location of
#'  both the local and remote.
#'
#'@return Nothing.
#'@section Author: Dave Fifield
#'
pitr_pull_from_repo <- function(repos, cred = NULL) {
  if (is.null(cred)) pitr_setup_git_creds()
  # cred = git2r::cred_ssh_key(publickey = pub, privatekey = priv, passphrase = passphrase)

  purrr::walk(repos, function(repo.str, cred){
    repo = git2r::repository(repo.str)
    message(sprintf("Pulling from '%s'\n\tto repo '%s'", git2r::remote_url(repo.str),
                    repo.str))
    # config(repo, user.name = "gull-island", user.email = "gull-island@gmail.com")
    print(git2r::pull(repo, credentials = cred))
  }, cred = cred)
}

#'@export
#'@title Setup credentials for connecting with GitHub PIT tag data repos
#'
#'@description This function looks up the GITHUB_PAT environment variable and
#'  combines it with \code{username} to create and return credentials suitable
#'  for passing to \code{git2r::pull()}, etc.
#'@param username Character string containing the github username. (Default:
#'  "gull-island").
#'
#'@details Since this function uses GitHub Personal Access Tokens, the returned
#'  credentials will only work with repos cloned with HTTPS.
#'@return credentials created by \code{git2r::cred_user_pass}
#'@section Author: Dave Fifield
#'
pitr_setup_git_creds <- function(username = "gull-island") {
  pat <- Sys.getenv("GITHUB_PAT")
  git2r::cred_user_pass(username = username, password = pat)
}


#' @importFrom magrittr %>%
#'@export
#'@title Clone data repos for a given year to the local machine from GitHub
#'
#'@description Sets up local data repos for all plots in a given year and
#'    clones them from GitHub
#'@param year The year to setup repos for.
#'@param base_folder The folder under which individual plot data repos are cloned.
#'@param plot_nos A vector of plot numbers indicating which plot repos to set up.
#'@param force_delete Should an existing local folder for this repo be deleted
#'    if found? Defalut:\code{FALSE}
#'
#'@details If an existing local folder is found for any repo and force_delete is
#'    \code{FALSE}, then a warning is generated for that repo and now further action
#'    will be taken for it.
#'@return xxx
#'@section Author: Dave Fifield
#'
pitr_setup_plot_repos <- function(base_folder,
                             year,
                             plot_nos = 1:6,
                             force_delete = FALSE) {

  # Setup credentials
  cred <- pitr_setup_git_creds()

  # Create year folder if needed
  if (!dir.exists(file.path(base_folder, year))) {
    message("Creating data folder for ", year)
    dir.create(file.path(base_folder, year))
  }

  # Make sure (or force) plot repo to be empty.
  purrr::walk(plot_nos, \(plot) {
    initialize_repo(plot = plot,
                    year = year,
                    base_folder = base_folder,
                    force_delete = force_delete,
                    cred = cred)
  })
}

#
initialize_repo <- function(plot, year, base_folder, force_delete, cred) {
  repo_folder <- here::here(base_folder, year, paste0("Plot", plot))

  if (length(repo_folder) > 1)
    stop(
      "initialize_repo: More than one repo folder supplied: ",
      paste(repo_folder, collapse = ", ")
    )

  # Check if folder already exists
  exist.dir <- dir.exists(repo_folder)

  # Optionally remove old repo first
  if (isTRUE(exist.dir) && isTRUE(force_delete)) {
    if(unlink(repo_folder, recursive = TRUE, force = TRUE) == 1)
      warning("inialize_repo: failed to remove old repo folder: ", repo_folder,
              immediate. = TRUE)
  }

  # Clone repo if folder doesn't exist
  if(isFALSE(dir.exists(repo_folder))) {
    message("Initializing repo: ", repo_folder)

    # The repo URL (use HTTPS, not SSH)
    # XXX Need to make this generic
    repo_url <- sprintf("https://gull-island@github.com/gull-island/plot%d_%d.git",
                        plot, year)
    repo <- git2r::clone(url = repo_url,
                  local_path = repo_folder,
                  credentials = cred)
  } else
    warning("Repository folder '", repo_folder, "' already",
            " exists and force_delete is FALSE. Repo will not be cloned!",
            immediate. = TRUE)
}
#
# base_folder <- "C:/Users/fifieldd/OneDrive - EC-EC/Projects/Burrow_Logger/Data"
# year <- 2025
# plot_nos <- 1:6
# setup_plot_repos(base_folder = base_folder, year = year, plot_nos = plot_nos,
#                  force_delete = TRUE)
