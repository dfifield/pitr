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
pull_from_repo <- function(repos, cred = NULL) {
  # Get credentials if needed
  if (is.null(cred))
    pitr_setup_git_creds()

  # do the pull
  purrr::walk(repos, function(repo.str, cred) {
    repo = git2r::repository(repo.str)
    message(sprintf(
      "Pulling from '%s'\n\tto repo '%s'",
      git2r::remote_url(repo.str),
      repo.str
    ))
    print(git2r::pull(repo, credentials = cred))
  }, cred = cred)
}

#'@title Setup credentials for connecting with GitHub PIT tag data repos
#'
#'@description This function creates and returns credentials suitable
#'  for passing to \code{git2r::pull()}, etc.
#'
#'@param username Character string containing the github username. (Default:
#'  "gull-island").
#'@param pat Character string containing the Personal Access Token (PAT). If
#'  \code{NULL} (default) it is looked up in the \code{GITHUB_PAT} environment
#'  variable.
#'
#'@details Since this function uses GitHub Personal Access Tokens, the returned
#'  credentials will only work with repos cloned with HTTPS. Note that if this
#'  function is called from a process that is started via Windows Task Scheduler
#'  under the "SYSTEM" pseudo-user, then the \code{GITHUB_PAT} environment
#'  variable will not be set. In such a case, you should pass the PAT via the
#'  \code{pat} argument.
#'
#'@return Credentials created by \code{git2r::cred_user_pass}
#'@section Author: Dave Fifield
#'
setup_git_creds <- function(username = "gull-island", pat = NULL) {
  if (is.null(pat))
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
                             force_delete = FALSE,
                             user.name = "gull-island",
                             user.email = "gull-island@gmail.com") {

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
                    cred = cred,
                    user.name = user.name,
                    user.email = user.email)
  })
}

#
initialize_repo <- function(plot,
                            year,
                            base_folder,
                            force_delete,
                            cred,
                            user.name = user.name,
                            user.email = user.email) {

  repo_folder <- here::here(base_folder, year, paste0("Plot", plot))
  message("Initializing repo: ", repo_folder)

  if (length(repo_folder) > 1)
    stop(
      "initialize_repo: More than one repo folder: ",
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
    # The repo URL (use HTTPS, not SSH)
    # XXX Need to make this generic
    repo_url <- sprintf("https://gull-island@github.com/gull-island/plot%d_%d.git",
                        plot, year)
    repo <- git2r::clone(url = repo_url,
                 local_path = repo_folder,
                 credentials = cred)

    # Configure the username and email since it isn't always there and will
    # cause trouble when pulling, etc.
    git2r::config(repo, user.name = user.name, user.email = user.email)

  } else
    warning("Repository folder '", repo_folder, "' already",
            " exists and force_delete is FALSE. Repo will not be cloned!",
            immediate. = TRUE)
}
