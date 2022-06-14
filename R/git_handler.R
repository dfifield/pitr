#'@export
#'@title Pull latest PIT tag data from Git repositories
#'
#'@description This function executes a 'git pull' (using \code{git2r::pull()})
#'  for each repository listed in repos. It uses \code{ssh} key authentication - see
#'  https://docs.github.com/en/authentication/connecting-to-github-with-ssh/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent for info.
#'@param repos A character vector of one or more complete pathnames to a local
#'   repository. Repositories must have a remote defined in order for the pull
#'   to succes
#'@param pub Full pathname to the file containing the public key to use for
#'   authentication.
#'@param priv Full pathname to the file containing the private key to use for
#'   authentication.
#'@param passphrase Character string containing the passphrase for the key.
#'@details Attempts to pull any updates from the remote for each repository given
#'  in \code{repos}. Prints a information message for each repo giving the location of
#'  both the local and remote.
#'
#'@return Nothing.
#'@section Author: Dave Fifield
#'
pitr_pull_from_repo <- function(repos, pub, priv, passphrase) {
  cred = git2r::cred_ssh_key(publickey = pub, privatekey = priv, passphrase = passphrase)

  purrr::walk(repos, function(repo.str, cred){
    repo = git2r::repository(repo.str)
    message(sprintf("Pulling from '%s'\n\tto repo '%s'", git2r::remote_url(repo.str),
                    repo.str))
    print(git2r::pull(repo, credentials = cred))
  }, cred = cred)
}
