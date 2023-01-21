import git
from typing import Dict, Tuple


def version() -> Tuple:
    """
    Returns relevant metadata for the git repository
    to comply with FAIR practices.

    """
    repo = git.Repo(search_parent_directories=True)
    # head = repo.branches[-1]
    active_branch = repo.branches[0]

    commit_info = active_branch.object.name_rev.split(" ")
    hash, branch_name = commit_info[0], commit_info[1]
    return repo.remotes.origin.url.replace("git@github.com:", "http://github.com/"), hash, branch_name


def script(fileame: str) -> str:
    str_parser = fileame.index("blobfish")
    return fileame[str_parser:]
