import logging
from git.repo import Repo
from dataclasses import dataclass


@dataclass
class GitInfo:
    origin_url: str
    commit_hash: str
    active_branch: str


class UncommittedChangesError(Exception):
    "There are uncommitted changes in the repo, commit hash pulled will not accurately reflect state of code"


def version() -> GitInfo:
    """
    Returns relevant metadata for the git repository
    to comply with FAIR practices.

    """
    repo = Repo(search_parent_directories=True)
    if repo.is_dirty():
        logging.error(
            f"git info fetched for script would not accurately reflect state of code due to uncommitted changes on branch {repo.active_branch}"
        )
        raise UncommittedChangesError
    info = GitInfo(
        repo.remotes.origin.url.replace("git@github.com:", "https://github.com/"),
        repo.active_branch.commit.hexsha,
        repo.active_branch.name,
    )
    return info


def script(filename: str) -> str:
    script_relative_path = filename
    repo = Repo(search_parent_directories=True)
    if repo.working_tree_dir:
        working_dir_str = str(repo.working_tree_dir)
        git_dir_idx = filename.index(working_dir_str) + len(working_dir_str)
        script_relative_path = filename[git_dir_idx:]
    return script_relative_path
