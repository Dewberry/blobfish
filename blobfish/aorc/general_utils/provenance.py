"""Functions used to retrieve information used in determining data provenance, like git and docker info"""
import logging
import os
import sys

from classes.common import ProvenanceMetadata


def retrieve_meta() -> ProvenanceMetadata:
    """Retrieves provenance metadata from environment variables

    Returns:
        ProvenanceMetadata: Provenance metadata
    """
    relative_composite_path = os.environ["COMPOSE_FILE_PATH"]
    relative_docker_file_path = os.environ["DOCKER_FILE_PATH"]
    git_url = os.environ["GIT_REPO"]
    remote_composite = create_raw_content_url(relative_composite_path, git_url)
    remote_docker_file = create_raw_content_url(relative_docker_file_path, git_url)
    prov_meta = ProvenanceMetadata(
        os.environ["DOCKER_IMAGE"],
        remote_composite,
        remote_docker_file,
        os.environ["GIT_REPO"],
        os.environ["GIT_HASH"],
        os.environ["DOCKER_URL"],
        os.environ["DOCKER_HASH"],
    )
    logging.info(f"Retrieved provenance metadata from environment variables: {prov_meta}")
    return prov_meta


def create_raw_content_url(relative_path: str, git_url: str) -> str:
    """Creates the publicly accessible format of raw content at github address

    Args:
        relative_path (str): relative path of resource in git repo
        git_url (str): github repo url

    Raises:
        ValueError: GitHub url is not in correct format

    Returns:
        str: Raw content URL
    """
    if not git_url.startswith("raw."):
        git_url = git_url.replace("https://github.com", "https://raw.githubusercontent.com/", 1)
        git_url = git_url.replace("/commit", "", 1)
        git_url += f"/{relative_path}"
        return git_url
    raise ValueError("Git URL supplied is already raw content, expecting public URL")


def get_command_list() -> list[str]:
    """Retrieves command list supplied to python

    Returns:
        list[str]: Commands, split by space delimiter
    """
    command_list = [sys.executable, *sys.argv]
    logging.info(f"Command list: {command_list}")
    return command_list
