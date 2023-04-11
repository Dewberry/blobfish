import pathlib
import subprocess
import os


def script(filename: str, dockerfile: str = "Dockerfile", workdir: str | None = None) -> str | None:
    """Function that gets the path of the input file relative to the home directory of the dockerfile used to generate the docker image

    Args:
        filename (str): File for which relative path is found
        dockerfile (str, optional): Name of dockerfile to search for. Defaults to "Dockerfile".
        workdir(str | None, optional): workdir to prepend to the relative path found

    Returns:
        str | None: _description_
    """
    # Find docker file from script path
    file_path = pathlib.Path(filename)
    file_root = pathlib.Path(file_path.root)
    while file_path != file_root:
        attempt = file_path / dockerfile
        if attempt.exists():
            relative_path = pathlib.Path(filename).relative_to(file_path)
            if workdir:
                relative_path = pathlib.Path(workdir).joinpath(relative_path)
            return str(relative_path)
        file_path = file_path.parent
    return None


def pull_hash(image: str, tag: str) -> None:
    """Function to get hash associated with latest version of a specific tag of image on docker hub

    Args:
        image (str): Image name
        tag (str): Tag name of interest
    """
    # Use command line to get the current hash associated with the docker hub image being used
    process = subprocess.Popen(["docker", "pull", f"{image}:{tag}"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, _ = process.communicate()
    digest_line = stdout.splitlines()[1].decode()
    hash = digest_line.replace("Digest: ", "", 1)
    os.environ["HASH"] = hash
