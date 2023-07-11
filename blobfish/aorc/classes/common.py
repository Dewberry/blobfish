"""Common classes used in AORC data pipeline"""
from dataclasses import dataclass


@dataclass
class BasicDescriptors:
    title: str
    dataset_id: str
    name: str
    url: str
    notes: str


@dataclass
class ProvenanceMetadata:
    docker_image: str
    remote_compose_file: str
    remote_docker_file: str
    git_repo: str
    commit_hash: str
    docker_repo: str
    digest_hash: str
