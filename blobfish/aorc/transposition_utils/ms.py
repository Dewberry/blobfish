"""Functions interfacing with meilisearch used in transposition dataset creation"""
import datetime
import logging
from typing import Any, Iterator

from classes.transposition import TranspositionMetadata
from meilisearch import Client


def create_meilisearch_client(host: str, api_key: str) -> Client:
    logging.info("Creating meilisearch client")
    ms_client = Client(host, api_key=api_key)
    return ms_client


def paginate_search_result(ms_client: Client, index: str, query: str) -> Iterator[dict]:
    offset = 0
    limit = 10
    while True:
        result = ms_client.index(index).search(query, {"offset": offset, "limit": limit})
        if len(result["hits"]) == 0:
            break
        for hit in result["hits"]:
            yield hit
        offset += limit


def retrieve_ms_data(ms_client: Client, index: str) -> Iterator[TranspositionMetadata]:
    logging.info(f"Retrieving transposition model metadata from meilisearch index {index}")
    for hit in paginate_search_result(ms_client, index, ""):
        start_date_str = hit["start"]["datetime"]
        duration = hit["duration"]
        hit_meta = hit["metadata"]
        hit_stats = hit["stats"]
        hit_geom = hit["geom"]
        start_date_dt = datetime.datetime.fromisoformat(start_date_str)
        end_date_dt = start_date_dt + datetime.timedelta(hours=duration)
        meta = TranspositionMetadata(
            hit["id"],
            duration,
            start_date_dt,
            end_date_dt,
            hit_meta["watershed_name"],
            hit_meta["watershed_source"],
            hit_meta["transposition_domain_name"],
            hit_meta["transposition_domain_source"],
            hit_meta["png"],
            hit_stats["count"],
            hit_stats["mean"],
            hit_stats["max"],
            hit_stats["min"],
            hit_stats["sum"],
            hit_stats.get("norm_mean"),
            hit_geom["center_x"],
            hit_geom["center_y"],
        )
        yield meta
