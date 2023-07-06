import datetime
import re
import sqlite3
from dataclasses import dataclass
from typing import Self

from rdflib import Literal, URIRef


@dataclass
class RetrievedMirror:
    uri: URIRef
    url: Literal
    start_date: Literal
    end_date: Literal
    resolution: Literal
    wkt: Literal


class DatasetTracker:
    def __init__(self) -> None:
        self.datetime_pattern = re.compile(r"(\d{4}\d{2}\d{2}\d{2})\.nc4$")

    def __enter__(self) -> Self:
        self.con = sqlite3.connect(":memory:", detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        self.cur = self.con.cursor()
        self.cur.execute("create table mirror_datasets(t timestamp, uri TEXT, nc_path TEXT)")
        return self

    def __exit__(self) -> Self:
        self.cur.close()
        self.con.commit()
        self.con.close()

    def register_netcdfs(self, mirror_dataset: URIRef, nc_paths: list[str]) -> None:
        insert_rows = []
        for nc_path in nc_paths:
            match = re.search(self.datetime_pattern, nc_path)
            ts = datetime.datetime.strptime(match.group(1), "%Y%m%d%H")
            insert_rows.append(
                (
                    ts,
                    str(mirror_dataset),
                    nc_path,
                )
            )
        self.cur.executemany("insert into mirror_datasets VALUES(?)", insert_rows)

    def get_nc_files(self, timestamp: datetime.datetime) -> list[str]:
        self.cur.execute("select nc_path from mirror_datasets where t = ?", timestamp.isoformat())
        nc_files = [f[0] for f in self.cur.fetchall()]
        return nc_files

    def get_mirror_datasets(self, timestamp: datetime.datetime) -> list[URIRef]:
        self.cur.execute("select DISTINCT uri from mirror_datasets where t = ?", timestamp.isoformat())
        mirror_datasets = [URIRef(f[0]) for f in self.cur.fetchall()]
        return mirror_datasets

    def group_data_by_time(self) -> list[tuple[list[str], list[URIRef], datetime.datetime]]:
        results = []
        for nc_path_concat, uri_concat, t in self.cur.execute(
            "select GROUP_CONCAT(nc_path) AS nc_path_concat, GROUP_CONCAT(uri) AS uri_concat, t from mirror_datasets group by t"
        ):
            nc_paths = nc_path_concat.split(",")
            mirror_dataset_uris = [URIRef(uri) for uri in uri_concat.split(",")]
            results.append((nc_paths, mirror_dataset_uris, t))
        return results
