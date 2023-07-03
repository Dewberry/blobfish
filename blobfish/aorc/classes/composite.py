import datetime
import re
import sqlite3
from dataclasses import dataclass
from typing import Self

from rdflib import Literal, URIRef


@dataclass
class RetrievedMirror:
    mirror_dataset: URIRef
    s3_url: Literal


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
        self.cur.execute("select nc_path where nc_path = ?", timestamp.isoformat())
        nc_files = [f[0] for f in self.cur.fetchall()]
        return nc_files

    def get_mirror_datasets(self, timestamp: datetime.datetime) -> list[URIRef]:
        self.cur.execute("select DISTINCT mirror_dataset where nc_path = ?", timestamp.isoformat())
        mirror_datasets = [URIRef(f[0]) for f in self.cur.fetchall()]
        return mirror_datasets
