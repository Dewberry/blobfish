import sys
import pathlib as pl
import boto3
import os
from rdflib import Literal
import time
from rdflib.namespace import XSD
from datetime import datetime, timedelta

from ..pyrdf import AORC
from .mirror import AORCMirror
from .source import AORCSource, create_source_data_catalog, source_catalog_to_file
from .const import (
    FIRST_RECORD,
    FTP_ROOT,
    MIRROR_ROOT,
)

from dotenv import load_dotenv, find_dotenv

if __name__ == "__main__":
    """
    Step 1: Create database (.ttl) of source datasets
    Step 2: Copy files from ftp (source-mirror)
    Step 3: Transform datasets (composite-mirror)
    """
    load_dotenv(find_dotenv())

    session = boto3.Session(
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )

    # Step 1
    creation_date = datetime.now().strftime(format="%Y-%m-%d")
    print(f"Creating source catalog for AORC on {creation_date}")
    catalog_graph = create_source_data_catalog(creation_date)
    source_catalog_to_file(catalog_graph, filepath="mirrors/AORC-ftp-db.ttl")

    source = AORCSource()
    time.sleep(3)
    mirror = AORCMirror(session)

    # start, end = 1979, 1985
    stride = 1
    for start in range(1979, 1980, stride):
        mirror = AORCMirror(session)
        mirror.graph.bind("s3mirror", MIRROR_ROOT)
        mirror.graph.bind("ftpserver", FTP_ROOT)
        end = start + stride

        dtm = datetime(start, 2, 1)
        end_date = datetime(end, 2, 1)

        daydelta = timedelta(days=1)
        hourdelta = timedelta(hours=1)

        # iterate over range of dates required to
        # create the datasets expected on the ftp and required
        # to develop the gridtransform datasets
        while dtm <= end_date:
            print(f"Processing {dtm}")
            # copy source data from the ftp to the mirror for the first day of each month
            if dtm.day == 1:
                xsd_date = Literal(dtm.strftime(format("%Y-%m-%d")), datatype=XSD.date)
                print(f"searching for sources with hasRefDate={xsd_date}")
                source_datsets = []
                for s, _, _ in source.graph.triples((None, AORC.hasRefDate, xsd_date)):
                    # print(f"\tcopying {s} to mirror")
                    # mirror.copy_to_mirror(source_datsets)
                    source_path = source.graph.value(s, AORC.hasSourceDatasetURI)
                    source_datsets.append(s)

            # copy hourly data directly from the mirror
            for i in range(24):
                dtmn = dtm + timedelta(hours=i)
                # print(f"\t\tcreating GridTransform for {dtmn} in mirror")
                # dst_prefix, file_map = mirror.grid_group_from_mirror(source_datsets, dtmn)
                dst_prefix = mirror.add_grid_group_to_graph(source_datsets, dtmn)

                # print(dst_prefix, file_map)
                # xdata = mirror.create_xrdataset_for_one_hour(file_map)
                # s3zarr = mirror.write_zarrfile_to_mirror(xdata, dst_prefix)
                # print(f"s3zarr: {s3zarr} complete")

            dtm += daydelta

        dbfile = f"mirrors/AORC-mirror-db-{start}-{end}.ttl"
        print(f"writing {dbfile}")
        with open(dbfile, "w") as f:
            f.write(mirror.graph.serialize(format="ttl"))
