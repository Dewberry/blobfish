import boto3
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import partial
import json
import logging
from multiprocessing import Pool
import os
from requests.exceptions import Timeout
from rdflib import Literal
from rdflib.namespace import XSD

from ..pyrdf import AORC
from ..utils import gitinfo, logger, blobstore
from .mirror import AORCMirror
from .source import create_source_data_catalog, source_catalog_to_file
from .const import (
    FIRST_RECORD,
    FTP_ROOT,
    MIRROR_ROOT,
)

from dotenv import load_dotenv, find_dotenv


@dataclass
class ProcessDates:
    start: datetime
    end: datetime


def create_mirror(process_dates: ProcessDates):

    runtime = datetime.now().strftime("%Y%m%d_%H%M")
    logfile = f"mirrors/logs/aorc-mirror-{process_dates.start.year}-{runtime}.log"
    print(logfile)
    logs = logger.set_up_logger(filename=logfile)
    logs.setLevel(logging.INFO)

    """
    Step 1: Create database (.ttl) of source datasets
    Step 2: Copy files from ftp (source-mirror)
    Step 3: Transform datasets (composite-mirror)
    """
    # Copy info for processing script / repository
    script_path = gitinfo.script(__file__)
    git_repo_info = gitinfo.version()

    session = boto3.Session(
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )

    # Step 1
    creation_date = datetime.now().strftime(format="%Y-%m-%d")
    catalog_graph = create_source_data_catalog(creation_date)
    catalog_file = "mirrors/AORC-ftp-db.ttl"
    source_catalog_to_file(catalog_graph, filepath=catalog_file)
    logging.info(json.dumps({source_catalog_to_file.__name__: catalog_file}))

    mirror = AORCMirror(session)
    mirror.graph.bind("s3mirror", MIRROR_ROOT)
    mirror.graph.bind("ftpserver", FTP_ROOT)

    dtm = process_dates.start
    end = process_dates.end
    daydelta = timedelta(days=1)

    year = dtm.year
    dbfile = f"mirrors/ttl/AORC-mirror-db-{year}.ttl"
    dbfile_part = f"mirrors/ttl/AORC-mirror-db-part-{year}.ttl"

    while dtm <= end:
        # copy source data from the ftp to the mirror for the first day of each month
        if dtm.day == 1:
            xsd_date = Literal(dtm.strftime(format("%Y-%m-%d")), datatype=XSD.date)
            source_datsets = []
            # Search the graph for all RFC data_sources for the given date
            for s, _, _ in mirror.graph.triples((None, AORC.hasRefDate, xsd_date)):
                r = mirror.data_source_to_mirror(s)
                if r == Timeout:
                    logging.error(json.dumps({"ftp_error": dbfile_part}))
                    with open(dbfile, "w") as f:
                        f.write(mirror.graph.serialize(format="ttl"))
                    raise (r)

                catalog_graph.value(s, AORC.hasSourceURI)
                source_datsets.append(s)

        # iterate over every hour in the data_source for the given day
        for i in range(24):
            dtmn = dtm + timedelta(hours=i)
            dst_prefix, file_map = mirror.data_source_map(source_datsets, dtmn)
            composite_grid_path = mirror.composite_grid_path(dtmn)
            checkfile = f"{dst_prefix}/.zmetadata"

            # Placeholder for when failure occurs deep in the proces
            # if blobstore.s3_key_exists(checkfile, mirror.s3_client, mirror.bucket_name):
            #     print(f"checking {checkfile}---->gotcha")
            #     logging.info(json.dumps({blobstore.s3_key_exists.__name__: dst_prefix, "status": "skipping"}))
            # else:
            #     print(f"not found... {checkfile}")
            #     logging.info(json.dumps({blobstore.s3_key_exists.__name__: dst_prefix, "status": "writing"}))

            # Derive data and copy to tranform
            xdata = mirror.create_composite_grid(file_map)
            mirror.composite_grid_to_mirror(xdata, composite_grid_path)

            # Add to graph after successful write
            composite_grid_uid = mirror.composite_grid_to_graph(dtmn, script_path, git_repo_info)
            mirror.link_composite_grid_sources_in_graph(composite_grid_uid, source_datsets, dtmn)

        # Advance to next day
        dtm += daydelta

    with open(dbfile, "w") as f:
        f.write(mirror.graph.serialize(format="ttl"))
    logging.info(json.dumps({"writing-ttl": dbfile, "status": "complete"}))


if __name__ == "__main__":

    load_dotenv(find_dotenv())

    first_record = datetime.strptime(FIRST_RECORD, "%Y-%m-%d")
    end = datetime.now()
    # end = datetime(198, 1, 1)

    years = []

    for i, y in enumerate(range(first_record.year, end.year)):
        if i == 0:
            start_date = first_record
        else:
            start_date = datetime(y, 1, 1)

        end_date = datetime(y + 1, 1, 1)

        """
        iterate over range of dates required to create the datasets expected on the ftp
        and required to develop the gridtransform datasets
        """
        years.append(ProcessDates(start_date, end_date))

    with Pool(5) as p:
        p.map(create_mirror, years)
