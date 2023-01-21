import boto3
from datetime import datetime, timedelta
import json
import logging
import os
from requests.exceptions import Timeout
from rdflib import Literal
from rdflib.namespace import XSD

from ..pyrdf import AORC
from ..utils import gitinfo, logger
from .mirror import AORCMirror
from .source import AORCSource, create_source_data_catalog, source_catalog_to_file
from .const import (
    FIRST_RECORD,
    FTP_ROOT,
    MIRROR_ROOT,
)

from dotenv import load_dotenv, find_dotenv


def create_mirror():
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

    start = dtm = datetime.strptime(FIRST_RECORD, "%Y-%m-%d")
    # end = datetime.now()
    end = datetime(1980, 2, 4)

    daydelta = timedelta(days=1)
    # hourdelta = timedelta(hours=1)

    # _ = day
    for _ in range(start.year, end.year):
        """
        iterate over range of dates required to create the datasets expected on the ftp
        and required to develop the gridtransform datasets
        """
        while dtm <= end:
            year = dtm.year
            # copy source data from the ftp to the mirror for the first day of each month
            if dtm.day == 1:
                xsd_date = Literal(dtm.strftime(format("%Y-%m-%d")), datatype=XSD.date)
                source_datsets = []
                # Search the graph for all RFC data_sources for the given date
                for s, _, _ in mirror.graph.triples((None, AORC.hasRefDate, xsd_date)):
                    r = mirror.data_source_to_mirror(s)
                    if r == Timeout:
                        dbfile = f"mirrors/ttl/AORC-mirror-db-part-{year}.ttl"
                        logging.error(json.dumps({"ftp_error": f"part-{dbfile}"}))
                        with open(dbfile, "w") as f:
                            f.write(mirror.graph.serialize(format="ttl"))
                        raise (r)

                    catalog_graph.value(s, AORC.hasSourceURI)
                    source_datsets.append(s)

                # print(source_datsets)

            # iterate over every hour in the data_source for the given day
            for i in range(24):
                dtmn = dtm + timedelta(hours=i)
                # print(f"\t\tcreating GridTransform for {dtmn} in mirror")

                dst_prefix, file_map = mirror.data_source_map(source_datsets, dtmn)
                composite_grid_uid = mirror.composite_grid_to_graph(dtmn, script_path, git_repo_info)
                mirror.link_composite_grid_sources_in_graph(composite_grid_uid, source_datsets, dtmn)

                # print(dst_prefix, file_map)
                xdata = mirror.create_composite_grid(file_map)
                mirror.composite_grid_to_mirror(xdata, dst_prefix)

            dtm += daydelta
            if dtm.year != year:
                dbfile = f"mirrors/ttl/AORC-mirror-db-{year}.ttl"
                with open(dbfile, "w") as f:
                    f.write(mirror.graph.serialize(format="ttl"))
                logging.info(json.dumps({"writing-ttl": dbfile, "status": "complete"}))

                mirror = AORCMirror(session)
                mirror.graph.bind("s3mirror", MIRROR_ROOT)
                mirror.graph.bind("ftpserver", FTP_ROOT)


if __name__ == "__main__":

    load_dotenv(find_dotenv())

    runtime = datetime.now().strftime("%Y%m%d_%H%M")
    logfile = f"mirrors/logs/aorc-mirror-{runtime}.log"
    logs = logger.set_up_logger(filename=logfile)
    logs.setLevel(logging.INFO)
    create_mirror()
