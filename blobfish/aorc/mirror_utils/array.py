import datetime
import os
from dataclasses import dataclass
from tempfile import TemporaryDirectory
from zipfile import ZipFile

import xarray as xr
from pandas import to_datetime, to_timedelta


@dataclass
class NCMetadata:
    start_time: datetime.datetime
    end_time: datetime.datetime
    temporal_resolution: datetime.timedelta
    spatial_resolution_meters: float


def check_metadata(s3_resource, bucket: str, zipped_data_key: str) -> NCMetadata:
    print(f"Checking netCDF resources in {zipped_data_key} for metadata")
    with TemporaryDirectory() as tmpdir:
        zipped_data_basename = os.path.basename(zipped_data_key)
        download_fn = os.path.join(tmpdir, zipped_data_basename)
        s3_resource.meta.client.download_file(bucket, zipped_data_key, download_fn)
        with ZipFile(download_fn) as zf:
            # Get first and last netCDF files from zipped dataset
            first = zf.filelist[0]
            second = zf.filelist[1]
            last = zf.filelist[-1]
            fns = [zf.extract(first, tmpdir), zf.extract(second, tmpdir), zf.extract(last, tmpdir)]
        return check_nc4s(fns)


def check_nc4s(nc4_paths: list[str]) -> NCMetadata:
    ds = xr.open_mfdataset(nc4_paths, concat_dim="time", combine="nested")
    # Use time difference between first and second record as temporal resolution
    time_ds = ds.isel(time=slice(0, 2))
    time_diff = time_ds.time.diff(dim="time").item()
    temporal_resolution_as_duration = to_timedelta(time_diff)
    # Get start and end datetimes
    temporal_coverage_start = to_datetime(ds.time[0].values)
    end_record_start_time = to_datetime(ds.time[-1].values)
    temporal_coverage_end = end_record_start_time + temporal_resolution_as_duration
    # Get spatial resolution, convert from degrees to meters using rough conversion factors
    conversion_factor_lat = 111000  # Approximate conversion factor for latitude in meters
    conversion_factor_lon = 111320  # Approximate conversion factor for longitude in meters (for the contiguous US)
    res_x, res_y = ds.rio.resolution()
    res_x_meters = res_x * conversion_factor_lon
    res_y_meters = res_y * conversion_factor_lat
    # Use the maximum of these two as to not overestimate resolution of data
    spatial_resolution_in_meters = max(res_x_meters, res_y_meters)

    return NCMetadata(
        temporal_coverage_start, temporal_coverage_end, temporal_resolution_as_duration, spatial_resolution_in_meters
    )
