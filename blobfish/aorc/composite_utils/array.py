"""Functions used in xarray dataset processing for composite dataset creation"""
import xarray as xr
import zarr.storage as storage


def create_composite_dataset(dataset_paths: set[str]) -> xr.Dataset:
    """Merges netCDF files provided into single dataset based on shared time coordinate

    Args:
        dataset_paths (set[str]): List of netCDF file paths

    Returns:
        xr.Dataset: Spatially merged data
    """
    datasets = []
    for dataset_path in dataset_paths:
        ds = xr.open_dataset(dataset_path)
        ds.rio.write_crs(4326, inplace=True)
        datasets.append(ds)
    merged_hourly_data = xr.merge(datasets, compat="no_conflicts", combine_attrs="drop_conflicts")
    return merged_hourly_data


def upload_zarr(zarr_s3_path: str, dataset: xr.Dataset) -> None:
    """Uploads dataset to zarr format

    Args:
        zarr_s3_path (str): s3 target path for zarr dataset
        dataset (xr.Dataset): Dataset to upload
    """
    dataset.to_zarr(store=storage.FSStore(zarr_s3_path))
