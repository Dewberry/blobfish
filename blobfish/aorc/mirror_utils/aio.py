"""Asynchronous operations for creating AORC data mirror on s3"""
import asyncio
import datetime
import io
import logging
from collections.abc import Iterator

from aiohttp import ClientSession, ClientTimeout
from classes.mirror import AORCDataURL


async def async_verify_url(session: ClientSession, url: str, **kwargs) -> dict | None:
    """Asynchronously verifies that a URL is valid

    Args:
        session (ClientSession): Session to issue request
        url (str): URL to verify

    Returns:
        dict | None: If verified, return dict of url and last modification of resource, else return None
    """
    logging.info(f"Verifying {url}")
    async with session.head(url) as resp:
        if resp.ok:
            last_modification_str = resp.headers.get("Last-Modified")
            last_modification_dt = datetime.datetime.strptime(last_modification_str, "%a, %d %b %Y %H:%M:%S %Z")
            result = {"url": url, "last_modified": last_modification_dt}
            if kwargs:
                result.update(kwargs)
            return result
        return None


async def async_stream_zip_to_s3(
    zip_url: AORCDataURL, client_session: ClientSession, s3_resource, bucket: str
) -> AORCDataURL:
    """Asynchronously streams zipped data from web address to s3 resource

    Args:
        zip_url (AORCDataURL): URL of zip data
        client_session (ClientSession): Client to use in getting zip data
        s3_resource: s3 resource to use in uploading zip data
        bucket (str): Bucket to target when uploading data

    Returns:
        AORCDataURL: Annotated s3 URI
    """
    s3_bucket = s3_resource.Bucket(bucket)
    logging.info(f"Starting download for {zip_url.url}")
    async with client_session.get(zip_url.url) as resp:
        data = io.BytesIO()
        async for chunk in resp.content.iter_chunked(1024):
            data.write(chunk)
        data.seek(0)
        logging.info(f"All data loaded for {zip_url.url}")
        source_url = zip_url.url
        source_last_modified = zip_url.last_modified
        obj = s3_bucket.Object(zip_url.s3_key())
        logging.info(f"Starting upload for {zip_url.s3_key()}")
        obj.upload_fileobj(data)
        s3_url = zip_url
        s3_url.url = f"s3://{bucket}/{zip_url.s3_key()}"
        obj.load()
        s3_url.last_modified = obj.last_modified
        s3_url.additional_args = {"url": source_url, "last_modified": source_last_modified}
    return s3_url


async def async_verify_url_iter(potential_urls: Iterator[AORCDataURL], limit: int, timeout: float) -> list[dict | None]:
    """Verifies a list of URLs

    Args:
        potential_urls (Iterator[AORCDataURL]): Iterator of annotated URLs
        limit (int): Limit of concurrent requests
        timeout (float): Timeout for each request

    Returns:
        list[dict | None]: List of results from async_verify_url function
    """
    timeout = ClientTimeout(timeout)
    semaphore = asyncio.BoundedSemaphore(limit)
    async with ClientSession(timeout=timeout) as session:
        tasks = []
        for aorc_data_url in potential_urls:
            async with semaphore:
                task = async_verify_url(
                    session,
                    aorc_data_url.url,
                    rfc_alias=aorc_data_url.rfc_alias,
                )
                tasks.append(task)
        results = await asyncio.gather(*tasks)
    return results


async def async_stream_zip_to_s3_iter(
    zip_urls: Iterator[AORCDataURL], s3_resource, bucket: str, limit: int, timeout: float
) -> list[AORCDataURL | None]:
    """Iterates through iterable of annotated zipped resource URLs and uploads them to s3

    Args:
        zip_urls (Iterator[AORCDataURL]): Iterator of verified zip urls
        s3_resource: s3 resource to use in uploads
        bucket (str): s3 bucket to target in uploads
        limit (int): Limit of concurrent requests
        timeout (float): Timeout for each request

    Returns:
        _type_: _description_
    """
    timeout = ClientTimeout(timeout)
    semaphore = asyncio.BoundedSemaphore(limit)
    async with ClientSession(timeout=timeout) as session:
        tasks = []
        for aorc_data_url in zip_urls:
            async with semaphore:
                task = async_stream_zip_to_s3(aorc_data_url, session, s3_resource, bucket)
                tasks.append(task)
        results = await asyncio.gather(*tasks)
    return results


def verify_urls(potential_urls: Iterator[AORCDataURL], limit: int = 5, timeout: float = 2000) -> Iterator[AORCDataURL]:
    """Synchronous wrapper for async URL verification

    Args:
        potential_urls (Iterator[AORCDataURL]): URLs to verify
        limit (int): Limit of concurrent requests. Defaults to 5.
        timeout (float): Timeout for each request in seconds. Defaults to 2000.

    Yields:
        Iterator[AORCDataURL]: Yields annotated zipped data web address that has been verified for source datasets
    """
    results_list = asyncio.run(async_verify_url_iter(potential_urls, limit, timeout))
    for result in results_list:
        if result != None:
            yield AORCDataURL(**result)


def stream_zips_to_s3(
    zip_urls: Iterator[AORCDataURL], s3_resource, bucket: str, limit: int = 5, timeout: float = 10000
) -> Iterator[AORCDataURL]:
    """Synchronous wrapper for async zip s3 upload

    Args:
        zip_urls (Iterator[AORCDataURL]): Iterator of annotated zipped data web addresses
        s3_resource: s3 resource to use when uploading data to s3
        bucket (str): s3 target bucket
        limit (int): Limit of concurrent requests. Defaults to 5.
        timeout (float): Timeout for each request in seconds. Defaults to 2000.

    Yields:
        Iterator[AORCDataURL]: Yields annoted s3 URIs for mirrored datasets
    """
    results_list = asyncio.run(async_stream_zip_to_s3_iter(zip_urls, s3_resource, bucket, limit, timeout))
    for result in results_list:
        if result != None:
            yield result
