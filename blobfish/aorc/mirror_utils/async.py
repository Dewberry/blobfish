""" Asynchronous operations for creating AORC data mirror on s3"""
from aiohttp import ClientSession, TCPConnector, ClientTimeout


async def verify_url(url: str) -> str:
    return url


async def download_zip(zip_url: str, dest_fn: str) -> None:
    return
