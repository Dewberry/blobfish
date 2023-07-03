def create_ckan_resource(
    download_url: str,
    format: str,
    compress_format: str,
    description: str,
    s3: bool,
) -> dict:
    args_dict = {}
    if s3:
        args_dict["access_rights"] = "Access to distribution requires access to parent s3 bucket"
    args_dict.update(
        {
            "url": download_url,
            "format": format,
            "compress_format": compress_format,
            "description": description,
        }
    )
    return args_dict
