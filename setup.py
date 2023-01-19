from platform import python_version_tuple
import setuptools
import os

with open("requirements.txt") as f:
    requires = f.read().splitlines()

system_spec_requires = {
    "posix": ["gdal"],
}

requires.extend(system_spec_requires.get(os.name, []))

setuptools.setup(
    name="blobfish",
    version="2023.1.19",
    author="Seth Lawler",
    author_email="slawler@dewberry.com",
    description="Package to create FAIR mirrors",
    long_description="Package to create FAIR mirrors",
    long_description_content_type="text",
    packages=setuptools.find_packages(),
    install_requires=requires,
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: linux_x86_64",
    ],
    python_requires=">=3.9",
)
