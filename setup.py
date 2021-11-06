#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-gorgias",
    version="0.1.6",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_gorgias"],
    install_requires=[
        # NB: Pin these to a more specific version for tap reliability
        "singer-python",
        "requests",
    ],
    entry_points="""
    [console_scripts]
    tap-gorgias=tap_gorgias:main
    """,
    packages=["tap_gorgias"],
    package_data = {
        "schemas": ["tap_gorgias/schemas/*.json"]
    },
    include_package_data=True,
)
