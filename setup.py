#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-gorgias",
    version="0.1.7",
    description="Singer.io tap for extracting data from the Gorgias API",
    author="Pathlight",
    url="https://www.pathlight.com/",
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
