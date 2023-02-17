import os

import pkg_resources
from setuptools import setup

setup(
    name="redash-pandas",
    version="0.1",
    description="A simple wrapper to query Redash and return a Pandas DataFrame",
    author="Alex Ishida",
    license="Apache License 2.0",
    license_files=["LICENSE"],
    packages=['redash_pandas'],
    install_requires=[
        str(r) for r in pkg_resources.parse_requirements(
            open(os.path.join(os.path.dirname(__file__), "requirements.txt"))
        )
    ],
    include_package_data=True,
    extras_require={},
)
