#! /usr/bin/python

""" Package setup for buttersink. """

from setuptools import setup

import buttersink.version
theVersion = buttersink.version.version

with open("README.md", "r") as readme:
    theReadMe = readme.read()

setup(
    name="buttersink",
    version=theVersion,
    packages=['buttersink'],

    # metadata for upload to PyPI
    author="Ames Cornish",
    author_email="buttersink@montebellopartners.com",
    description="Buttersink is like rsync for btrfs snapshots",
    long_description=theReadMe,
    license="GPLv3",
    keywords="btrfs sync synchronize rsync snapshot subvolume buttersink backup",
    url="https://github.com/AmesCornish/buttersink/wiki",
    # could also include long_description, download_url, classifiers, etc.

    entry_points={
        'console_scripts': [
            'buttersink=buttersink.buttersink:main',
            'btrfslist=buttersink.btrfslist:main',
            ],
    },

    install_requires=['boto', 'crcmod', 'psutil'],

    # These will be in the package subdirectory, accessible by package code
    # package_data={
    #     '': ['version.txt'],
    # },

    # Top-level files, for access by setup, must be listed in MANIFEST.in

    scripts=['scripts/checksumdir'],

    # Problematic.  Avoid this.
    # data_files=[
    #     ('data', ['README.md', 'LICENSE.txt'])
    # ],

    # Problematic.  Avoid this.
    # include_package_data=True,

    # package_dir={'buttersink': '..'},
)
