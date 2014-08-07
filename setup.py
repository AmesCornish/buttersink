#! /usr/bin/python

""" Package setup for buttersink. """

from setuptools import setup

with open("buttersink/version.txt", "r") as version:
    theVersion = version.readline().strip()

with open("buttersink/README.md", "r") as readme:
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
        'console_scripts': ['buttersink=buttersink.buttersink:main'],
    },

    install_requires=['boto', 'psutil'],

    package_data={
        '': ['version.txt', 'README.md', 'LICENSE.txt'],
    },

    scripts=['scripts/checksumdir'],

    # package_dir={'buttersink': '..'},
    # include_package_data=True,

    # Project uses reStructuredText, so ensure that the docutils get
    # installed or upgraded on the target machine
    # install_requires=['docutils>=0.3'],

)
