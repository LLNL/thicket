from setuptools import setup
from setuptools import Extension
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()


setup(
    packages=[
        "thicket",
        "thicket.tests",
        "thicket.notebooks",
    ],
)
