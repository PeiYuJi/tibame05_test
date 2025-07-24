import os
from setuptools import setup

here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="etf-lab",
    version="0.0.1",
    description="ETF lab crawler and database",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="peiyuji",
    author_email="abc0970870@gmail.com",
    classifiers=[
        "Development Status :: 3 - Alpha",
    ],
    packages=["crawler"],
)
