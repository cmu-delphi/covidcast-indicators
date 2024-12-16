from setuptools import setup
from setuptools import find_packages

required = [
    "numpy",
    "pandas",
    "pydocstyle",
    "pytest",
    "pytest-cov",
    "pylint==2.8.3",
    "delphi-utils",
    "sodapy",
    "epiweeks",
    "freezegun",
    "us",
]

setup(
    name="delphi_nhsn",
    version="0.1.0",
    description="Indicators NHSN Hospital Respiratory Data",
    author="",
    author_email="",
    url="https://github.com/cmu-delphi/covidcast-indicators",
    install_requires=required,
    classifiers=[
        "Development Status :: 1 - Planning",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3.8",
    ],
    packages=find_packages(),
)
