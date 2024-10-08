from setuptools import setup
from setuptools import find_packages

required = [
    "boto3",
    "darker[isort]~=2.1.1",
    "delphi-utils",
    "epiweeks",
    "freezegun",
    "moto~=4.2.14",
    "numpy",
    "pandas",
    "pydocstyle",
    "pylint==2.8.3",
    "pytest-cov",
    "pytest",
    "sodapy",
]

setup(
    name="delphi_nchs_mortality",
    version="0.0.1",
    description="Indicators from NCHS Mortality Data",
    author="",
    author_email="",
    url="https://github.com/cmu-delphi/covidcast-indicators",
    install_requires=required,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3.8",
    ],
    packages=find_packages(),
)
