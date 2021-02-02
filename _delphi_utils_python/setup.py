from setuptools import setup
from setuptools import find_packages

required = [
    "boto3",
    "covidcast",
    "gitpython",
    "moto",
    "numpy",
    "pandas>=1.1.0",
    "pydocstyle",
    "pylint",
    "pytest",
    "pytest-cov",
    "slackclient",
    "structlog",
    "xlrd"
]

setup(
    name="delphi_utils",
    version="0.1.0",
    description="Shared Utility Functions for Indicators",
    author="",
    author_email="",
    url="https://github.com/cmu-delphi/",
    install_requires=required,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3.7",
    ],
    packages=find_packages(),
    package_data={'': ['data/*.csv']}
)
