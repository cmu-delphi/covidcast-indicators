from setuptools import setup
from setuptools import find_packages

with open("README.md", "r") as f:
    long_description = f.read()

required = [
    "boto3",
    "cvxpy",
    "darker[isort]~=2.1.1",
    "delphi-epidata==4.1.20",
    "epiweeks",
    "freezegun",
    "gitpython",
    "importlib_resources>=1.3",
    "mock",
    "moto~=4.2.14",
    "numpy",
    "pandas>=1.1.0",
    "pydocstyle",
    "pylint==2.8.3",
    "pytest-cov",
    "pytest",
    "requests-mock",
    "slackclient",
    "structlog",
    "xlrd"
]

setup(
    name="delphi_utils",
    version="0.3.23",
    description="Shared Utility Functions for Indicators",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="",
    author_email="",
    url="https://github.com/cmu-delphi/",
    install_requires=required,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3.8",
    ],
    packages=find_packages(),
    package_data={'': ['data/20*/*.csv']}
)
