from setuptools import setup
from setuptools import find_packages

required = [
    "numpy",
    "pandas",
    "pytest",
    "pytest-cov",
    "boto3",
    "moto",
    "gitpython",
]

setup(
    name="delphi_utils",
    version="0.0.1",
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
)
