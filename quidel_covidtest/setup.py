from setuptools import setup
from setuptools import find_packages

required = [
    "boto3",
    "darker[isort]~=2.1.1",
    "delphi-utils",
    "imap-tools",
    "numpy",
    "openpyxl",
    "pandas",
    "pyarrow",
    "pydocstyle",
    "pylint==2.8.3",
    "pytest-cov",
    "pytest",
    "xlrd==1.2.0", # needed by Pandas to read Excel files
]

setup(
    name="quidel_covidtest",
    version="0.1.0",
    description="Indicators Pulled from datadrop email",
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
