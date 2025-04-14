from setuptools import setup
from setuptools import find_packages

required = [
    "numpy",
    "pandas",
    "cvxpy",
    "pydocstyle",
    "pytest",
    "pytest-cov",
    "pylint",
    "delphi-utils",
    "covidcast",
    "boto3",
    "moto",
    "paramiko",
    "statsmodels"
]

setup(
    name="delphi_backfill_alerting",
    version="0.0.0",
    description="Backfill Alerting for the Change Healthcare indicator",
    author="Jingjing Tang",
    author_email="jtang2@andrew.cmu.edu",
    url="https://github.com/cmu-delphi/covidcast-indicators",
    install_requires=required,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3.7",
    ],
    packages=find_packages(),
)
