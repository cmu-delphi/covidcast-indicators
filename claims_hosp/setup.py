from setuptools import setup
from setuptools import find_packages

required = [
    "covidcast",
    "darker[isort]~=2.1.1",
    "delphi-utils",
    "numpy",
    "pydocstyle",
    "pandas",
    "paramiko",
    "pyarrow",
    "pydocstyle",
    "pylint==2.8.3",
    "pytest-cov",
    "pytest",
    "cvxpy<1.6",
    "scs<3.2.6", # TODO: remove this ; it is a cvxpy dependency, and the excluded version appears to break our jenkins build. see: https://github.com/cvxgrp/scs/issues/283
]

setup(
    name="delphi_claims_hosp",
    version="0.1.0",
    description="Create COVID-19 claims-based hospitalization indicator",
    author="Maria Jahja",
    author_email="maria@stat.cmu.edu",
    url="https://github.com/cmu-delphi/covidcast-indicators",
    install_requires=required,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3.8",
    ],
    packages=find_packages(),
)
