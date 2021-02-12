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
    "paramiko"
]

setup(
    name="delphi_changehc",
    version="0.0.0",
    description="Parse information for the Change Healthcare indicator",
    author="Aaron Rumack",
    author_email="arumack@andrew.cmu.edu",
    url="https://github.com/cmu-delphi/covidcast-indicators",
    install_requires=required,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3.7",
    ],
    packages=find_packages(),
)
