from setuptools import setup
from setuptools import find_packages

required = [
    "boto3",
    "covidcast",
    "darker[isort]~=2.1.1",
    "delphi-utils",
    "moto~=4.2.14",
    "numpy",
    "pandas",
    "paramiko",
    "pyarrow",
    "pydocstyle",
    "pylint==2.8.3",
    "pytest-cov",
    "pytest",
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
        "Programming Language :: Python :: 3.8",
    ],
    packages=find_packages(),
)
