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
]

setup(
    name="delphi_nwss_wastewater",
    version="0.0.1",
    description="Indicators National Wastewater Surveillance System",
    author="David Weber",
    author_email="davidweb@andrew.cmu.edu",
    url="https://github.com/cmu-delphi/covidcast-indicators",
    install_requires=required,
    classifiers=[
        "Development Status :: 1 - Planning",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3.8",
    ],
    packages=find_packages(),
)
