from setuptools import setup
from setuptools import find_packages

required = [
    "numpy",
    "openpyxl",
    "pandas",
    "pydocstyle",
    "pytest",
    "pytest-cov",
    "pylint==2.8.3",
    "delphi-utils==0.3.18",
    "covidcast==0.2.2"
]

setup(
    name="delphi_dsew_community_profile",
    version="0.1.0",
    description="Indicator tracking specimen test results and hospital admissions published in the COVID-19 Community Profile Report by the Data Strategy and Execution Workgroup",
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
