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
    "us",
    "darker",
]

setup(
    name="delphi_nssp_ed",
    version="0.1.0",
    description="Indicators from the public NSSP Emergency Department respiratory feed",
    author="developer-rpai",
    url="https://github.com/cmu-delphi/covidcast-indicators",
    install_requires=required,
    classifiers=[
        "Development Status :: 1 - Planning",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3.8",
    ],
    packages=find_packages(),
)
