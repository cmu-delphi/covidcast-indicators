from setuptools import setup
from setuptools import find_packages

required = [
    "covidcast",
    "numpy",
    "pandas",
    "pytest",
    "pytest-cov",
    "pylint",
    "delphi-utils"
]

setup(
    name="delphi_safegraph",
    version="0.0.1",
    description="Mobility Indicators from Safegraph",
    author="",
    author_email="",
    url="https://github.com/cmu-delphi/covidcast-indicators",
    install_requires=required,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3.7",
    ],
    packages=find_packages(),
)
