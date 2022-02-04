from setuptools import setup
from setuptools import find_packages

required = [
    "numpy",
    "pandas",
    "pydocstyle",
    "pytest",
    "pytest-cov",
    "coverage==6.2.0",
    "pylint==2.8.3",
    "delphi-utils"
]

setup(
    name="delphi_safegraph_patterns",
    version="0.0.1",
    description="Mobility Indicators from Safegraph",
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
