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
    "covidcast"
]

setup(
    name="delphi_cdc_vaccines",
    version="0.0.1",
    description="The number of people who are vaccinated per county.",
    author="Ananya Joshi",
    author_email="aajoshi@andrew.cmu.edu",
    url="https://github.com/cmu-delphi/covidcast-indicators",
    install_requires=required,
    classifiers=[
        "Development Status :: 0 - Attempt",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3.8",
    ],
    packages=find_packages(),
)
