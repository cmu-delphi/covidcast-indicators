from setuptools import setup
from setuptools import find_packages

required = [
    "numpy",
    "pandas",
    "cvxpy",
    "pytest",
    "pytest-cov",
    "pylint",
    "delphi-utils",
    "covidcast"
]

setup(
    name="delphi_emr_hosp",
    version="0.1.0",
    description="Parse information for the EMR hospitalizations indicator",
    author="Maria Jahja",
    author_email="maria@stat.cmu.edu",
    url="https://github.com/cmu-delphi/covidcast-indicators",
    install_requires=required,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3.7",
    ],
    packages=find_packages(),
)
