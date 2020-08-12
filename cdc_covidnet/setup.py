from setuptools import setup
from setuptools import find_packages

required = [
    "numpy",
    "pandas",
    "pytest",
    "pytest-cov",
    "pylint",
    "delphi-utils",
    "requests",
    "covidcast"
]

setup(
    name="delphi_cdc_covidnet",
    version="0.1.0",
    description="Parse information for the CDC COVID-NET indicator",
    author="Eu Jing Chua",
    author_email="eujingc@andrew.cmu.edu",
    url="https://github.com/cmu-delphi/covidcast-indicators",
    install_requires=required,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3.8",
    ],
    packages=find_packages(),
)
