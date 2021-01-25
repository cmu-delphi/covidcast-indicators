from setuptools import setup
from setuptools import find_packages

required = [
    "numpy",
    "pandas",
    "pydocstyle",
    "pytest",
    "pytest-cov",
    "pylint",
    "delphi-utils",
    "covidcast",
    "pyarrow",
]

setup(
    name="delphi_covid_act_now",
    version="0.1.0",
    description="Indicators from COVID Act Now",
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
