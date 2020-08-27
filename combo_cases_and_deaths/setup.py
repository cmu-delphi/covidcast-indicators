from setuptools import setup
from setuptools import find_packages

required = [
    "pandas",
    "pytest",
    "pytest-cov",
    "pylint",
    "delphi-utils",
    "covidcast"
]

setup(
    name="delphi_combo_cases_and_deaths",
    version="0.1.0",
    description="A combined signal for cases and deaths using JHU for Puerto Rico and USA Facts everywhere else",
    author="Jingjing Tang, Kathryn Mazaitis",
    author_email="krivard@cs.cmu.edu",
    url="https://github.com/cmu-delphi/covidcast-indicators",
    install_requires=required,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3.7",
    ],
    packages=find_packages(),
)
