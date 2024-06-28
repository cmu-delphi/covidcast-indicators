from setuptools import setup
from setuptools import find_packages

required = [
    "covidcast",
    "darker[isort]~=2.1.1",
    "delphi-utils",
    "pandas",
    "pylint==2.8.3",
    "pytest-cov",
    "pytest",
    "slackclient",
]

setup(
    name="delphi_sir_complainsalot",
    version="0.1.0",
    description="Complains when signals are outdated",
    author="Alex Reinhart",
    author_email="areinhar@stat.cmu.edu",
    url="https://github.com/cmu-delphi/covidcast-indicators",
    install_requires=required,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3.8",
    ],
    packages=find_packages(),
)
