from setuptools import setup
from setuptools import find_packages

required = [
    "requests",
    "pytest",
    "pytest-cov",
    "pylint",
    "delphi-utils"
]

setup(
    name="delphi_facebook",
    version="0.1.0",
    description="Fetches survey data from the qualtrics platform",
    author="Katie Mazaitis",
    author_email="krivard@cs.cmu.edu",
    url="https://github.com/cmu-delphi/covidcast-indicators",
    install_requires=required,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3.8",
    ],
    packages=find_packages(),
)
