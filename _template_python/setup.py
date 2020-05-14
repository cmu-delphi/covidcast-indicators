from setuptools import setup
from setuptools import find_packages

required = [
    "numpy",
    "pandas",
    "pytest",
    "pytest-cov",
    "pylint",
    "delphi-utils"
]

setup(
    name="delphi_google_health",
    version="0.1.0",
    description="Indicators from Google Health Trends",
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
