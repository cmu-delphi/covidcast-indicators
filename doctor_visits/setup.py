from setuptools import setup
from setuptools import find_packages

required = [
    "numpy",
    "pandas",
    "sklearn",
    "cvxpy",
    "pytest",
    "pytest-cov",
    "pylint",
    "delphi-utils"
]

setup(
    name="delphi_doctor_visits",
    version="0.1.0",
    description="Parse information for the doctors visits indicator",
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
