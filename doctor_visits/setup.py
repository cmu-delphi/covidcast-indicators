from setuptools import setup
from setuptools import find_packages

required = [
    "darker[isort]~=2.1.1",
    "delphi-utils",
    "numpy",
    "pandas",
    "paramiko",
    "pylint==2.8.3",
    "pytest-cov",
    "pytest",
    "scikit-learn",
    "dask==2024.6.*",
    "cvxpy>=1.5",
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
        "Programming Language :: Python :: 3.8",
    ],
    packages=find_packages(),
)
