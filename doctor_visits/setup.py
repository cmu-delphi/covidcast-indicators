from setuptools import setup
from setuptools import find_packages

required = [
    "numpy",
    "pandas",
    "paramiko",
    "scikit-learn",
    "pytest",
    "pytest-cov",
    "pylint==2.8.3",
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
        "Programming Language :: Python :: 3.8",
    ],
    packages=find_packages(),
)
