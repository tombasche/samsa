from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="samsa",
    version="0.0.1",
    author="Thomas Basche",
    author_email="tcbasche@gmail.com",
    description="Kafka state stores in Python",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/tombasche/samsa",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)