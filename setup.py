from setuptools import setup, find_packages


with open("requirements.txt") as f:
    requirements = f.read().splitlines()

version = "0.1"
setup(
    name="cmapingest",
    version=version,
    install_requires=requirements,
    author="Norland Raphael Hagen",
    author_email="norlandrhagen@gmail.com",
    packages=find_packages(),
    include_package_data=True,
    url="https://github.com/simonscmap/CMAP_Ingestion",
)
