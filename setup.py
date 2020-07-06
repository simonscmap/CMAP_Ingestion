from setuptools import setup, find_packages





version = "0.1"
setup(
    name="cmapingest",
    version=version,
    author="Norland Raphael Hagen",
    author_email="norlandrhagen@gmail.com",
    packages=find_packages(),
    include_package_data=True,
    url="https://github.com/simonscmap/CMAP_Ingestion",
    python_requires=">3.6"
)
