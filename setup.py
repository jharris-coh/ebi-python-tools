from setuptools import setup, find_packages

setup(
    name="ebi_python_tools",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "pyarrow",
        "polars",
        "pandas",
        "arrow_odbc",
        "adbc_driver_manager",
        "adbc_driver_postgresql",
        "adbc_driver_snowflake",
        "duckdb",
    ],
    author="Jeremy Harris",
    author_email="jharris@coh.org",
    description="A suite of python tools aimed to simplify common tasks performed by EBI python users.",
    url="https://github.com/jharris-coh/ebi-python-tools",
)
