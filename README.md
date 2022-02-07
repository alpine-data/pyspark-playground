# PySparkDataVault

A PySpark library to support development of a DataVault on basis of Spark and Delta Lake.

## Development

**System Requirements**
* [Conda](https://docs.conda.io/en/latest/miniconda.html)
* [Poetry](https://python-poetry.org/)

To start development of the project. Create a local development environment.

```bash
$ cd pyspark-datavault
$ conda create -p ./env python=3.8
$ conda activate ./env
$ poetry install
```

To run the tests of the project, execute the following command from project root.

```bash
$ python -m pytest tests
```