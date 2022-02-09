# PySpark Playground

A simple Python project to test PySpark algrothms for Data Vault transformations on Spark.

## Development

**System Requirements**

- [Conda](https://docs.conda.io/en/latest/miniconda.html)
- [Poetry](https://python-poetry.org/)

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

## Test Data
**Relational Schema**

![image](https://user-images.githubusercontent.com/62486916/153193425-a8337124-6ac6-441c-be21-f402025bb7af.png)

**Data Vault Schema**

![image](https://user-images.githubusercontent.com/62486916/153193456-639ae7a1-8d07-439e-aceb-679da637ed2f.png)
