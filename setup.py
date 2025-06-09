from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="spark-utils",
    version="0.1.0",
    author="Henrique Lamera Carvalho",
    author_email="henrique.lamera@icloud.com",
    description="Biblioteca de utilitários para manipulação de dados no Apache Spark",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/seu-usuario/spark-utils",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires=[
        'pyspark>=3.0.0',
    ],
    package_data={
        'spark_utils': ['*.py'],
    },
)
