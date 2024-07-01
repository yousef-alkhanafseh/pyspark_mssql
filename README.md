# Overview
This repository accompanies the Medium article titled "How to Read and Write from MSSQL Using PySpark in Python" The article provides a comprehensive tutorial on performing MSSQL read and write operations using the partitioning technique in standalone mode with PySpark and Python. Detailed explanations of essential dependencies and step-by-step instructions are included.
For more details, please refer to: https://medium.com/turknettech/how-to-read-and-write-from-mssql-using-pyspark-in-python-1207c07af864

# Repository Structure
```
.
├── src
│   ├── mssql_with_partitions.py
│   └── mssql_without_partitions.py
└── jars
    └── mssql-jdbc-7.0.0.jre8.jar
    └── mysql-connector-java-5.1.49.jar
    └── spark-mssql-connector_2.12-1.2.0.jar
```
**src Folder**
This folder contains the Python scripts that illustrate the read and write operations using PySpark and MSSQL:

mssql_with_partitions.py: This script demonstrates how to read from and write to an MSSQL database using the partitioning technique in PySpark. Partitioning can significantly enhance the performance of read and write operations by dividing the data into smaller, more manageable chunks.

mssql_without_partitions.py: This script shows the read and write operations without using partitioning. This approach is simpler but might be less efficient for handling large datasets.

**jars Folder**
The jars folder contains the necessary JAR files required to enable PySpark connections to MSSQL databases.

# License
None

# Acknowledgments
We hope this tutorial and repository help you efficiently read from and write to MSSQL databases using PySpark and Python. For any questions or suggestions, please feel free to open an issue or contact me on alkhanafseh15@gmail.com
