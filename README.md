# rosetta 
### Declarative Database Management - DDL Transpiler
![Alt text](./logo.png?raw=true "Rosetta")

## Overview

RosettaDB is an open-source, declarative data modeling and transpilation tool that simplifies database migrations, data quality assurance, and data exploration. With support for schema extraction, AI-driven querying, and automated code generation, RosettaDB equips data engineers and developers to manage complex data workflows across diverse platforms with ease. 

Rosetta utilizes JDBC to extract schema metadata from a database and generates declarative DBML models that can be used for conversion to alternate database targets.

Key Features

- **Declarative Data Modeling**: Define your database schema using DBML (Database Markup Language), and RosettaDB generates target database-specific DDL (Data Definition Language) automatically.
- **Transpilation**: Seamlessly convert database objects from one database platform to another. RosettaDB eliminates the manual effort in migrating between heterogeneous database systems.
- **Data Quality and Validation**: Implement and automate data quality checks using built-in test rules to ensure data accuracy, consistency, and reliability.
- **DBT Model Generation**: Generate dbt models from your database schema effortlessly, empowering robust and scalable analytics workflows.
- **AI-Powered Data Exploration**: Query and explore your data using natural language, leveraging AI to simplify complex SQL tasks and uncover insights.
- **Spark Code Generation**: Automatically generate PySpark or Scala code for transferring data between source and target systems, streamlining data movement in big data pipelines.

Whether you’re modernizing your data architecture, migrating legacy systems, implementing data validation pipelines, or orchestrating data transfer in Spark environments, RosettaDB provides a comprehensive suite of tools tailored to your needs.

**Get Involved**

Join our growing community of developers and data engineers on [RosettaDB Slack](https://join.slack.com/t/rosettadb/shared_invite/zt-1fq6ajsl3-h8FOI7oJX3T4eI1HjcpPbw), and visit our GitHub repository to explore supported databases, translations, and use cases.


## Supported Databases and Translations

The table below lists the currently supported databases and their respective translation capabilities.

|                          |  **BigQuery**  | **Snowflake** |  **MySQL**   |  **Postgres**   | **Kinetica** | **Google Cloud Spanner** | **SQL Server**  |   **DB2**   |   **Oracle**   | **Redshift** |
|--------------------------|:--------------:|:-------------:|:------------:|:---------------:|:------------:|:------------------------:|:---------------:|:-----------:|:--------------:|:------------:|
| **BigQuery**             |       /        |       ✅       |      ✅       |        ✅        |      ✅       |            ✅             |        ✅        |      ✅      |       ✅        |      ✅       |
| **Snowflake**            |       ✅        |       /       |      ✅       |        ✅        |      ✅       |            ✅             |        ✅         |      ✅       |        ✅        |      ✅       |
| **MySQL**                |       ✅        |       ✅       |      /       |        ✅        |      ✅       |            ✅             |        ✅        |      ✅      |       ✅        |      ✅       |
| **Postgres**             |       ✅        |       ✅       |      ✅       |        /        |      ✅       |            ✅             |        ✅        |      ✅      |       ✅        |      ✅       |
| **Kinetica**             |       ✅        |       ✅       |      ✅       |        ✅        |      /       |            ✅             |        ✅        |      ✅      |       ✅        |      ✅       |
| **Google Cloud Spanner** |       ✅        |       ✅       |        ✅       |         ✅         |        ✅       |            /             |        ✅        |      ✅      |       ✅        |      ✅        |
| **SQL Server**           |       ✅        |       ✅       |      ✅       |        ✅        |      ✅       |            ✅             |        /        |      ✅      |       ✅        |      ✅       |
| **DB2**                  |       ✅        |       ✅       |      ✅       |        ✅        |      ✅       |            ✅             |        ✅        |      /      |       ✅        |      ✅       |
| **Oracle**               |       ✅        |       ✅        |      ✅       |        ✅        |      ✅       |            ✅             |        ✅        |      ✅      |       /        |      ✅       |
| **Redshift**             |       ✅        |       ✅        |      ✅       |        ✅        |      ✅       |             ✅              |        ✅        |      ✅      |       ✅       |      /       |


## Getting Started

Follow these steps to get started with RosettaDB:

### 1. Download and initialize RosettaDB

**Linux/MacOS**:

- **Linux (x64)**: Compatible with 64-bit Intel/AMD processors.
- **MacOS (x64)**: For Intel-based Mac systems.
- **MacOS (Arch64)**: For Apple Silicon (M1/M2) Mac systems.

Run the following command to download and set up RosettaDB:
```
curl -L "https://github.com/rosettadb/rosetta/releases/download/v2.8.5/rosetta_setup.sh" -o rosetta_setup && chmod u+x rosetta_setup && ./rosetta_setup
```

**Windows (x64)**

Compatible with 64-bit Intel/AMD processors running Windows

```
curl -L "https://github.com/rosettadb/rosetta/releases/download/v2.8.5/rosetta_setup.bat" -o rosetta_setup.bat && .\rosetta_setup.bat
```

### 2. Initialize a New Project

This step is automatically executed if you completed Step #1.

Create a new RosettaDB project with the following command:

```
   rosetta init database-migration
```
This will create a `database-migration` directory containing the `main.conf` file, which is used to configure connections to data sources.

The `rosetta init` command also prompts you to specify source and target databases and automatically downloads the necessary drivers.

### 3. Configure connections in `main.conf`

An example configuration for connecting to PostgreSQL and MySQL:

```
# Automatically commit and push changes if linked to a Git repository (default: false).
git_auto_commit: false 
connections:
  - name: mysql
    databaseName: sakila
    schemaName:
    dbType: mysql
    url: jdbc:mysql://root:sakila@localhost:3306/sakila
    userName: root
    password: sakila
  - name: pg
    databaseName: postgres
    schemaName: public
    dbType: postgres
    url: jdbc:postgresql://localhost:5432/postgres?user=postgres&password=sakila
    userName: postgres
    password: sakila
```

### 4. Extract and Transpile Your Schemas

Extract the schema from PostgreSQL and transpile it to MySQL:

```
   rosetta extract -s pg -t mysql
```

Migrate the translated schema to the target MySQL database:

```
   rosetta apply -s mysql
```

Need More Help?

For detailed installation instructions and advanced setup, refer to the Installation Guide [here](docs/markdowns/installation.md).


## Rosetta Commands

RosettaDB provides a comprehensive set of commands to cover various aspects of database modeling, validation, and migration. Each command is documented in detail for your convenience.

### Available Commands
- **[config](docs/markdowns/config.md)**: Manage RosettaDB configuration settings.
- **[init](docs/markdowns/init.md)**: Initialize a new RosettaDB project with required configuration files.
- **[validate](docs/markdowns/validate.md)**: Validate database connections.
- **[drivers](docs/markdowns/drivers.md)**: List and manage supported database drivers.
- **[extract](docs/markdowns/extract.md)**: Extract schema metadata from a source database.
- **[compile](docs/markdowns/compile.md)**: Compile DBML models into target DDL statements.
- **[apply](docs/markdowns/apply.md)**: Apply generated DDL to the target database.
- **[diff](docs/markdowns/diff.md)**: Compare and display differences between the DBML model and the database.
- **[test](docs/markdowns/test.md)**: Run data quality and validation tests against your database.
- **[dbt](docs/markdowns/dbt.md)**: Generate dbt models for analytics workflows.
- **[generate](docs/markdowns/generate.md)**: Generate Spark code for data transfers (Python or Scala).
- **[query](docs/markdowns/query.md)**: Explore and query your data using AI-driven capabilities.

## Copyright and License Information
Unless otherwise specified, all content, including all source code files and documentation files in this repository are:

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this tool except in compliance with the License. You may obtain a copy of the License at: [http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
