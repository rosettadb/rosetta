## Installation Instructions

### ROSETTA_DRIVERS Environment

Set the ENV variable `ROSETTA_DRIVERS` to point to the location of your JDBC drivers.

```
export ROSETTA_DRIVERS=<path_to_jdbc_drivers>
```

example:

```
export ROSETTA_DRIVERS=/Users/adaptivescale/drivers/*
```

### rosetta binary

1. Download the rosetta binary for the supported OS ([releases page](https://github.com/AdaptiveScale/rosetta/releases)).
   ```
    rosetta-<version>-linux_x64.zip
    rosetta-<version>-mac_aarch64.zip
    rosetta-<version>-mac_x64.zip
    rosetta-<version>-win_x64.zip
    ```
2. Unzip the downloaded file
3. Run rosetta commands using `./rosetta` which is located inside `bin` directory.
4. Create new project using `rosetta init` command:

```
   rosetta init database-migration
```

The `rosetta init` command will create a new rosetta project within `database-migration` directory containing the `main.conf` (for configuring the connections to data sources).

5. Configure connections in `main.conf`
   example: connections for postgres and mysql

```
# If your rosetta project is linked to a Git repo, during apply you can automatically commit/push the new version of your model.yaml
# The default value of git_auto_commit is false
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

6. Extract the schema from postgres and translate it to mysql:

```
   rosetta extract -s pg -t mysql
```

The extract command will create two directories `pg` and `mysql`. `pg` directory will have the extracted schema
from Postgres DB. The `mysql` directory will contain the translated YAML which is ready to be used in MySQL DB.

7. Migrate the translated schema to MySQL DB:

```
   rosetta apply -s mysql
```

The apply command will migrate the translated Postgres schema to MySQL.

## RosettaDB CLI JAR and RosettaDB Source

### Setting Up the CLI JAR (Optional)

1. Download the rosetta CLI JAR ([releases page](https://github.com/AdaptiveScale/rosetta/releases))
2. Create an alias command

```bash
alias rosetta='java -cp "<path_to_our_cli_jar>:<path_to_our_drivers>" com.adaptivescale.rosetta.cli.Main'
```

example:

```bash
alias rosetta='java -cp "/Users/adaptivescale/cli-1.0.0.jar:/Users/adaptivescale/drivers/*" com.adaptivescale.rosetta.cli.Main'
```

**Note:** If we are using the **cli** JAR file, we need to specify the location of the JDBC drivers (directory). See the Getting Started section.

### Build from the source (Optional)

```gradle binary:runtimeZip```