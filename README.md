# dossierfacile-data
data transformation for DossierFacile using DBT framework

## Install DBT

Install DBT in a virtual environment

```bash
python -m venv .venv
source .venv/bin/activate
```

```bash
pip install dbt-core dbt-postgres
```

## Configure DBT

Create a `profiles.yml` file in the root of the project

```yml
dossierfacile:
  target: dev
  outputs:
    dev:
      type: postgres
      host: {{ host }}
      user: {{ user}}
      password: {{ password }}
      port: {{ port }}
      dbname: {{ database }}
      schema: {{ schema }}
      threads: 3
```

Replace the `{{}}` with the values in the vaultwarden secret "user SQL df-data dbt_dev"

To test the connection, run the following command:

```bash
dbt debug
```

## Sqlfluff linter

Install Sqlfluff in a virtual environment:

```bash
source .venv/bin/activate
pip install sqlfluff sqlfluff-templater-dbt
```

Sqlfluff usage:

```bash
sqlfluff lint #to lint all files
sqlfluff fix #to fix the files
```

## Unit tests

To run the unit tests, run the following command:

```bash
dbt test -s test_type:unit
```

## Pre-commit hook

Install pre-commit:

```bash
pip install pre-commit
```

You can add sqlfluff fix to the pre-commit hook by adding the following to the `.pre-commit-config.yaml` file:

```yaml
repos:
  - repo: https://github.com/sqlfluff/sqlfluff
    rev: 3.3.0
    hooks:
      - id: sqlfluff-lint
      - id: sqlfluff-fix
```

To add the pre-commit hook, run the following command:

```bash
pre-commit install
```

## Run DBT

Try running the following commands:

```bash
dbt run
```

Select a model to run:

```bash
dbt run -s model_name
```

## Managing SQL permissions

To grant read access to all users in the group `sql_group` to all future tables created by `sql_user` inside the schema `sql_schema`, you can use this query:
```sql
alter default privileges 
for user sql_user 
in schema sql_schema 
grant select on tables 
to group sql_group;
```

To grant read access to all users in the group `sql_group` to all existing tables inside `sql_schema`:
```sql
alter default privileges in schema sql_schema
grant select on tables to group sql_group;
```

To grant usage on the schema `sql_schema` to the group `sql_group`:
```sql
grant usage on schema sql_schema to group sql_group;
```

To grant select access to all tables in the schema `sql_schema` to the group `sql_group`:
```sql
grant select on all tables in schema sql_schema to group sql_group;
```