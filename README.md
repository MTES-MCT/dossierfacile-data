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
