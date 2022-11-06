# dbt-snowflake-key-finder
# Install options:

## Add to your packages.yml
- Add the below code to your packages.yml file:
```
  - git: "https://github.com/ciejer/dbt-snowflake-key-finder.git"
    revision: main
```

## As standalone repository
If you are unable to add this macro to an existing repository, use it in the following way:
- Clone repository
- Adjust dbt-project to point at the correct profile for your dbt snowflake connection
- Run `dbt run-operation keyfinder --args '{table_fqn: "db.schema.table", depth: 3, keycount: 1}'

## As part of another repository, manually
- Copy the macros/keyfinder.sql file to your macros folder
- Run `dbt run-operation keyfinder --args '{table_fqn: "db.schema.table", depth: 3, keycount: 1}'
