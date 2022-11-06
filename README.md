# dbt-snowflake-key-finder
## Usage:
Once installed, run the below command to identify unique keys within a table in your Snowflake environment:

```
dbt run-operation keyfinder --args '{table_fqn: "db.schema.table", depth: 3, keycount: 1}'
```
### Parameters:
- `table_fqn`: fully qualified name, as in db.schema.table. Table name is case sensitive; db/schema are not.
- `depth`: number of columns to join together in the search for uniqueness - this is recursive, so higher numbers may be expensive. Maximum 3.
- `keycount`: minimum number of keys to return. Suggest starting with 1-2 for any given table.

## Install options:

### Add to your packages.yml
- Add the below code to your packages.yml file:
```
  - git: "https://github.com/ciejer/dbt-snowflake-key-finder.git"
    revision: main
```

### As standalone repository
If you are unable to add this macro to an existing repository, use it in the following way:
- Clone repository
- Adjust dbt-project to point at the correct profile for your dbt snowflake connection

### As part of another repository, manually
- Copy the macros/keyfinder.sql file to your macros folder
- Run `dbt run-operation keyfinder --args '{table_fqn: "db.schema.table", depth: 3, keycount: 1}'
