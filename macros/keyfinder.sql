{% macro keyfinder(table_fqn, depth, keycount) %}
{#
    get_keys procedure, for determining the unique key column / columns of a given snowflake table.
    table_fqn: fully qualified name, as in db.schema.table. Table name is case sensitive; db/schema are not.
    depth: number of columns to join together in the search for uniqueness - this is recursive, so higher numbers may be expensive. Maximum 3.
    keycount: minimum number of keys to return. Suggest starting with 1-2 for any given table.
#}


{% set create_function %}
WITH get_keys AS PROCEDURE (table_fqn string, depth integer, keycount integer)
RETURNS array
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ( 'snowflake-snowpark-python')
HANDLER = 'run'
AS
$$



def run(session, table_fqn, depth, keycount):
    output_columns = []
    unique_keys = []
    total_rows = session.sql('select count(*) from ' + table_fqn).collect()[0][0]
    dbname = table_fqn.split(".")[0]
    schemaname = table_fqn.split(".")[1]
    tablename = table_fqn.split(".")[2]
    all_columns = session.sql('select column_name, data_type from ' + dbname + '.information_schema.columns where table_schema = \'' + schemaname + '\' and table_name = \'' + tablename + '\'').collect()
    for i in all_columns:
        column_name = i[0]
        this_column_count = session.sql('select count(distinct "' + i[0] + '") from ' + table_fqn).collect()
        this_column_details = {"column": column_name, "datatype": i[1], "distinct_values": this_column_count[0][0]}
        output_columns.append(this_column_details)
    for i in output_columns:
        # Priority is to put keys at the top of the list - they are more likely to be a valid key.
        i["priority"] = ("key" in i['column'].lower() or "id" in i['column'].lower() or "pk" in i['column'].lower())
        # Order priority is for catching key + timestamp / version columns, which alongside a key should be top of the list.
        # The below catches timestamp, timestamp_xtz, number, numeric, all the ints, all the floats.
        i["order_priority"] = ("time" in i['datatype'] or "num" in i['datatype'] or "int" in i['datatype'] or "float" in i['datatype'])
        
    # Rank the columns in order of most distinct, then by priority column to put keys first
    output_columns = sorted(output_columns, key=lambda d: (d['distinct_values'], d['priority'], d['order_priority']), reverse=True)
    
    # Take the first 5 most distinct columns, and loop through them
    for i in output_columns[0:5]:
        if i['distinct_values'] == total_rows:
            # No need to add other columns for those already unique
            unique_keys.append(i)
        elif depth > 1:
            for j in output_columns:
                if len(unique_keys) >= keycount:
                    break
                if not j['distinct_values'] > i['distinct_values'] and j['column'] != i['column']: # Ignore more distinct columns; they'll have matched the other way around
                    two_column_count = session.sql('select count(distinct "' + i['column'] + '", "' + j['column'] + '") from ' + table_fqn).collect()
                    if two_column_count[0][0] == total_rows:
                        two_column_details = {"column": [i['column'], j['column']], "distinct_values": two_column_count[0][0]}
                        unique_keys.append(two_column_details)
                    elif depth > 2:
                        kcount = 0
                        for k in output_columns:
                            if len(unique_keys) >= keycount:
                                break
                            if not k['distinct_values'] > j['distinct_values'] and k['column'] != i['column'] and k['column'] != j['column']: # Ignore more distinct columns; they'll have matched the other way around
                                
                                kcount += 1
                                if kcount >= 5: # only run top 5 most distinct below the j; k should be rare
                                    break
                                three_column_count = session.sql('select count(distinct "' + i['column'] + '", "' + j['column'] + '", "' + k['column'] + '") from ' + table_fqn).collect()
                                if three_column_count[0][0] == total_rows:
                                    three_column_details = {"column": [i['column'], j['column'], k['column']], "distinct_values": three_column_count[0][0]}
                                    unique_keys.append(three_column_details)
    
    if len(unique_keys) > 0:
        # We found unique keys
        return unique_keys
    else:
        return output_columns_sorted
$$
call get_keys ('{{ table_fqn }}', {{ depth }}, {{ keycount }});
{% endset %}

{% set wrap_results %}
select
    all_columns.value:column as column_name,
    all_columns.value:column  as data_type,
    all_columns.value:distinct_values  as count_distinct_values
from table(result_scan(last_query_id())) last_result,
lateral flatten(input => last_result.get_keys) all_columns;
{% endset %}

{% do run_query(create_function) %}

{% set results = run_query(wrap_results) %}

{{ results }}
{{ log(results.print_table(), info=True) }}

{% endmacro %}