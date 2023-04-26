import csv
import psycopg2

def shorten_field_name(field):
    return field.replace(" ", "_").replace("-", "").replace('"', '')[:63]

def determine_data_type(column_values):
    is_integer = True
    is_float = True

    for value in column_values:
        try:
            int(value)
        except ValueError:
            is_integer = False
            break

    if not is_integer:
        for value in column_values:
            try:
                float(value.replace(',', '.'))
            except ValueError:
                is_float = False
                break

    return 'integer' if is_integer else 'float' if is_float else 'text'

# Read and parse the CSV data
csv_data = []
header = []
with open('sample_data.csv', newline='', encoding='ISO-8859-1') as csvfile:
    reader = csv.reader(csvfile, delimiter=';')
    for index, row in enumerate(reader):
        if index == 0:
            header = [shorten_field_name(k) for k in row]
            continue
        if len(row) == len(header):
            csv_data.append({header[i]: row[i].replace(',', '.') if determine_data_type(row[i]) == 'float' else row[i] for i in range(len(row))})
# Determine the data types of the columns
columns_data = {shorten_field_name(k): [row[k] for row in csv_data] for k in header}
fields = {k: determine_data_type(v) for k, v in columns_data.items()}

# Connect to PostgreSQL
conn = psycopg2.connect("dbname=df5rqre4ki72of user=fgovdlbidegvaw password=29e6c31f24efa103b9d08b264abed0471459fcdfce736ef38d87ae8e8229b20d host=ec2-52-215-12-242.eu-west-1.compute.amazonaws.com port=5432")
cur = conn.cursor()

# Create the "survey_results" table
create_table_query = 'CREATE TABLE survey_results (' + ' ,'.join([f'"{k}" {v}' for k, v in fields.items()]) + ')'
cur.execute(create_table_query)

# Insert the parsed CSV data into the "survey_results" table
for row in csv_data:
    row_values = [row.get(k, None) for k in fields.keys()]
    insert_query = 'INSERT INTO survey_results (' + ' ,'.join([f'"{k}"' for k in fields.keys()]) + ') VALUES (' + ' ,'.join(['%s'] * len(fields)) + ')'
    cur.execute(insert_query, tuple(row_values))

# Commit the changes and close the connection
conn.commit()
cur.close()
conn.close()
