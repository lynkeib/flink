# Import required modules from the pyflink.table package
from pyflink.table import EnvironmentSettings, TableEnvironment
# Import column operations for table expressions
from pyflink.table.expressions import col

# ---- Step 1: Setting up the Table Environment ----
# Initialize environment settings for a streaming TableEnvironment
env_settings = EnvironmentSettings.in_streaming_mode()
# Create a TableEnvironment using the streaming settings
table_env = TableEnvironment.create(env_settings)

# ---- Step 2: Define and Create the Source Table ----
# Define a new table "datagen" with columns "id" and "data"
# The data for this table is generated using the "datagen" connector
# The 'id' column values are a sequence ranging from 1 to 10
table_env.execute_sql("""
    CREATE TABLE datagen (
        id INT,
        data STRING
    ) WITH (
        'connector' = 'datagen',
        'fields.id.kind' = 'sequence',
        'fields.id.start' = '1',
        'fields.id.end' = '10'
    )
""")

# ---- Step 3: Define and Create the Sink Table ----
# Define a new sink table "print" to display the output results
# This table uses the 'print' connector to print the data to the console
table_env.execute_sql("""
    CREATE TABLE print (
        id INT,
        data STRING
    ) WITH (
        'connector' = 'print'
    )
""")

# ---- Step 4: Data Processing ----
# Retrieve data from the "datagen" table and store it in "source_table"
source_table = table_env.from_path("datagen")
# Alternatively, you can use SQL to fetch data:
# source_table = table_env.sql_query("SELECT * FROM datagen")

# Perform calculations on the "source_table"
# In this case, we're incrementing the "id" value by 1
result_table = source_table.select(col("id") + 1, col("data"))

# ---- Step 5: Writing Results to the Sink ----
# Emit the processed data (stored in "result_table") to the "print" sink table
result_table.execute_insert("print").wait()
# Alternatively, you can use SQL to insert the data:
# table_env.execute_sql("INSERT INTO print SELECT * FROM datagen").wait()
