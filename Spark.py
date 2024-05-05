# Pyspark application to read from mongoDB and write a dataframe to mySQL server
# How can I add to an already existing table? - Could use the pyspark sql connector library, set up and connection and run an insert qeury
# Problems - May need a separate table to store arrays that have multiple values and then relate that table to the patient table 

# API imports
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as f

# Functions to flatten my dataframe
def flatten_structs(nested_df):
    # Stack should start with a single tuple containing an empty parent node and the nested df 
    stack = [((), nested_df)]
    # Will build this list with flattened columns
    columns = []

    while len(stack) > 0:
        # parents - empty tuple, df - nested dataframe -->> pop from stack 
        parents, df = stack.pop()

        # Gives a list of all the non-struct elements of the nested dataframe ie: "gender":"male"
        # Upon successive iterations this will concatenate the parent columns from a previous iteration with their child columns, so the struct name is preserved
        flat_cols = [
            f.col(".".join(parents + (c[0],))).alias("_".join(parents + (c[0],)))
            for c in df.dtypes
            if c[1][:6] != "struct"
        ]

        # Gives a list of all nested elements of the nested dataframe
        nested_cols = [
            c[0]
            for c in df.dtypes
            if c[1][:6] == "struct"
        ]

        # Adds the non-struct elements from the flat_cols list to the columns list
        columns.extend(flat_cols)

        # Iterates the list of all nested columns identified
        for nested_col in nested_cols:
            # Expands the current nested column and stores in a temp df
            projected_df = df.select(nested_col + ".*")

            # Adds the nested column to the parent path with its expanded children columns being represented by projected df
            # This allows us to process all the children columns of each nested column to check if they are also nested
            # Eventually the algorithm will not detect any other nested columns and the stack will be empty
            stack.append((parents + (nested_col,), projected_df))

    # Selects the list of flat columns and their values from the df
    return nested_df.select(columns)

def flatten_array_struct_df(df):
    # List comprehension that gives a list columns if that column is of type array
    # In this instance telecom, address and name would be added
    array_cols = [
            c[0]
            for c in df.dtypes
            if c[1][:5] == "array"
        ]
    
    # As long as there are array columns to process
    while len(array_cols) > 0:
        
        # Iterate over each array column
        for array_col in array_cols:
            # Replace the array column in the dataframe with its exploded version
            df = df.withColumn(array_col, f.explode(f.col(array_col)))

        # Gets rid of all the structs after removing all array types    
        df = flatten_structs(df)
        
        # Update the array cols list
        array_cols = [
            c[0]
            for c in df.dtypes
            if c[1][:5] == "array"
        ]
    return df

# create a spark session
# I could add a .filter() command to select the specific document I want to work with among other things
spark = SparkSession \
.builder \
.master("local") \
.appName("Spark FHIR App") \
.config("spark.driver.memory", "15g") \
.config("spark.mongodb.read.connection.uri", "mongodb://localhost:27017/fhir-resource") \
.config('spark.jars.packages', 'com.mysql:mysql-connector-j:8.0.33,org.mongodb.spark:mongo-spark-connector:10.0.2') \
.getOrCreate()

# read the document from mongo and store it in a dataframe
df = spark.read \
.format("mongodb") \
.option("uri", "mongodb://localhost:27017/fhir-resource") \
.option("database", "fhir-resource") \
.option("collection", "patients") \
.load() 

df.show()

# define mysql url, mysql username/ password, and driver required for jdbc connector
#jdbcUrl = "jdbc:mysql://127.0.0.1:3306/fhirdb"
#username = "root"
#password = "Thebigblue12!"
#driver = "com.mysql.cj.jdbc.Driver"

# Flatten to I can write the df to mysql
#flatDf = flatten_array_struct_df(df)

# Write to jdbc connector - mysql
# Driver class needed to provide the actual java class that allows me to implement the jdbc connector
#flatDf.write \
#  .format("jdbc") \
#  .option("driver", driver) \
#  .option("url", jdbcUrl) \
#  .option("dbtable", "patients") \
#  .option("user", username) \
#  .option("password", "Thebigblue12!") \
#  .save()



