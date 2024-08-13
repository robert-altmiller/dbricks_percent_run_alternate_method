# Databricks notebook source
from fix_imports import *

# COMMAND ----------

notebook_import_order = ["library_imports", "inventory_data", "products_data"]

def get_order_index(path):
  """function to get the index from notebook_import_order"""
  for idx, name in enumerate(notebook_import_order):
      if name in path:
          return idx
  return len(notebook_import_order)  # Return a high index if not found


if is_running_in_databricks() == True: # code is running in Databricks
  dir_path = f"/Workspace{dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()}"  # Path to the notebook in Databricks
  dir_path_parent = '/'.join(dir_path.split('/')[:-1])
  notebook_paths = get_notebook_paths(spark, dbutils, dir_path_parent)
  notebook_paths = sorted(notebook_paths, key = get_order_index)
  notebook_paths.remove(dir_path)  
  original_globals = globals()
  current_globals = get_imports(spark, dbutils, notebook_paths, original_globals)
else: # code is running locally (order could be important here)
  for nb in notebook_import_order:
    exec(f"from {nb} import *", globals())

# COMMAND ----------

# Initialize Spark session
if is_running_in_databricks() == False: # code is running locally
  spark = SparkSession.builder \
      .appName("Inventory Join Example") \
      .getOrCreate()

# Create DataFrames from the sample data
products_df = spark.createDataFrame(products_data, ["product_id", "product_name", "category"])
inventory_df = spark.createDataFrame(inventory_data, ["warehouse_id", "product_id", "quantity"])

# Show the DataFrames
print("Products DataFrame:")
products_df.show()

print("Inventory DataFrame:")
inventory_df.show()

# Perform an inner join on product_id
joined_df = products_df.join(inventory_df, on="product_id", how="inner")

# Show the result of the join
print("Joined DataFrame:")
joined_df.show()

if is_running_in_databricks() == False: # code is running locally
  # stop the Spark session
  spark.stop()
