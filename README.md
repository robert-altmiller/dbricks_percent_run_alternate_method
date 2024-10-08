# dbricks_percent_run_alternate_method

## Replacement for '%run" in Databricks using Exec() and Globals() Namespace<br><br>

Running unit and integration tests in Databricks and locally can be challenging when using the %run command in Databricks to execute dependencies from other notebooks. Often, codebases integrate '.py' file dependencies with Databricks notebooks, which work seamlessly within Databricks but present challenges when executed locally in code editors like Visual Studio Code (VSCode). The '%run' command, specific to Databricks, does not function in VSCode, meaning that if your notebooks rely on it in your DevOps repository, they won't execute properly locally.

This GitHub repository offers an alternative solution that replicates the behavior of the '%run' command. It uses Databricks REST API 2.0 calls to fetch the content of Databricks notebooks, executes the code using Python's exec() function, and integrates the executed code and results into the active session global namespace, similar to how the 'from file1 import *' command works.

### Scenario 1: Import Databricks Notebooks Locally in VSCode

- You can import Databricks Notebooks directly in a local Python code file using "from notebook1 import *".  This method works because all Databricks notebooks are sync'ed back to a devops repository (e.g. Github) with a .py extension so they can be use locally like regular '.py' files.  
- See code Python code snippit below to import the library_imports.py, inventory_data.py, and products_data.py Databricks notebooks into a local Python code file.

  ```Python
  notebook_import_order = ["library_imports", "inventory_data", "products_data"]
  for nb in notebook_import_order:
      exec(f"from {nb} import *", globals())
  ```

### Scenario 2: Import Databricks Notebooks in Other Databricks Notebooks Not Using '%run' Within Databricks

- If any of your Databricks notebooks in your devops repository are using '%run' command to import and run other Databricks notebooks these notebooks will not work locally in VSCode without changes.
- You will need an alternative method which replicates what the "%run" command does.  You __CANNOT__ replace '%run ./notebook1' with 'from notebook1 import *' within another Databricks notebook in Databricks.  If you could then this would be an easy fix for running Databricks notebook locally in VSCode and in Databricks. 
- Instead you can achieve the same functionality as '%run' within a Databricks notebook by using a Databricks Rest API 2.0 call to read the contents of the Databricks notebook you want to import, then you execute that code using exec(code, globals()).  This command also integrates the executed code and results into the active session global namespace.  
- If you reference the __main.py__ code below this section where __is_running_in_databricks() == True__ is the replacement for '%run' within Databricks.

  ```Python
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
  ```

## See The Steps Below For How to Get Started With the %run Replacement

### Step 1: Clone Down the 'dbricks_percent_run_alternate_method' Repo into Databricks Workspace: <br>

- git clone https://github.com/robert-altmiller/dbricks_percent_run_alternate_method.git

### Step 2: Update User Defined Parameters For Notebook Imports

- Open the __'main.py'__ Databricks notebook, and update the the __'notebook_import_order'__ Python list to specify the order of the root level notebooks (e.g. inventory_data.py, products_data.py, library_imports.py) for import into the __'main.py'__ notebook.  Notebook import order is important because one notebook might be dependent on other Databricks notebooks.  Commit your changes back to the repository from the Databricks workspace.

  ![update_parameters.png](/readme_images/update_parameters.png)

### Step 3: Clone the 'dbricks_percent_run_alternate_method' down repo locally in VSCode.

![vscode_clone.png](/readme_images/vscode_clone.png)

### Step 4: Execute the 'main' notebook in Databricks.

![run_main_in_databricks.png](/readme_images/run_main_in_databricks.png)

### Step 5: Execute the 'main.py' locally in VSCode.

![run_main_in_vscode.png](/readme_images/run_main_in_vscode.png)

### Step 5: How to verify successful execution of the main notebook?

- The 'products_data' notebook has the data for __'products_df'__ Spark dataframe, and the 'inventory_data' notebook has the data for __'inventory_df'__ Spark dataframe in the 'main' notebook.  These two notebooks have to be imported into the 'main' notebook successfully in order to display the __'inventory_df'__, __'products_df'__, and __'joined_df'__ dataframes successfully in the 'main' notebook run.  The Python library imports in the 'library_imports' notebook also needs to be successfully imported into the 'main' notebook to run certain Spark commands like creating a local Spark Session.

- Here is the output of what a successful run looks like:

  ![successful_main_nb_run.png](/readme_images/successful_main_nb_run.png)
