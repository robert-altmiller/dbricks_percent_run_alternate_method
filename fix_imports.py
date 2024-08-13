import requests, base64, random, os, time, sys


def get_params(params = None):
    """get requests parameters"""
    params = {}
    return params


def get_headers(token = None):
    """get requests headers"""
    headers = {'Authorization': 'Bearer %s' % token}
    return headers


def post_request(url = None, headers = None, params = None, data = None):
    """post request"""
    if params != None:
        return requests.post(url, params = params, headers = headers, json = data)
    else: return requests.post(url, headers = headers, json = data)


def get_request(url = None, headers = None, params = None, data = None):
    """get request"""
    if params != None:
        return requests.get(url, params = params, headers = headers, json = data)
    else: return requests.get(url, headers = headers, json = data)


def get_api_config(dbricks_instance = None, api_topic = None, api_call_type = None, dbricks_pat = None):
    """databricks rest API 2.0 configuration"""
    config = {
        # databricks workspace instance
        "databricks_ws_instance": dbricks_instance,
        # databricks rest api version
        "api_version": "api/2.0",
        # databricks rest api service call
        "api_topic": api_topic,
        # databricks api call type
        "api_call_type": api_call_type
    }
    config["databricks_host"] = "https://" + config["databricks_ws_instance"]
    if api_topic != None and api_call_type != None:
      config["api_full_url"] = config["databricks_host"] + "/" + config["api_version"] + "/" + config["api_topic"] + "/" + config["api_call_type"]
    return config


def execute_rest_api_call(function_call_type, config = None, token = None, jsondata = None, params = None):
    """execute databricks rest API 2.0 call (generic)"""
    headers = get_headers(token)
    if params == None: params = get_params()
    response = function_call_type(url = config["api_full_url"], headers = headers, params = params, data = jsondata)
    return response


def get_nb_content(dbricks_instance, dbricks_pat, notebook_source_path):
  """convert Databricks notebook to Databricks python file"""
  jsondata = {}
  # Request payload
  params = {
    "path": notebook_source_path,
    "format": "SOURCE"  # Use "SOURCE" to get the notebook in its source format (Python, Scala, etc.)
  }
  response = execute_rest_api_call(get_request, get_api_config(dbricks_instance, "workspace", "export"), dbricks_pat, jsondata, params)

  if response.status_code == 200:
    # Get the content of the notebook as a base64 encoded string
    notebook_content_base64 = response.json().get("content")
    # Decode the base64 content to get the notebook source code
    notebook_content = base64.b64decode(notebook_content_base64).decode("utf-8")

  if len(notebook_content) > 0: return notebook_content
  else: return None


def get_notebook_paths(spark, dbutils, dir_path):
    """get Databricks notebook paths"""
    # Iterate over all files in the directory
    notebook_paths = []
    for root, dirs, files in os.walk(dir_path):
        for file in files:
            file_path = os.path.join(root, file)
            # Check if the path is a file and read the contents
            if os.path.isfile(file_path) and "fix_imports.py" not in file_path:
                try:
                    databricks_instance = spark.conf.get("spark.databricks.workspaceUrl") # format "adb-723483445396.18.azuredatabricks.net" (no https://) (don't change)
                    databricks_pat = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get() # format "dapi*****" (don't change)
                    nb_content = get_nb_content(databricks_instance, databricks_pat, file_path)

                    if "# Databricks notebook source" in str(nb_content):
                        notebook_paths.append(file_path)
                except: continue
    return notebook_paths


def is_running_in_databricks():
    """check if code is running in Databricks or locally"""
    # Databricks typically sets these environment variables
    if 'DATABRICKS_RUNTIME_VERSION' in os.environ:
        print("code is running in databricks....\n")
        return True
    else:
        #print("code is running locally....\n")
        return False


def fix_imports(spark, dbutils, nb_path, random_int_start, random_int_end):
    """fix Databricks notebook imports"""
    databricks_instance = spark.conf.get("spark.databricks.workspaceUrl") # format "adb-723483445396.18.azuredatabricks.net" (no https://) (don't change)
    databricks_pat = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get() # format "dapi*****" (don't change)
    nb_content = get_nb_content(databricks_instance, databricks_pat, nb_path)

    parent_dir_path = '/'.join(nb_path.split('/')[:-1])
    python_file_name = f"{nb_path.rsplit('/', 1)[1]}_{random.randint(random_int_start, random_int_end)}.py"
    python_file_path = f"{parent_dir_path}/{python_file_name}"
    print(python_file_path)
    with open(python_file_path, "w") as file:
        file.write(nb_content)
    file.close()
    
    module_name = python_file_name[:-3]  # Remove .py extension
    return module_name, python_file_path, f"{parent_dir_path}/", nb_content


def get_imports(spark, dbutils, notebook_paths, globals):
    """get Databricks notebook imports"""
    for nb_path in notebook_paths:
        module_name, module_path, parent_dir_path, nb_content = fix_imports(spark, dbutils, nb_path, 0, 100)
        if nb_content != None:
            exec(nb_content, globals)
            print(f"imported and executed code in module: {module_name}")
            os.remove(f"{module_path}")
            print(f"removed module_path: {module_path}\n")
    return globals
            