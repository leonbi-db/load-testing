# Databricks notebook source
dbutils.widgets.text("num_cells", "20")
dbutils.widgets.text("kb_per_cell", "1000")

cell_text_size = int(dbutils.widgets.get("kb_per_cell"))*1000
num_cells = int(dbutils.widgets.get("num_cells"))

# COMMAND ----------


e2_workspace = { 
    "url": 'https://e2-dogfood.staging.cloud.databricks.com', 
    "api_token": dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
}


# COMMAND ----------

import requests
from datetime import datetime
from collections import Counter
import json
import pyspark.sql as sql
import time
import uuid
from csv import DictWriter
from pathlib import Path
import time
import shutil
import pathlib


exp_id = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4()}"
test_name = "gns_large_notebook"

counter = Counter()

def log(msg):
    txt = f"{datetime.now().strftime('%Y-%m-%d-%H:%M:%S')} {msg}"
    print(txt)
    _plogger.write_log_to_file(txt)

class PersistentLogger():
    def __init__(self, exp_id, suffix):
        self.dbfs_log = f"/dbfs/sai/{test_name}/{exp_id}/{suffix}"
        self.tmp_log = f"/tmp/{test_name}_{exp_id}-{suffix}"
        pathlib.Path(f"/dbfs/sai/{test_name}/{exp_id}").mkdir(parents=True, exist_ok=True)    
        self.log_flush_time = time.time()
        self.handle = open(self.tmp_log, 'w')
    
    def write_log_to_file(self, txt):
        self.handle.write(txt)
        self.handle.write('\n')
        if self.log_flush_time + 10 < time.time():
            self.flush()

    def flush(self):
        self.handle.flush()
        shutil.copy(str(self.tmp_log), str(self.dbfs_log))
        self.log_flush_time = time.time()
        print(f"{datetime.now().strftime('%Y-%m-%d-%H:%M:%S')} flushed log")

_plogger = PersistentLogger(exp_id, "driver")

# COMMAND ----------

import requests
from datetime import datetime
from collections import Counter
import json
import pyspark.sql as sql
import time
import uuid
from csv import DictWriter
from pathlib import Path
import time
import base64

def gen_traceid():
    uuid1 = str(uuid.uuid4()).replace("-", "")
    uuid2 = str(uuid.uuid4()).replace("-", "")
    return f"00-{uuid1}-{uuid2[16:]}-01"




def base64_notebook_text():
    cell_text = "".join(['0' for i in range(cell_text_size)])
    nb_join = "\n# COMMAND ----------\n"
    notebook_text = nb_join.join([cell_text for _ in range(num_cells)])
    notebook_text = "# Databricks notebook source\n" + notebook_text
    return base64.b64encode(notebook_text.encode()).decode()
    return 


import uuid
def import_notebook(workspace, dir_name):
    path = f"{dir_name}/testing_{uuid.uuid4()}"
    resp = requests.post(
        f'{workspace["url"]}/api/2.0/workspace/import', 
        headers = {"Authorization": f'Bearer {workspace["api_token"]}'},
        json={ 
            "path": path, 
            "content": base64_notebook_text(), 
            "language": "PYTHON", 
            "overwrite": True, 
            "format": "SOURCE"
        }
    )
    log(resp.text)
    assert(resp.status_code == 200)  
    return path

def gns(workspace, path, description=None):
    trace_parent = gen_traceid()
    start_time = time.time()
    status_code = 200
    try:
        resp = requests.get(
            f'{workspace["url"]}/api/2.0/workspace/get-notebook-snapshot', 
            headers = {
                "Authorization": f'Bearer {workspace["api_token"]}',
                "traceparent": trace_parent
            },
            json={ "path": path }
        )
        log(f"Response size is {len(resp.text)}")
        if resp.status_code != 200:
            log(f"Error: {resp.text}")
        assert(resp.status_code == 200)
        status_code = resp.status_code  
    except HTTPException as e:
        status_code = e.status
    except Exception as e:
        status_code = -1

    end_time = time.time()
    # log(f"batch_get_signed_urls end   = {end_time}, took {end_time-start_time}s")
    return {
        "description": description or "na",
        "timestamp_str": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "duration": end_time - start_time,
        "operation": f"gns",
        "trace_parent_id": trace_parent,
        "workspace_url": workspace["url"],
        "status":  status_code
    }


# COMMAND ----------

nb_path = import_notebook(e2_workspace, "/Users/sai.suram@databricks.com")

# COMMAND ----------

def sequential_operation(workspace, debug_info, num_secs, append_res_to=None):
    start_sec = time.time()
    ret = []
    while time.time() < num_secs + start_sec:
        ret.append(gns(workspace, nb_path))
    if append_res_to is not None:
        append_res_to.extend(ret)
    return ret

import threading
def parallel_operation(workspace, note, pism, num_secs, append_res_to):
    threads = []
    for i in range(pism):
        t = threading.Thread(target=lambda: sequential_operation(workspace, f"{note} pism:{pism}", num_secs, append_res_to))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()

# COMMAND ----------

res1 = []
parallel_operation(e2_workspace, "default", pism=1, num_secs=120, append_res_to=res1)

# COMMAND ----------

display(res1)

# COMMAND ----------

res5 = []
parallel_operation(e2_workspace, "default", pism=5, num_secs=120, append_res_to=res5)

# COMMAND ----------

display(res5)
