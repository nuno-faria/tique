import subprocess
import sys
import os
import shutil

if len(sys.argv) < 3:
    exit('Usage: python3 gen_data.py <num_warehouses> <scale_factor>')

warehouses = sys.argv[1]
scale = sys.argv[2]

out_folder = f'py-tpcc/pytpcc/tpcc_data_{warehouses}_{scale}'
if os.path.exists(out_folder):
    shutil.rmtree(out_folder)

os.makedirs(out_folder)

subprocess.run(["python3", "tpcc.py", "--no-execute", "--clients=64", f"--warehouses={sys.argv[1]}", "--reset", f"--scalefactor={sys.argv[2]}", "sqlitepopulate"], cwd='py-tpcc/pytpcc')

subprocess.run(["cp", "order_line.csv", "order_line_mysql.csv"], cwd=out_folder)
subprocess.run(["sed", "-i", r"s/|None|/|\\N|/", "order_line_mysql.csv"], cwd=out_folder)
