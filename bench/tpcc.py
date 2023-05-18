import subprocess
import signal
import os
import argparse

parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('engine', type=str, help='Benchmark driver (postgres|monetdb*|tique|singlestore|tidb)')
parser.add_argument('-w', '--warehouses', type=int, help='Number of warehouse', action='store', required=True)
parser.add_argument('-c', '--clients', type=int, help='Number of clients', action='store', required=True)
parser.add_argument('-t', '--time', type=int, help='Duration in seconds', action='store', default=60)
parser.add_argument('-s', '--scale', type=int, help='Scale factor', action='store', required=True)
parser.add_argument('--populate', help='Only populate the database', action='store_true')
parser.add_argument('--no-load', help='Execute the benchmark without populating', action='store_true')
parser.add_argument('--soft-reset', help='Execute the benchmark without populating but clear metadata tables (tique)', action='store_true')
parser.add_argument('--contention', type=int, help='Execute the contention benchmark with the provided number of warehouses', default=0)
args = parser.parse_args()


command = [
    "python3",
    "tpcc.py",
    "--no-load" if args.no_load or args.soft_reset else "--load-from-csv",
    f"--warehouses={args.warehouses}",
    f"--scalefactor={args.scale}",
    f"--clients={args.clients}",
    f"--duration={args.time}",
    f'--contention={args.contention}',
    args.engine
]

if args.populate:
    command.append('--no-execute')
    command.append('--reset')
elif args.soft_reset:
    command.append('--soft-reset')

p = subprocess.Popen(command, cwd='py-tpcc/pytpcc')

def term(*args):
    os.kill(p.pid, signal.SIGTERM)

signal.signal(signal.SIGTERM, term)

p.wait()
