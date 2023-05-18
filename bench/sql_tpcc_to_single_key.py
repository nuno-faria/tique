import multiprocessing
import sys
import os


def process_file(args):
    filename = args[0]
    sql_dir = args[1]
    out_dir = args[2]

    with open(os.path.join(sql_dir, filename)) as f:
        with open(os.path.join(out_dir, filename), 'w') as out:
            for line in f:
                row = line[:-1].split('|')

                if 'district' in filename:
                    row.append(row[1] + '.' + row[0])
                
                elif 'customer' in filename:
                    row.append(row[2] + '.' + row[1] + '.' + row[0])
                    row.append(row[2] + '.' + row[1] + '.' + row[5])

                elif 'stock' in filename:
                    row.append(row[1] + '.' + row[0])

                elif 'orders' in filename:
                    row.append(row[3] + '.' + row[2] + '.' + row[0])

                elif 'new_order' in filename:
                    row.append(row[2] + '.' + row[1] + '.' + row[0])
                    row.append(row[2] + '.' + row[1])
                
                elif 'order_line' in filename:
                    row.append(row[2] + '.' + row[1] + '.' + row[0])
                    row.append(row[2] + '.' + row[1])

                out.write('|'.join(row) + '\n')


sql_dir = os.path.join(sys.argv[1], '')
out_dir = os.path.dirname(sql_dir) + '_single_pk'
try:
    os.mkdir(out_dir)
except:
    pass

pool = multiprocessing.Pool(multiprocessing.cpu_count())
pool.map(process_file, [(filename, sql_dir, out_dir) for filename in os.listdir(sql_dir)])
