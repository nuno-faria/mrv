import sys
import pandas as pd
import os
import re

if len(sys.argv) < 3:
    exit('Missing filename with CSV data and output name.')
    
if not os.path.isfile(sys.argv[1]):
    exit(f'File {sys.argv[1]} does not exist.')

df = pd.read_csv(sys.argv[1])
df = df.pivot(index='type', columns='clients', values='size')
nClients = df.columns.size
df.loc['1 hotspot'] = df.loc['1-hotspot']
df.loc['1 column'] = df.loc['1-column-mrv'] / df.loc['1-column-native']
df.loc['TPC-C'] = df.loc['tpcc-mrv'] / df.loc['tpcc-native']
df.loc['1 col. static'] = [df.loc['1-column-static'].values[0] / df.loc['1-column-native'].values[0]] * nClients
df = df.loc[['1 hotspot', '1 column', 'TPC-C', '1 col. static']]
df = df.add_suffix('_col')

with open(sys.argv[2], 'w') as f:
    f.write('\\documentclass{standalone}\n')
    f.write('\\usepackage{booktabs}\n')
    f.write('\\begin{document}\n')
    table = df.to_latex(index_names=False, column_format='l' + ('c' * nClients))
    table = table.replace('clients', '\\textbf{\# of clients}')
    table = re.sub(r'(\d+)_col', r'\\textbf{\1}', table)
    table = re.sub(r'(\.\d{2})\d+', r'\1', table)
    singleHotspotRow = re.findall(r'1 hotspot.*\n', table)[0]
    singleHotspotRow = re.sub(r'(\d+)\.\d+', r'\\textit{\1}', singleHotspotRow)
    table = re.sub(r'1 hotspot.*\n', "###" , table)
    table = table.replace("###", singleHotspotRow)
    table = table.replace("1 col. static", "\\hline  1 col. static ")
    table = re.sub(r'(?<=1 col. static)\s*&\s*(\d+\.\d+).*?(?=\\)', r'& \\multicolumn{' + str(nClients) + r'}{c}{\1}', table)
    f.write(table)
    f.write('\\end{document}\n')
