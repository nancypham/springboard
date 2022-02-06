# !/Users/nancypham/opt/anaconda3/bin/ python3

import sys

def mapper():
    '''
    Reads input data from autoinc_reducer1. Returns print of key and value
    
    Output composite key: makeyear
    Output value: 1
    '''
    for line in sys.stdin:
        record = line.strip().split(',')
        key = str(record[1].strip() + record[2].strip())
        value = 1
        print(f'{key}\t{value}')

if __name__ == '__main__':
     mapper()

'''
Execution log:
Mercedes2016	1
Mercedes2015	1
Mercedes2015	1
Nissan2003	1
'''