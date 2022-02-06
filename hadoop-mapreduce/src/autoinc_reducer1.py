# !/Users/nancypham/opt/anaconda3/bin/ python3

import sys
import re

def reset(make_year):
    make_year = []
    return make_year

def flush(make_year):
    '''
    Write result to STDOUT. Run at end of every group.
    '''
    print(f'{make_year[0]}, {make_year[1]}, {make_year[2]}')

def reducer():
    '''
    Reads mapper output. Within each vin_number group, iterate through all the records to find the one that has the make and year available and captures that in group-level master info. Filter accident records and modify by adding the master info before outputing the accident records.

    Assumption: mapper output sorted by key (i.e. vin) before passed to the reducer.
    '''
    current_vin = None
    make_year = []

    for line in sys.stdin:
        vin, record = line.strip().split('\t', 1)
        
        if current_vin != vin:
            if current_vin != None and make_year[0] == 'A':
                flush(make_year)
            make_year = reset(make_year)

        current_vin = vin

        value = record.split(',')
        value = [re.sub('[\W_]', '', x) for x in value] # Clean stringified list

        if value[0] == 'A':
            make_year.append('A')
        else:
            make_year.append('')
        
        if value[0] == 'I':
            make_year[1:2] = [value[1+i] for i in range(2)]

    # Output the last group if needed
    flush(make_year)

if __name__ == '__main__':    
    reducer()

'''
Execution log:
A, Mercedes, 2016
A, Mercedes, 2015
A, Mercedes, 2015
A, Nissan, 2003
'''