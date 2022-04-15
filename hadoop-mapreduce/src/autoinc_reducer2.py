# !/Users/nancypham/opt/anaconda3/bin/ python3

import sys
import re

def reset(count):
    count = 0
    return count

# Run for end of every group
def flush(current_make_year, count):
    print(f'{current_make_year}\t{count}')

def reducer():
    '''
    Reads mapper output. Within each makeyear group, iterate through all the records to find the one that has the make and year available and captures that in group-level master info. Filter accident records and modify by adding the master info before outputing the accident records.

    Assumption: mapper output sorted by key (i.e. vin) before passed to the reducer.
    '''
    current_make_year = None
    count = 0
    
    for line in sys.stdin:
        make_year = line.strip().split('\t', 1)[0]

        if current_make_year != make_year:
            if current_make_year != None:
                flush(current_make_year, count)
            count = reset(count)

        current_make_year = make_year
        count += 1

    # Output the last group if needed
    flush(current_make_year, count)

if __name__ == '__main__':    
    reducer()

'''
Execution log:
Mercedes2015	2
Mercedes2016	1
Nissan2003	1
'''