# !/Users/nancypham/opt/anaconda3/bin/ python3

import sys

def mapper():
    '''
    Reads input data and propagates data. Returns print of key and value

    Output key: vin_number
    Output value: incident_type, make, year
    '''
    for line in sys.stdin:
        record = line.strip().split(',')
        key = record[2].strip()
        value = record[1].strip(), record[3].strip(), record[5].strip()
        print(f'{key}\t{value}')

if __name__ == '__main__':
    mapper()

'''
Execution log:
VXIO456XLBB630221	('I', 'Nissan', '2003')
INU45KIOOPA343980	('I', 'Mercedes', '2015')
VXIO456XLBB630221	('A', '', '')
VXIO456XLBB630221	('R', '', '')
VOME254OOXW344325	('I', 'Mercedes', '2015')
VOME254OOXW344325	('R', '', '')
VXIO456XLBB630221	('R', '', '')
EXOA00341AB123456	('I', 'Mercedes', '2016')
VOME254OOXW344325	('A', '', '')
VOME254OOXW344325	('R', '', '')
EXOA00341AB123456	('R', '', '')
EXOA00341AB123456	('A', '', '')
VOME254OOXW344325	('R', '', '')
UXIA769ABCC447906	('I', 'Toyota', '2017')
UXIA769ABCC447906	('R', '', '')
INU45KIOOPA343980	('A', '', '')
'''