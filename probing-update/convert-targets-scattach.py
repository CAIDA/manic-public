# coding: utf-8
# In[2]:

import csv
import sys

monitor = str(sys.argv[1])
input_file = sys.argv[2]
#input_path + monitor + '.dot1.max3.targets'
output_file = sys.argv[3]
#input_path + monitor + '.sc-targets'
    
with open(input_file, 'rb') as f: #import file

    reader = csv.reader(f, delimiter=' ') #read file into variable reader, delimited using space
    g = open(output_file,'w+')
    for row in reader: #for as long as there is file
            
        #read values from file: IP, TTL, ICMP checksum
        ip = row[2]
        ttl = row[3]
        icmp = row[5]
            
    	#format data for sc_attach to read:
    	#ping -C <sum> -M <ttl> -c 1 <destination-ip>
    	line = 'ping -C ' + icmp + ' -m ' + ttl + ' -c 1 ' + ip + '\n'
    	g.write(line) # python will convert \n to os.linesep
            
f.close() #close files
g.close()
