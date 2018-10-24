#This is the command line interface to process the traces from a given AS. The set of traces to trace is specified in the measurement sequence_data_1.
#It calls process_one_trace to do the actual work. 
#If process_one_trace determines that an assertion about congestion needs human validation, it queues that task using the tinydb package (this will be replaced with a better task flow management tool in the future.)

# import modules used here 
import sys
import os
import numpy as np
import matplotlib.pyplot as plt
from scipy import signal
from weeks import *
import time
import statistics as st
import math as m
from influxdb import InfluxDBClient
from tinydb import TinyDB, Query
import ipdb
import random
import pandas as pd
from pandas.plotting import scatter_matrix
from process_trace_bulk import process_one_trace


def process_days(method, test):
    
    os.environ['TZ'] = 'UTC'
    time.tzset()
    #Force pyplot to use UTC time. There must be a better way...
    uid_seed = random.randint(1,1000000)
    print 'UID', uid_seed
    
    debug = False
    
    test = False
    
    ASs = open('AS-table-short.txt', 'rU')
    ASdict = {}
    for lines in ASs:
        ASnumstr, AStext = lines.strip('\n').split(',')
        ASdict[ASnumstr] = AStext
    ASs.close()
    
    AS = open('AS-table.txt', 'rU')
    ASdictlong = {}
    for lines in AS:
        if len(lines) > 0 and lines[0] == 'A':
            ASnumstr = lines.split()[0][2:] #throw away the AS
            AStextlist = lines.split()[1:]
            ASdictlong[ASnumstr] = " ".join(AStextlist)
    AS.close()
    
    def get_ASname(asn):
        if asn in ASdict.keys():
            AS_name = ASdict[asn]
        elif asn in ASdictlong.keys():
            AS_name = ASdictlong[asn]
        else:
            AS_name = 'Unknown'
        return AS_name
    ASo = open('AS-omit.txt', 'rU')
    ASomit = {}
    for lines in ASo:
        if len(lines) > 0 and lines[0] != '#':
            ASnumstr, ASoname = lines.strip('\n').split(',') #throw away the AS
            ASomit[ASnumstr] = ASoname
    ASo.close()
    
    
    client = InfluxDBClient('beamer.caida.org', 8086,  NAME, PASSWORD,, 'tspmult', ssl = True, verify_ssl=True)
    metaclient = InfluxDBClient('beamer.caida.org', 8086,  NAME, PASSWORD, 'tspmeta', ssl = True, verify_ssl=True)
    

    inputstr = sys.argv[1] #the origin AS from which to report congestion
    if inputstr.isdigit(): 
        cnasn = inputstr
        xmons = client.query("""SELECT * FROM "names" WHERE "asn" = '""" +\
        cnasn + """'""")
        mon_set = set()
        m = xmons.items()[0][1]
        for t in m:
            mon_set.add(t['mon'])
        # now we have the mon list. Get the primary AS number.
        pasn_set = set()
        m = xmons.items()[0][1]
        for t in m:
            pasn_set.add(t['primary_asn'])
        if len(pasn_set) == 0:
            print 'AS error: no AS entry for monitor {}'.format(mon)
            return
        if len(pasn_set) > 1:
            print 'AS error: More than one primary AS for this provider'
            return
        pasn = pasn_set.pop() 
        
    else: #find the primary AS for the monitor, then the mon list
        xasnl = client.query("""SELECT * FROM "names" WHERE "mon" = '""" +\
        inputstr + """'""")
        pasn_set = set()
        m = xasnl.items()[0][1]
        for t in m:
            pasn_set.add(t['primary_asn'])
        if len(pasn_set) == 0:
            print 'AS error: no AS entry for monitor {}'.format(mon)
            return
        if len(pasn_set) > 1:
            print 'AS error: More than one primary AS for this provider'
            return
        pasn = pasn_set.pop()
        xmons = client.query("""SELECT * FROM "names" WHERE "asn" = '""" +\
        pasn + """'""")
        mon_set = set()
        m = xmons.items()[0][1]
        for t in m:
            mon_set.add(t['mon'])
    mon_list = list(mon_set)
                    
    mon_name = inputstr[0:4] 
    if mon_name[3] == '-':
        mon_name = inputstr[0:3]      
    tiny_file = 'validation_queue_m6_' + mon_name + '_db.json'
    validation_queue = TinyDB(tiny_file)
    print 'Making ', tiny_file
    #print 'Process traces for {} {}'.format(nasn, ASdict[nasn])
    print 'Monitors: {}'.format(','.join(mon_list))
    
    trace_count = 0
    trace_dict = {}  #dict indexed by far AS, of dicts indexed by far, of list of all  traces, named by mon
    
    for mon in mon_list:
        query_string = """show tag values from "sequence_data_1" with key = "fasn" where "mon" = '""" + mon + """'"""
        returns = metaclient.query(query_string)
        items = returns.items()
        count = 0
        fasn_set = set()
        try:
            for ta in items[0][1]:
                tasn = ta['value']            
                fasn_set.add(tasn)
                count += 1
            print 'Fasn count: {} {}'.format(mon,count)
        except:
            print 'Error: No fasn for monitor {}'.format(mon)
            #continue
            #ipdb.set_trace()
            continue
        fasn_list = list(fasn_set) #just this one mon here
        
        if debug:
            fasn_list = ['2828']
     
        for fasn in sorted(fasn_list):
            as_trace_count = 0
            ASname = get_ASname(fasn)
                
            if fasn in ASomit.keys():
                print 'Omitting AS {} {}'.format(fasn, ASomit[fasn]) 
                continue
            print '\nProcessing AS {}, {}'.format(fasn, ASname) 
            
            if fasn not in trace_dict.keys():
                trace_dict[fasn] = {}
            query_string = """show tag values from "sequence_data_1" with key = "far" where "mon" = '""" + mon + """' and "fasn" = '""" + fasn + """' AND "ind" = '1' """
            returns = metaclient.query(query_string)
            items = returns.items()
            count = 0
            far_set = set()
            try:
                for ta in items[0][1]:
                    tfar = ta['value']            
                    far_set.add(tfar)
                    count += 1
                print 'Far count: {}'.format(count)
            except:
                print 'Error: No far for monitor {} fasn {}'.format(mon, fasn)
                #continue
                ipdb.set_trace()
                return
            
            for far in far_set:
                if far not in trace_dict[fasn].keys():
                    trace_dict[fasn][far] = set()
                trace_dict[fasn][far].add(mon)
                trace_count += 1
                as_trace_count += 1
                
                
            print 'Total count {}'.format(as_trace_count)
    #now process each far as, within which process each far in turn, looking at all mons. 
       
    done_count = 0   
    for fasn in trace_dict.keys():
    #for fasn in ['6453','2914','22773']:
        ASname = get_ASname(fasn)
        print 'Processing', ASname
        for far in trace_dict[fasn].keys():
            val_dict = {}
            val_start_day = []
            v_trace_list = []
            merge_data = {} #Not used in bulk mode, so empty
            for mon in trace_dict[fasn][far]:
                print 'Processed count: {} out of {}'.\
                format(done_count,trace_count)
                status, start_date = process_one_trace\
                (pasn, fasn, far,mon, client, metaclient, method,0, merge_data)            
                if status == 2: #found some congestion
                    val_start_day.append(start_date)
                    v_trace_list.append([far,mon])
                done_count += 1

            if len(v_trace_list) > 0: #for some trace, needed validation
                uid_seed += 1
                validation_queue.insert(dict(fasn = fasn,\
                                    nasn = pasn,\
                                    gid = -1,
                                    trace = v_trace_list,\
                                    day = min(val_start_day),
                                    uid = uid_seed))
                print 'Data to be queued' 
                print dict(fasn = fasn,\
                    trace = v_trace_list,\
                    day = min(val_start_day),
                    uid = uid_seed) 
            

def main():
    
    process_days('ddc-m7', test = False)

if __name__ == '__main__':
    main()