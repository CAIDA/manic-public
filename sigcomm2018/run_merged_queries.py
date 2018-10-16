from __future__ import with_statement
#usage python run_merged_queries.py >> supplemental_data/merged_queries_log.txt 2>&1
#gets list of ipmap files to process from hardcoded list below
#list created by running:
#ls rtt_and_loss_data/rtt/book_keeping/*/*/*ipmap* > supplemental_data/list_of_ipmaps.txt
import subprocess
from multiprocessing import Pool
import os
import signal
import time
import csv
import sys

command = "python querying_merged_assertions.py "
log = " >> "
log_post = " 2>&1 "
timeout_seconds = 1800

ipmap_file = "/project/comcast-ping/kabir-plots/loss_data/supplemental_data/list_of_ipmaps.txt"
#one hour

monitor_set = set(['atl2-us', \
'wbu2-us', \
'pao-us', \
'mry-us', \
'gai-us', \
'bos5-us', \
'bed-us', \
'bna-us', \
'sts-us', \
'sea2-us', \
'lke-us', \
'iad2-us', \
'cld2-us', \
'tul2-us', \
'oak3-us', \
'oak5-us', \
'san7-us', \
'san6-us', \
'atl3-us', \
'msn3-us', \
'lwc2-us', \
'san4-us', \
'lex-us', \
'ith-us', 'lex-us',   'igx-us',   'dal-us',   'avl-us',   'msn2-us',   'rno2-us',   'bed2-us',   'mnz-us',   'bos2-us',   'dca2-us',   'bos6-us',   'dca3-us',   'msy-us',   'tul-us',   'san2-us',   'tul3-us',   'bed3-us',   'aza-us',   'bfi-us',   'las-us'])

monitor_dict = {'atl2-us':7922, \
'wbu2-us':7922, \
'pao-us':7922, \
'mry-us':7922, \
'gai-us':7922, \
'bos5-us':7922, \
'bed-us':7922, \
'bna-us':7922, \
'sts-us':7922, \
'sea2-us':7922, \
'lke-us':7922, \
'iad2-us':7922, \
'cld2-us':7018, \
'tul2-us':7018, \
'oak3-us':7018, \
'oak5-us':7018, \
'san7-us':7018, \
'san6-us':7018, \
'atl3-us':7018, \
'msn3-us':7018, \
'lwc2-us':7018, \
'san4-us':7843, \
'lex-us':7843, \
'ith-us':7843, 'igx-us':5650,'dal-us':5650,'avl-us':20115,'msn2-us':20115,'rno2-us':20115,'bed2-us':701,'mnz-us':701,'bos2-us':701,'dca2-us':701,'bos6-us':701,'dca3-us':701,'msy-us':22773,'tul-us':22773,'san2-us':22773,'tul3-us':22773,'bed3-us':6079,'aza-us':209,'bfi-us':209,'las-us':209 }

def systemCall(parameter):
    os.system(parameter)

def create_commands(input_file, query_tuples_set):
#Create list of commands to pass to the multiprocessing manager
    #open grab monitor and time-period from IP map filename
    monitor = input_file.split('/')[-1].split('.')[0]
    
    if monitor not in monitor_set:
        return query_tuples_set #monitor hasn't been analyzed yet
    access_asn = monitor_dict[monitor]
    time_period_year = input_file.split('/')[-2].split('_')[-1]
    year_string = time_period_year + '_'
    year_string_replace = time_period_year + '.'
    time_period = input_file.split('/')[-2].replace(year_string, year_string_replace)

    rtt_path = input_file.replace('book_keeping','levelshift')
    loss_path = input_file.replace('/rtt/','/loss/').replace('book_keeping/','')

    #remove filename portion

    #Then, grab from each line the ASN, near-end IP and far-end IP
    #Use that to build command string for final heuristics
    with open (input_file,'rb') as f:
        reader = csv.reader(f, delimiter=' ')
        for row in reader:
            asn = row[0]
            near_ip = row[1]
            far_ip = row[2]
            
            access_asn = str(monitor_dict[monitor])   
            #HARDCODED TO CHECK VERIZON GOOGLE THING
            if access_asn != '701' or asn != '15169':
                continue
            #END HARDCODED THING
            descriptor = access_asn + ' ' + asn + ' ' + far_ip
            query_tuples_set.add(descriptor)
    #print "querying following tuples: "
    #print query_tuples_set
    return query_tuples_set	

def read_ipmap(filename):
    #read ipmap lines into list
    ipmap_list = ''
    with open(filename, 'rb') as f: #import file
        ipmap_list = f.readlines()
    return ipmap_list

def list_lines(filename):
    try:
        with open(filename, 'rb') as f:
            return str(len(f.readlines()))
    except EnvironmentError: # parent of IOError, OSError *and* WindowsError where available
            #sys.stderr.write('could not open file ' + filename + '\n')
            return '-1'
       
def call_create_commands(ipmap_list):
    query_tuples_set = set()

    for i in range(len(ipmap_list)):
        passing_string = ipmap_list[i].strip('\n')
        query_tuples_set = create_commands(passing_string,query_tuples_set)
        
    for j in query_tuples_set:
        querying_command = command + j
        sys.stderr.write(querying_command + '\n')
        #print querying_command
        systemCall(querying_command)

    #return combined_list

def main():

    command_list = []

    #get list of ipmap files using below list
    ipmap_list = read_ipmap(ipmap_file)

    #create influx queries for each monitor-month tuple
    call_create_commands(ipmap_list)

    
    #run influx/loss commands 20 at a time
    #run_commands(command_list)
		
main()
