#This code retrieves all the measurements from a given source AS, filters out the links that go to customers (since we don't currently process them), and makes the measurement called 'sequence_data_1', which is the measuremnt used by the process step.

# import modules used here 
import sys
import numpy as np
import matplotlib.pyplot as plt
from influxdb import InfluxDBClient
from tinydb import TinyDB, Query
import time
from weeks import *
import ipdb 


def main():

    
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
    
    ASo = open('AS-omit.txt', 'rU')
    ASomit = {}
    for lines in ASo:
        if len(lines) > 0 and lines[0] != '#':
            ASnumstr, ASoname = lines.strip('\n').split(',') #throw away the AS
            ASomit[ASnumstr] = ASoname
    ASo.close()
    

    
    AS_extras = open('AS-extra.txt', 'rU')
    ASextra = []
    for line in AS_extras:
        ASextra.append(line.split(',')[0])
    AS_extras.close()

    ASd = open('default-as.txt', 'rU')
    default_as = {}
    for lines in ASd:
        if len(lines) > 0 :
            mon,dasn = lines.strip('\n').split(',')
            default_as[mon] = dasn
    ASd.close()
    
    excluded_asns = ['1280', '4837', '4134', '4637', '5511', '2516', '4766',\
     '3215', '577', '3491', '680', '3320', '286', '12956', '9929',\
        '10026', '17676', '3786', '38861', '4589', '6762', '6830', '7474',\
         '812', '852', '6327', '5400']

    
    client = InfluxDBClient('beamer.caida.org', 8086, NAME, PASSWORD, 'tspmult', ssl = True, verify_ssl=True)
    metaclient = InfluxDBClient('beamer.caida.org', 8086,  NAME, PASSWORD, 'tspmeta', ssl = True, verify_ssl=True)

        # Warning--slightly gnarly code. The idea is I associate one near As with a monitor, but the user might not type that number (the other numbers are the siblings). I need that number for the sequence retrieval. 
    
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
        # Now we have the right AS number--get the monitor list
        xmons = client.query("""SELECT * FROM "names" WHERE "primary_asn" = '""" + pasn + """'""")
        mon_set = set()
        m = xmons.items()[0][1]
        for t in m:
            mon_set.add(t['mon'])
            #nAS_name = t['network']
            
           
    omon_list = list(mon_set)
        
    print  'Near mon set: ', ', '.join(omon_list) 
        
    #Which CAIDA ASrank to use?
    try:
        rank_method = int(sys.argv[2])
    except:
        rank_method = 1 
        
    if rank_method == 1:
        filestr = "as-rel1.txt"
    else:
        filestr = "as-rel2.txt"
        
    print 'Processing ', filestr
        
    AS_rel_file = open(filestr, 'rU')
    ASrel = {}
    
    for lines in AS_rel_file:
        if len(lines) > 0 and lines[0] != '#':
            #print lines
            if rank_method == 1:
                asone, astwo,relation = lines.strip("\n").split('|')
            else:
                asone, astwo,relation,source = lines.strip("\n").split('|')
            if relation == '0':
                ASrel[(asone, astwo)] = relation
                ASrel[(astwo, asone)] = relation
                #print 'Test', asone,astwo, ASrel[(asone,astwo)]
            elif relation == '-1':
                ASrel[(asone,astwo)] = '-1'
                ASrel[(astwo,asone)] = '1'
            elif relation == '1':
                ASrel[(asone, astwo)] = '1'
                ASrel[(astwo, asone)] = '-1'
                
                #print 'Reverse', astwo,asone, ASrel[(astwo,asone)]
    AS_rel_file.close()
    
    AS_set = set()
    AS_good_set = set()
    
    for mon in omon_list:
        print 'Fetching traces from {}'.format(mon)
        
        #Get the set of near side ASN for this monitor
        xasnl = client.query("""SELECT * FROM "names" WHERE "mon" = '""" +\
        mon + """'""")
        nasn_list = []
        m = xasnl.items()[0][1]
        for t in m:
            nasn_list.append(str(t['asn']))
        if len(nasn_list) == 0:
            print 'AS error: no AS entry for monitor {}'.format(mon)
            return
        #Get the far AS values for each monitor in the near AS.
        query_string = """show tag values from "tsplnk" with key = "asn" where "mon" = '""" + mon + """'"""
        
        as_returns = client.query(query_string)
        
        as_items = as_returns.items()
        as_count = 0
        try:
            for ta in as_items[0][1]:
                fasn = ta['value']
                #print fasn
                AS_set.add(fasn)
                as_count += 1
            print 'AS count: {}'.format(as_count)
        except:
            print 'Error: no AS data for mon ',mon
            continue
        
        
    cust_count = 0
    peer_count = 0
    transit_count = 0
    missing_count = 0
    extra_count = 0
    
    t_set = set() #transit
    p_set = set() #peer
    c_set = set() #customer
    o_set = set() #omit
    m_set = set() #missing--possible error
    e_set = set() #added from extras list--assume a peer.
    
    for fasn in AS_set:
        if fasn in ASdict.keys():
            ASname = ASdict[fasn]
        elif fasn in ASdictlong.keys():
            ASname = ASdictlong[fasn]
        else:
            ASname = 'Unknown'
        #print 'Far AS ', fasn
        if fasn in ASomit.keys():
            print 'Omitting AS {} {}'.format(fasn, ASomit[fasn])
            o_set.add(fasn)
            continue
        if fasn in excluded_asns:
            print 'Excluding AS {} {}'.format(fasn, ASname)
            o_set.add(fasn)
            continue
        success = False
        for nasn in nasn_list:
            #print '   ', nasn
            try:
                relation = ASrel[(nasn,fasn)]
                if relation == "-1": #customer
                    c_set.add(fasn)
                    success = True
                elif relation == '0':
                    p_set.add(fasn)
                    success = True
                    AS_good_set.add(fasn)
                elif relation == '1':
                    t_set.add(fasn)
                    success = True
                    AS_good_set.add(fasn)
                    #print 'Transit provider', fasn
            except:
                continue
                #This is not an error. The relation should exist for some nasn,
                #but not all of them. 
            
        if success == False:
            m_set.add(fasn)
            
        
        if fasn in ASextra:
            try:
                AS_name = ASdict[fasn]
            except:
                AS_name = 'Unknown'
                print '{} {} matched in extras list'.format(fasn,AS_name)
            AS_good_set.add(fasn)
            if fasn not in p_set:
                e_set.add(fasn)
                extra_count += 1
        
    fasn_list = []
    for fasn in (AS_good_set): #Does not include the customers. (See above)
        try:
            AS_name = ASdict[fasn]
        except:
            try:
                AS_name = ASdictlong[fasn]
            except:
                AS_name = 'Unknown'
        fasn_list.append([fasn,AS_name])
    fasn_list.sort(key = lambda x: x[1])
    print 'Count of peers/providers, extras {}'.format(len(fasn_list))
    for item in fasn_list:
        print '{},{}'.format(item[0],item[1])    
            
    print "C: {}, Pt: {}, Pe: {},T: {}, U: {}, E: {}".\
    format( len(c_set),len(p_set) +len(o_set),\
    len(p_set), len(t_set),len(m_set), len(e_set))
    
    #ipdb.set_trace()
    
    
    for fasn in AS_good_set:
        if fasn in ASdict.keys():
            ASname = ASdict[fasn]
        elif fasn in ASdictlong.keys():
            ASname = ASdictlong[fasn]
        else:
            ASname = 'Unknown'
        print 'Fetching for far AS', ASname
        
        for mon in omon_list:
            query_string = """show tag values from "tsplnk" with key = "link" where "asn" = '""" + fasn + """' and "mon" = '""" + mon + """' """
    
            as_returns = client.query(query_string)
    
            as_items = as_returns.items()
            as_count = 0
            sequences = []
            found_count = 0
            if len(as_items) > 0:
                for ta in as_items[0][1]:
                    link = ta['value']
                    #print fasn
                    near,far = link.split(':')
                    as_count += 1
                    query_string = """SELECT * FROM "sequence_data_1"  WHERE  "far" = '"""+ far + """'  AND "near" = '"""+ near + """'  AND "mon" = '"""+ mon + """' AND "fasn" = '"""+ fasn + """' AND "ind" = '1'"""

                    prior_sequence = metaclient.query(query_string)

                    if len(prior_sequence) == 0: #don't have this sequence yet
                        sequences.append(make_sequence_entry('sequence_data_1',\
                        far, near, mon, 1, rank_method,nasn, fasn, pasn))
                        print 'New', mon, fasn, near,far
                        #print sequences[-1] #for debugging
                    else:
                        found_count += 1
                        print 'Exists', mon, fasn, near,far

        #Warning: Earlier versions of this code fetched the ind from the actual
        #data. It is 0 for the near side and 1 for the far side. 
        #This value turns out to be useless. In this code, I just set the value
        #to 1, which is the value that subsequent programs search for.
        #Every now and then, there is a trace recorded for one side but not the 
        #other. This is some effor on brdmap. 
        #This version does not set a value for the tag "first_week", which is no 
        #longer used.
        

                if len(sequences) > 0:
                    success = metaclient.write_points(sequences) 
                    #success = 43 
                    print "Success {}, AS {}, count {}".format(success, fasn,\
                     len(sequences))
                if found_count > 0:
                    print '{} known traces found\n'.format(found_count)   



def make_sequence_entry(measurement,far,near, mon,ind,rank_method,nasn,fasn, pasn):
    time_str = time.strftime('%Y-%m-%dT00:00:00Z', time.localtime())
    return {"measurement": measurement,   
"tags": 
        {
        "far": far,
        "near": near,
        "mon": mon,
        "ind": ind,
        "rank_method": rank_method,
        "nasn": nasn,
        "fasn": fasn 
},
"time": time_str,
"fields": {
        "pasn": pasn}

}      

# Standard boilerplate to call the main() function to begin
# the program.
if __name__ == '__main__':
    main()