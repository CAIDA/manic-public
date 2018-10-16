#Queries day assertions for the monitor/asn/ip specified
#savs returned results to file
#usage python querying_ddc_assertions.py <mon> <asn> <far_ip>

from influxdb import InfluxDBClient
#import ipdb
import time
import sys
import numpy as np

output_path = '/project/comcast-ping/kabir-plots/loss_data/supplemental_data/ddc/'

def make_assertions(far, mon, asn):
    
    
    client = InfluxDBClient('', , '', '', 'tspmult', ssl = True, verify_ssl=True)
    metaclient = InfluxDBClient('', , '', '', 'tspmult', ssl = True, verify_ssl=True)
    
    #insert suitable code to find far, mon, asn, where far is the IP on the far side of the link. 

#You can query for a single day, but that is inefficient. For a given trace, I pull all the days at once. As follows:

#To get the assertions for the valid days for the trace.    
    query_string = """SELECT  * FROM "day_data_assertion_ddcm4"  WHERE  "far" = '""" + far + """' AND "mon" = '""" + mon + """' and "asn" = '""" + asn + """'  GROUP BY "day"   """ #THIS IS THE KEY-VALUE-PAIR THAT INDEXES THE DB
    #print query_string
    try:
        asrt_items = metaclient.query(query_string)
    except:
        asrt_items = ''
        while len(asrt_items) == 0:
            time.sleep(0.01)
            asrt_items = metaclient.query(query_string)
    akeys = asrt_items.keys()
    a_day_dict = {}
    vdays = {}
    if len(akeys) > 0: 
        for k in akeys: # DAY IS THE KEY BECAUSE OF THE QUERY STRUCTURE
            day = int(k[1]['day']) #K[1] IS THE DICT, THEN ['day'] gives you the day
            a_day_dict[day] = k
            old_version = 0
            g = asrt_items[k]    
            for i in g:
                trial_version = int(i['version']) #FIND THE HIGHEST VERSION OF THE ASSERTION 
                if trial_version > old_version:
                    old_version = trial_version
                    trial_dict = i
            vdays[day] = trial_dict  #FIND CONTENT OF THAT RECORD
        

#When I make a better assertion (e.g., by human inspection, I do not overwrite
#the previous one. So that bit of code above finds the latest version of the assertion.   


#To get the day data for the falid days for the trace. 
    query_string = """SELECT  * FROM "day_data_ddcm4_3"  WHERE  "far" = '""" + far + """' AND "mon" = '""" + mon + """' and "asn" = '""" + asn + """'  GROUP BY *   """
            #print query_string
    day_items = metaclient.query(query_string)
    dkeys = day_items.keys()
    days = {}
    d_days_found = []
    if len(dkeys) > 0: 
        for k in dkeys:
            ddict = {}
            ddict.update(k[1])
            ddict.update(day_items[k].next())
            day = int(ddict['day'])
            d_days_found.append(day)
            days[day] = ddict

    return vdays, days       
#vdays has the assertions, days has the days with data
#How I convert unix time in seconds to my day index:

def get_day(time):
    return(int(time)//86400)         
    
def get_day_lb(day):
    return day*86400 
    
def get_day_ub(day):
    return (day+1)*86400 - 900
    
#this is the code to make an assertion. You can see what the tags and fields are
def make_day_assertion(measurement,far,mon,asn,day,method, assertion,rule, version,status, congestion):
    time_str = time.strftime('%Y-%m-%dT00:00:00Z',\
     time.localtime(get_day_lb(day)))
    return {"measurement": measurement,   
"tags": 
        {
        "far": far,
        "mon": mon,
        "asn": asn,
        "day": str(day),
        "method": method,
        "asrt": assertion,
        "rule": rule,
        "version": version,
        "status": status
        
},
"time": time_str,
"fields": 
        {
        "congestion": congestion
}
}

def read_arguments():

    far = str(sys.argv[3])
    mon = str(sys.argv[1])
    asn = str(sys.argv[2])
    first_day = str(sys.argv[4]) 
    last_day = str(sys.argv[5]) 
    code = str(sys.argv[6])
    return mon, asn, far, first_day, last_day, code

def write_assertions(mon, far, asn, assertions, days, code):

#    output_file = output_path + mon + '_dec.csv'

#    with open(output_file, 'a+') as f:
#        write_string = ''
    avg_congestion = 0.0
    congestion_values = []
    congested_days = 0
    uncongested_days = 0
    human_time = str(time.strftime('%Y-%m-%dT00:00:00Z', time.localtime(get_day_lb(days[1]))))
    for day in days: #THIS SHOULD BE FOR day in vdays (the output already has the key in it)
        write_string = ''
        try:
            assertion = str(assertions[day]['asrt'])
            if assertion == '3':
                uncongested_days = uncongested_days + 1
            elif assertion == '8':
                day_congestion = float(assertions[day]['congestion'])
                #print day_congestion
                if day_congestion >= 0.04:
                    congested_days = congested_days + 1
                    congestion_values.append(day_congestion)
                elif day_congestion < 0.04:
                    uncongested_days = uncongested_days + 1
        except KeyError:
            continue
            day_str = str(day)
            #assertion = 'none'
                #day_utc_seconds = 'none'
                #congestion_estimate = 'none'
                #assertion_status = 'none'
                #human_time = 'none'
                #write_string = mon + ',' + asn + ',' + far + ',' + day_str + ',' + day_utc_seconds + ',' + assertion \
                #        + ',' + congestion_estimate + ',' + assertion_status + ',' + human_time + '\n'
    f = open('/project/comcast-ping/kabir-plots/loss_data/camreadyrean/bottomnewtestlosssig.csv', 'a')
    write_string = ''

    if congested_days > 0:
        mean = np.mean(congestion_values)
    else:
        mean = '0.0'
    if congested_days > 0:
        write_string = mon + ',' + asn + ',' + far + ',' + human_time + ',' + str(congested_days) + ',' + str(uncongested_days) + ',' + str(mean) + ',' + code + '\n'
        f.write(write_string)
    elif uncongested_days > 0:
        write_string = mon + ',' + asn + ',' + far + ',' + human_time + ',' + str(congested_days) + ',' + str(uncongested_days) + ',' + str(mean) + ',' + code + '\n'
        f.write(write_string)
    elif congested_days == 0 and uncongested_days == 0:
        write_string = mon + ',' + asn + ',' + far + ',' + human_time + ',' + str(congested_days) + ',' + str(uncongested_days) + ',' + str(mean) + ',' + code + '\n'
        f.write(write_string)

    #f.write(write_string)

def create_days(first_day, last_day):
    days = []
    ind = int(first_day) #here is where you hardcode the days for each month
    while ind <= int(last_day):
        days.append(ind)
        ind = ind + 1
    return days

def main():
    
    assertions = []
    
    mon, asn, far, first_day, last_day, code = read_arguments()
    
    assertions, days = make_assertions(far, mon, asn)
    days = []
    
    days = create_days(first_day, last_day) #hacky thing to hardcode days
    #day = 17236

    write_assertions(mon, far, asn, assertions, days, code)

main()
'''   
   
====
Codes:
    1: error or oddity
    2: insufficient samples
    3: no congestion
    4: near side congestion 
    5: uncorrelated congestion
    6: uncorrelated congestion
    8: correlated congestion
    9: correlation based local correlation
    
Status of an assertion:
    0: not done
    1: Algorithm assertion--assumed valid
    2: Algorithm assertion--needs validation
    3: Validated by visual inspection
    
'''
