#Queries day assertions for the monitor/asn/ip specified
#savs returned results to file
#usage python querying_ddc_assertions.py <mon> <asn> <far_ip>

from influxdb import InfluxDBClient
#import ipdb
import time
import sys

output_path = '/project/comcast-ping/kabir-plots/loss_data/merged_data/'

def make_assertions(far, nasn, asn):
    metaclient = InfluxDBClient('', , '', '', '', ssl = True, verify_ssl=True)
    #insert suitable code to find far, mon, asn, where far is the IP on the far side of the link. 
#You can query for a single day, but that is inefficient. For a given trace, I pull all the days at once. As follows:
#To get the assertions for the valid days for the trace.    
    query_string = """SELECT  * FROM "merged_assertions_ddcm6_2" WHERE   "asn" = '""" + asn + """' AND "far" = '""" + far + """' and "nasn" = '""" + nasn + """'  GROUP BY  "day"  """
    #print query_string
    asrt_items = metaclient.query(query_string)
    #print asrt_items
    #print len(asrt_items)
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
    
    days = []
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
def make_day_assertion(measurement,far,nasn,asn,day,method, assertion,rule, version,status, congestion):
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
    nasn = str(sys.argv[1])
    asn = str(sys.argv[2])

    return nasn, asn, far

def write_assertions(nasn, far, asn, assertions, days):

    output_file = output_path + nasn + '_allmonths.csv'

    with open(output_file, 'a+') as f:
        write_string = ''

        for day in days: #THIS SHOULD BE FOR day in vdays (the output already has the key in it)
            write_string = ''
            try:
                day_str = str(day)
                assertion = str(assertions[day]['asrt'])
                day_utc_seconds = str(get_day_lb(day))
                human_time = str(time.strftime('%Y-%m-%dT00:00:00Z', time.localtime(get_day_lb(day))))
                congestion_estimate = str(assertions[day]['congestion'])
                assertion_status = str(assertions[day]['status'])
                write_string = nasn + ',' + asn + ',' + far + ',' + day_str + ',' + day_utc_seconds + ',' + assertion \
                        + ',' + congestion_estimate + ',' + assertion_status + ',' + human_time + '\n'
                f.write(write_string)     
            except KeyError:
                day_str = str(day)
                assertion = 'none'
                day_utc_seconds = str(get_day_lb(day))
                human_time = str(time.strftime('%Y-%m-%dT00:00:00Z', time.localtime(get_day_lb(day))))
                congestion_estimate = 'none'
                assertion_status = 'none'
                write_string = nasn + ',' + asn + ',' + far + ',' + day_str + ',' + day_utc_seconds + ',' + assertion \
                        + ',' + congestion_estimate + ',' + assertion_status + ',' + human_time + '\n'
                f.write(write_string)

def create_days():
    days = []
    ind = 16861 #here is where you hardcode the days for each month
    while ind < 17532:
        days.append(ind)
        ind = ind + 1
    return days

def main():
    
    assertions = []
    
    nasn, asn, far = read_arguments()
    
    assertions, days = make_assertions(far, nasn, asn)
    days = []
    
    days = create_days() #hacky thing to hardcode days
    #day = 17236

    write_assertions(nasn, far, asn, assertions, days)

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
