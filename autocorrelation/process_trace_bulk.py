'''This program contains the rutine process_one_trace, which does the work of processing a trace. It can be called by a variety of command line or scripted modules, to do processing by source AS, dest AS, and so on. 
It produces the measurement 'day_data_ddcm7', where the processed results for each day of the trace are stored. 
The measurement day_data_assertion_ddcm4 contains the current best validation of the data for each day, either from this program or from human validation.'''
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
from tabulate import tabulate
from process_functions import make_filter, compute_filtered_congestion, identify_step, close, findlimits, compute_near_congestion, compute_far_congestion, compute_q
from tinydb import TinyDB, Query
import ipdb
import random

       
def process_one_trace(nasn, fasn, far,mon, client, metaclient, method, starting_day = 0, merge_data = {}, merge_type = 0):
    test = False
    debug = False
    val_start_day = [] #applies to this trace only. 
    
    
    evaluate = False
    if evaluate:
        d_eval = open('d_eval.txt', 'a')
        f_eval = open('f_eval.txt', 'a')
    
    
    query_string = """SELECT  * FROM "day_data_assertion_ddcm4"  WHERE  "far" = '""" + far + """' AND "mon" = '""" + mon + """' and "asn" = '""" + fasn + """'  GROUP BY "day"   """
            #print query_string
    asrt_items = metaclient.query(query_string)
    akeys = asrt_items.keys()
    a_day_dict = {}
    vdays = {}
    v_days_found = []
    if len(akeys) > 0: #already did some days
        for k in akeys:
            day = int(k[1]['day'])
            v_days_found.append(day)
            a_day_dict[day] = k
            old_version = 0
            g = asrt_items[k]    
            for i in g:
                trial_version = int(i['version'])
                if trial_version > old_version:
                    old_version = trial_version
                    trial_dict = i
            vdays[day] = trial_dict 
            
#This bit of gnarly code above unpacks the results of a query to InfluxDB.     

    query_string = """SELECT  * FROM "day_data_ddcm7"  WHERE  "far" = '""" + far + """' AND "mon" = '""" + mon + """' and "asn" = '""" + fasn + """'  GROUP BY *   """
    day_items = metaclient.query(query_string)
    dkeys = day_items.keys()
    days = {}
    d_days_found = []
    if len(dkeys) > 0: #already processed some days, get old stuff
        for k in dkeys:
            ddict = {}
            ddict.update(k[1])
            ddict.update(day_items[k].next())
            day = int(ddict['day'])
            d_days_found.append(day)
            days[day] = ddict

    if starting_day == 0:# will be, except in catchup mode
        if len(d_days_found) > 0:
            starting_day = max(d_days_found) + 1
            first_50 = False
        else: #get starting day from Influx
            earliest_time_result = client.query("""SELECT  "ind", "mon", "asn", "link", "rtt" FROM "tsplnk"  WHERE  "target" = '""" + far + """' AND "mon" = '""" + mon + """' and "asn" = '""" + fasn + """' GROUP BY "ind", "mon", "asn", "link" ORDER BY time ASC LIMIT 1 """)
            first_50 = True 
            if len(earliest_time_result) == 0:
                print 'Error--no data for  {}, {} {}'.format( far, fasn, mon)
                return 0,0 #on to next mon 
            g=earliest_time_result.items()[0][1]
            for x in g:
                first_time_extracted = convertrfc3339(x['time'])
            
            starting_day = get_day(first_time_extracted) + 1 + 3 
                #start 3 days after the first full day--part of correlation
    
    query_string = """SELECT "rtt", "mon" FROM "tsplnk"  WHERE  "target" = '"""+ far + """' AND "ind" = '1' AND "mon" = '"""+ mon + """'AND "asn" = '"""+ fasn + """' ORDER BY time DESC LIMIT 1 """
    latest_time_result = client.query(query_string)
    if len(latest_time_result) == 0:
        print 'Oddity--no data for  {}, {} {}'.format( far, fasn, mon)
        return  0, 0
    g=latest_time_result.items()[0][1]
    for x in g:
        Latest_time_extracted = convertrfc3339(x['time'])
        ending_day = get_day(Latest_time_extracted) - 1
    print '\nTrace {} {}  {}, day {} to {}'.format(far, fasn,\
          mon, starting_day, ending_day)
    if debug:
        ending_day = min(ending_day, starting_day + 300)
            
    #Now we have first unprocessed day, and latest day. Process them.

    next_day_to_process = day = starting_day
    day_count = ending_day - starting_day
    if starting_day > ending_day:
        return 0,0
    if ending_day - day < 50:
        day = ending_day - 50 #look at a full 50 days if possible
    #Code wants to look at a multi-day chunk, so look
    #backwards if there are not enough unprocessed days to make a
    #chunk.
        
    found_congestion = 0
    validation_done = False
    validation_start_date = 0
    total_good_days = 0
    
    #When did a person last look at assertions?
    vd = vdays.keys()
    vd.sort(reverse = True)
    last_hv = 0
    starting_eight = 0
    unval_eight_count = 0
    for v in vd: #look backwards through the validated days
        if int(vdays[v]['asrt']) == 8:
            starting_eight = v  #Counting backwards, so will be the 
            #first day asserted as an 8. It may have status == 2.
            unval_eight_count += 1
        if int(vdays[v]['status']) == 3:
            last_hv = v
            break
    print 'Last day for human validation: {}, val count {}'.format(last_hv, len(vd))
    
    while day <= ending_day:
        if ending_day - day > 60:
            print 'Do 50 days'
            temp_ending_day = day + 49
        else:
            temp_ending_day = ending_day
        #day50_count += 1

        #table = []
        #table.append(heading)
        #now = time.time()
        #latest_week = get_week(now)
        start_time = get_day_lb(day-3)
        start_time_str = str(start_time)+'s'
        end_time = get_day_lb(temp_ending_day + 1 + 3) - 1
        end_time_str = str(end_time) + 's' 
            #ending time is the last second of the day, which is the 
            #first second of the next day - 1 
            #go three days beyond


        #print 'Plotting {},{},{}'.format(mon,fasn,week)
        #Finally, we fetch some real data. 
        far_rtts = []
        near_rtts = []
        times = []
        print 'Fetch far trace'    
        query_string = """SELECT min("rtt") FROM "tsplnk"  WHERE  "target" = '"""+ far + """' AND "ind" = '1' AND "mon" = '""" + mon +"""'AND "asn" = '"""+ fasn + """'AND time > """ + start_time_str + """ AND time < """ + end_time_str + """ GROUP BY time(15m) """
        trace = client.query(query_string) 
        try:    
            g=trace.items()[0][1]
        except:
            print 'Error--no rtts in trace'
            day = day + 50  
            continue   
        for x in g:

            times.append(convertrfc3339(x['time']))
            far_rtts.append(x['min'])

        print 'Fetch near trace'   
        far_escape = '\.'.join(far.split('.'))
        link_query = '/\:'+far_escape+'$/' #cannot use target--find all links with this far side and ind = '0'
        query_string = """SELECT min("rtt") FROM "tsplnk"  WHERE  "link" =~ """+ link_query + """ AND "ind" = '0' AND "mon" = '""" + mon +"""'AND "asn" = '"""+ fasn + """'AND time > """ + start_time_str + """ AND time < """ + end_time_str + """ GROUP BY time(15m) """
        trace = client.query(query_string)
        try:    
            g=trace.items()[0][1]
        except:
            print 'Error--no rtts in trace'
            day = day + 50  
            continue   
        for x in g:
            near_rtts.append(x['min'])

        good_days = 0
        days = {}
        if first_50:
            day_to_start = day -3
        else:
            day_to_start = day
            
        for tday in range(day_to_start, temp_ending_day + 1):
            day_index = (tday - day  + 3) * 96
            if tday not in days.keys():
                days[tday] = {}
                days[tday]['revised-asrt'] = 0
                days[tday]['rule'] = 0

            days[tday]['ngood_count'], \
            days[tday]['nl_bound'], \
            days[tday]['nu_bound'], \
            days[tday]['ncutoff'] = \
            findlimits(near_rtts[\
            day_index:day_index + 96],\
            near_rtts[day_index:day_index + 96],96,\
              'near')
            # if days[tday]['ngood_count'] == 0:
#                 print 'No data, near, day = ', tday,\
#                  (tday - day  + 3)

            ncutoff = max(days[tday]['ncutoff'],\
                         days[tday]['nl_bound'] + 10)
        

            days[tday]['fgood_count'], \
            days[tday]['fl_bound'], \
            days[tday]['fu_bound'], \
            days[tday]['fcutoff'] = \
            findlimits(far_rtts[\
            day_index:day_index + 96],\
             near_rtts[day_index:day_index + 96],96,\
             'far', ncutoff)
            # if days[tday]['ngood_count'] == 0:
#                 print 'No data, far, day = ', tday,\
#                 (tday - day  + 3)

            # if days[tday]['fgood_count'] > 30:
    #                                     total_good_days += 1                        
            if days[tday]['fgood_count'] > 30 and\
                days[tday]['ngood_count'] > 30:
                days[tday]['pearson_1'] = \
                compute_pearson(far_rtts[\
                day_index:day_index + 96],\
                near_rtts[day_index:day_index + 96])
                good_days += 1
            else:
                days[tday]['revised-asrt'] = 2
                days[tday]['rule'] = 2
                days[tday]['pearson_1'] = 0.0
                
        identify_step(days, day - 3, far_rtts)
    
        f_start,f_end, ef_start, ef_end, of_start, of_end, d_count, d_set,t2p,peak_options, \
        length_list, fifty_centrality, filter_status,efilter_status, ofilter_status, j_days =\
         make_filter(far_rtts,\
         near_rtts, \
        day - 3, days ) 
        print 'Filter range: {}, {} status {}'.\
        format(f_start,f_end,filter_status)
        print 'Overall centrality', fifty_centrality
        #print t2p
        print 'Max days in slot:', max(d_count)
        print peak_options 
        if f_end < f_start:
            temp_end = f_end + 96
        else:
            temp_end = f_end
        filter_count = temp_end - f_start
        if len(length_list) > 0:
            print filter_count, max(length_list),\
              max(d_count), length_list  
        elevated_days = 0
        s_days = 0
        eight_days = 0
        c_list = []
        for tday in range(day, temp_ending_day + 1):
            day_index = (tday - day  + 3) * 96
            days[tday]['fcongestion'] = 0.0
            days[tday]['ficongestion'] = 0.0
            days[tday]['nicongestion'] = 0.0
            days[tday]['ntcongestion'] = 0.0
            
            if days[tday]['revised-asrt'] <= 2 and\
              days[tday]['revised-asrt'] > 0:
                continue 
            if tday not in days.keys():
                print '{} missing from days',format(tday)
                continue #program will blow up later...
            if days[tday]['ftcongestion'] == 0:
                if days[tday]['pearson_1'] > .4:
                    days[tday]['revised-asrt'] = 4
                    days[tday]['rule'] = 3
                    #odd situation, but can occur.
                else:
                    days[tday]['revised-asrt'] = 3 
                    days[tday]['rule'] = 1
                continue
            if (days[tday]['fu_bound'] - \
                days[tday]['fl_bound'] < 7):
                if days[tday]['pearson_1'] > .4:
                    days[tday]['revised-asrt'] = 4
                    days[tday]['rule'] = 11
                    #odd situation, but can occur.
                else:
                    days[tday]['revised-asrt'] = 3 
                    days[tday]['rule'] = 11
                continue
                
            if filter_status < 2:
                days[tday]['revised-asrt'] = 5 
                days[tday]['rule'] = 8
                continue
            if fifty_centrality < .3:
                days[tday]['revised-asrt'] = 5 
                days[tday]['rule'] = 62
                continue
       
        
            days[tday]['fcongestion'] =\
              compute_filtered_congestion(\
               f_start,f_end,tday,d_set,96)
            if days[tday]['fcongestion'] > 0.0:
                c_list.append(days[tday]['fcongestion'])
           
            days[tday]['ntcongestion'],\
            days[tday]['nicongestion']=\
              compute_near_congestion(\
               near_rtts[day_index:day_index + 96],\
               ncutoff, f_start,f_end)
           
            #Only use this for plotting
            ftc, days[tday]['ficongestion'] = \
            compute_far_congestion\
            (far_rtts[day_index:day_index + 96],\
            near_rtts[day_index:day_index + 96],\
            days[tday]['fcutoff'],\
            days[tday]['nl_bound'],\
            days[tday]['fl_bound'], f_start, f_end)

           
            if days[tday]['nicongestion'] >= \
                .4 * days[tday]['ficongestion']  and\
                days[tday]['nu_bound'] - \
                days[tday]['nl_bound'] > 7:
                days[tday]['revised-asrt'] = 4
                days[tday]['rule'] = 3
                #Removed Pearson test.
            
            elif days[tday]['fcongestion'] > 0.0:
                days[tday]['revised-asrt'] = 8
                if days[tday]['pearson_1'] > .4:
                    days[tday]['rule'] = 22
                else:
                    days[tday]['rule'] = 31
                eight_days += 1
                elevated_days += 1 
            
            elif days[tday]['fcongestion'] == 0.0:
                days[tday]['revised-asrt'] = 5
                days[tday]['rule'] = 30
                elevated_days += 1 
            
            if days[tday]['revised-asrt'] == 0 or\
              days[tday]['rule'] == 0:
                ipdb.set_trace()
        #Compute some features of the 50 day
        q1,q2 = compute_q(d_count, eight_days,\
            f_start, f_end)
        
        print 'Q1 {}, Q2 {}'.format(q1,q2)
        print 'Peak count', len(peak_options) 
    
        if len(c_list) >= 3:
            d_mean = st.mean(c_list)
            d_sd = st.stdev(c_list)
    
            print 'Mean {}, SD {}'.\
            format(d_mean, d_sd)
        else:
            print 'No SD computed'
            d_mean = 0.0
            d_sd = 0.0       
        q3 = d_mean*100/filter_count
        
        influx_insert = []
        assertion_insert = []
        table = [[' '],['vasrt'], ['vstat'],['asrt'],['rule'],\
            ['C'],['cen'],['offset'], ['P']] 
        summary = ['1','2', '3','4','5','8','New', 'Not'],\
            [0,0,0,0,0,0,0,0]
        new_8 = 0
        new_not_8 = 0

        for tday in range(day, temp_ending_day + 1):
            #Rejection rules
            if eight_days < 3 and\
                days[tday]['revised-asrt'] == 8:
                days[tday]['revised-asrt'] = 5
                days[tday]['rule'] = 61
        
            if good_days < 5 and \
               days[tday]['revised-asrt'] == 8:
                days[tday]['revised-asrt'] = 5
                days[tday]['rule'] = 50
            
            if elevated_days > eight_days * 2 and\
             days[tday]['revised-asrt'] == 8:
                days[tday]['revised-asrt'] = 5
                days[tday]['rule'] = 66
                
            

            #fill in some numbers
        
            days[tday]['overall-centrality'] =\
             fifty_centrality
            days[tday]['good_days'] = good_days
            days[tday]['corr_days'] = eight_days
            days[tday]['congested_days'] = elevated_days
            days[tday]['num_c_events'] = len(peak_options)
            days[tday]['filter_status'] = filter_status
            days[tday]['efilter_status'] = efilter_status
            days[tday]['ofilter_status'] = ofilter_status
            days[tday]['f-start'] = f_start
            days[tday]['f-end'] = f_end
            days[tday]['ef-start'] = ef_start
            days[tday]['ef-end'] = ef_end
            days[tday]['of-start'] = of_start
            days[tday]['of-end'] = of_end
            days[tday]['q1'] = q1
            days[tday]['q2'] = q2
            days[tday]['q3'] = q3
            days[tday]['d-mean'] = d_mean
            days[tday]['d-sd'] = d_sd
            days[tday]['j-days'] = j_days
            days[tday]['uncertain-days'] = 0
            days[tday]['first-uncertain'] = 0
            days[tday]['first_day'] = day
            
            
            
           
                         
            if debug:
                day_measurement = 'day_data_ddcm7_test3'
            else:
                day_measurement =  "day_data_ddcm7"
                                  
            if tday >= next_day_to_process:
                time_str = time.strftime('%Y-%m-%dT00:00:00Z',\
                time.gmtime(get_day_lb(tday)))
                
                influx_insert.append({"measurement": day_measurement,   
                "tags": 
                    {
                    "far": far,
                    "mon": mon,
                    "asn": fasn,
                    "day": str(tday),
                    "method": method
                    },
                    "time": time_str,
                    "fields": days[tday]

                    })
                #In catchup mode, this will overwrite what we had before

                #Now decide whether/how to make assertion
                if tday in days.keys():  #Should always be true...
                    asrt = int(days[tday]['revised-asrt'])
                    rule = int(days[tday]['rule'])
                    congestion =\
                     float(days[tday]['fcongestion'])
                else:
                    asrt = 2
                    rule = 21
                    congestion = 0.0
                #The day may have already been validated by human.
                #As I tune this algorithm, I use human validation of
                #prior traces to check how well it is working. This
                #is the human version of using training data. 
                #Fill in the merge data for later merging.
                
                if tday not in merge_data.keys():
                    merge_data[tday] = []
                merge_data[tday].append([asrt,rule,congestion, mon, far,\
                    merge_type, int(days[tday]['fgood_count'])])
                #Always do this. Currently only used in catchup mode.
                     
                if tday in vdays.keys(): 
                    vstatus =  int(vdays[tday]['status'])
                    vversion = int(vdays[tday]['version'])
                    vasrt = int(vdays[tday]['asrt'])
                else:
                    vstatus = 0
                    vversion = 0
                    vasrt = 0
                    #Even if old validation, use new congestion
                    
                if asrt == 1:
                    summary[1][0] += 1
                elif asrt == 2:
                    summary[1][1] += 1
                elif asrt == 3:
                    summary[1][2] += 1
                elif asrt == 4:
                    summary[1][3] += 1
                elif asrt == 5:
                    summary[1][4] += 1
                elif asrt == 8:
                    summary[1][5] += 1

                     
                if vasrt != asrt:              
                    table[0].append(tday)
                    table[1].append(vasrt)
                    table[2].append(vstatus)
                    table[3].append(asrt)
                    table[4].append(rule)
                    table[5].append(congestion)
                    table[6].append(round(days[tday]['initial-centrality'],2))
                    table[7].append(days[tday]\
                        ['fcutoff'] - days[tday]['fl_bound']) 
                    table[8].append(round(days[tday]['pearson_1'],2))
                    
                    if asrt == 8:
                        new_8 += 1
                        summary[1][6] += 1
                    if vasrt == 8:
                        summary[1][7] += 1
                        
                if evaluate:
                    d_eval.write(",".join(map(str, [\
                    far,mon,fasn,tday,\
                    vasrt,vstatus,asrt, rule, congestion,\
                    days[tday]['ftcongestion'],\
                    round(days[tday]['initial-centrality'],2),\
                    round(days[tday]['final-centrality'],2),\
                    days[tday]['fcutoff'] - days[tday]['fl_bound'],\
                    days[tday]['fu_bound'] - days[tday]['fl_bound'],\
                    days[tday]['pearson_1'],\
                    days[tday]['num-gaps'],days[tday]['f-start'],\
                    days[tday]['f-end'],\
                    days[tday]['ef-start'],\
                    days[tday]['ef-end'],\
                    days[tday]['of-start'],\
                    days[tday]['of-end'],\
                    good_days, elevated_days,eight_days,\
                    filter_status,efilter_status, ofilter_status, q1,q2,q3,\
                    d_mean,d_sd, j_days, len(peak_options),\
                    fifty_centrality, days[tday]['first_day']]
                    )) + '\n')
                
                if vstatus == 3:
                    if vasrt != 8:
                        continue
                    #overwrite congestion value.
                    #Otherwise, do not overwrite a human validation
                    
                if asrt <= 4:
                    asrt_status = 1
                    
                elif asrt <= 7:
                    asrt_status = 1
                    if found_congestion == 0:
                        found_congestion = 1
                    
                elif asrt == 8: #fix this code later to accept congestion
                    if vstatus == 3:
                        asrt_status = 3
                    else: 
                        asrt_status = 2
                    #if vasrt != 8:
                    if asrt_status == 2: 
                    #if we get here and vasrt != 8, status is <3
                    #So no human validation. 
                        found_congestion = 2
                        if starting_eight == 0:
                            starting_eight = tday
                else:
                    print 'Invalid assertion', asrt
                    ipdb.set_trace()
                    
                if  asrt != vasrt or\
                    asrt == 8: #have we changed our mind?
                    version =  vversion + 1
                    assertion_insert.append(make_day_assertion(\
                    'day_data_assertion_ddcm4',\
                    far, mon,fasn, tday, method,\
                    asrt,\
                    rule,\
                    version,\
                    asrt_status,\
                    congestion
                    ))
                    #print assertion_insert[-1]
        
        if evaluate:
            f_eval.write(",".join(map(str, [\
            far,mon,fasn,day,\
            good_days, elevated_days,eight_days,\
            f_start, f_end, ef_start, ef_end, of_start, of_end,\
            filter_count,\
            filter_status,q1,q2, q3, d_mean,d_sd, j_days,\
            max(length_list) if len(length_list) > 0 else 0,\
            max(d_count) if len(d_count) > 0 else 0,\
            len(peak_options),\
            fifty_centrality]
            )) + '\n')
            
            
        
        print 'Val string', far, fasn, mon, day
        print 'New 8 {}, new not 8 {}'.format(new_8,new_not_8)
        print tabulate(summary, tablefmt = "grid")        
        print tabulate(table,tablefmt="grid")
        success = 48
        success = metaclient.write_points(influx_insert)
        print 'Write days: Success {}, day {}, count {}'.format(success,day,\
            len(influx_insert))
        if len(assertion_insert) > 0:
            success = 24
            success = metaclient.write_points(assertion_insert)
            print 'Write assertions: Success {}, day {},count {}, congestion {}'.format(success, day,\
                 len(assertion_insert), found_congestion)
        
        #Now decide whether we need human validation
        if validation_done == False:
            if found_congestion == 2:
                validation_start_date = starting_eight
                validation_done = True 
                print 'Pending validation queue, day = {}, start_day {}, mon {} far {}'.format(day, validation_start_date, mon, far) 
            
        day = tday + 1
        continue #with next 50 day block.
        
    if evaluate:
        d_eval.close()
        f_eval.close()
    return found_congestion, validation_start_date    
        
    
      

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
        "congestion": congestion,
}
}

    
def compute_pearson(frtts, nrtts):
    fmean = mean(frtts)
    nmean = mean(nrtts)
    cosum = nsumsq = fsumsq = 0
    for index in range(len(frtts)):
        if (nrtts[index] == None) or (frtts[index] == None):
            continue 
        cosum += (nrtts[index] - nmean) * (frtts[index] - fmean)
        nsumsq += (nrtts[index] - nmean) ** 2
        fsumsq += (frtts[index] - fmean) ** 2
    try:
        P_co = cosum/((nsumsq ** .5) * (fsumsq ** .5))
    except:  # in corner case sqrt of one value is less than one 
        P_co = 0.0
    return P_co
                    
def mean(n):
    sum_val = 0
    count = 0
    for num in range(len(n)):
        if n[num] == None:
            continue
        sum_val += n[num]
        count +=1
    #print 'Samples: total {}, good {}'.format(len(n),count)
    if count > 0:
        return float(sum_val/count) 
    else:
        return 0.0        
        
def ts_trace(rtts, test = False):
#NOT CURRENTLY USED.     
    smooth_rtts = []

    max = len(rtts)
    for i in range(max):
        if rtts[i] == None:
            smooth_rtts.append(None)
        else: 
            smooth = rtts[i]*.4
            if i-1 >= 0 and rtts[i-1] != None:
                smooth = smooth + rtts[i-1]*.2
                if i-2 >= 0 and rtts[i-2] != None:
                    smooth = smooth + rtts[i-2]*.1
                else:
                    smooth = smooth + rtts[i-1]*.1    
            else:
                smooth = smooth + rtts[i]*.2
                if i-2 >= 0 and rtts[i-2] != None:
                    smooth = smooth + rtts[i-2]*.1
                else:
                    smooth = smooth + rtts[i]*.1
            if i+1 < max and rtts[i+1] != None:
                smooth = smooth + rtts[i+1]*.2
                if i+2 < max and rtts[i+2] != None:
                    smooth = smooth + rtts[i+2]*.1
                else:
                    smooth = smooth + rtts[i+1]*.1    
            else:
                smooth = smooth + rtts[i]*.2
                if i+2 < max and rtts[i+2] != None:
                    smooth = smooth + rtts[i+2]*.1
                else:
                    smooth = smooth + rtts[i]*.1 
            smooth_rtts.append(smooth)
    return smooth_rtts
            
    
    
    
        
def main():
    
    process_days('ddc-m7', test = False)

if __name__ == '__main__':
    main()