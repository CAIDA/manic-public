'''This code implements the human validation of trace assertions. It takes input from the tinydb queue, and writes validated assertions to the 'day_data_assertion_ddcm4' measurement. It calls validate_one_trace to do the work. '''
# import modules used here 
import sys
import os
import numpy as np
import matplotlib.pyplot as plt
from scipy import signal
from weeks import *
import time
import statistics as st
from process_functions import make_filter, compute_filtered_congestion, identify_step, close, findlimits, compute_near_congestion, compute_far_congestion
import math as m
from tabulate import tabulate
from influxdb import InfluxDBClient
from tinydb import TinyDB, Query
import ipdb


def validate_days(method):
    
    os.environ['TZ'] = 'UTC'
    time.tzset()
    #Force pyplot to use UTC time. There must be a better way...
    
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
    
    ASo = open('AS-omit.txt', 'rU')
    ASomit = {}
    for lines in ASo:
        if len(lines) > 0 and lines[0] != '#':
            ASnumstr, ASoname = lines.strip('\n').split(',') #throw away the AS
            ASomit[ASnumstr] = ASoname
    ASo.close()
    
    MON = open('ARK-data.txt', 'rU')
    MONdict = {}  #maps monitor name to AS number string
    AStoMONdict = {} #Maps AS number to the set of monitors in that AS
    AStoMONdict['0'] = set() #there are monitors not in the CAIDA list
    for lines in MON:
        if len(lines) > 0:
            monstr, asnr = lines.split(',')  #Assumes the file from CAIDA has been manually pre-processed. Write code to do that later.
            asn = asnr.strip('\n').strip()
            MONdict[monstr] = asn
            if asn not in AStoMONdict.keys():
                AStoMONdict[asn] = set()
            AStoMONdict[asn].add(monstr)           
            #print MONdict[monstr], lines
    MON.close()
    
    client = InfluxDBClient('beamer.caida.org', 8086, USER, PASSWORD, 'tspmult', ssl = True, verify_ssl=True)
    metaclient = InfluxDBClient('beamer.caida.org', 8086,  USER, PASSWORD, 'tspmeta', ssl = True, verify_ssl=True)
    
    inputstr = sys.argv[1]
    if inputstr.isdigit(): 
        mon_name = inputstr
    else:
        mon_name = inputstr[0:4] 
        if mon_name[3] == '-':
            mon_name = inputstr[0:3]      
    tiny_file = 'validation_queue_m6_' + mon_name + '_db.json'
    #tiny_file = 'validation_queue_m6_' + mon_name + '_db_new.json'
    validation_queue = TinyDB(tiny_file)
    print 'Using ', tiny_file
    pre = False
    try:
       text = sys.argv[2]
       if text == '1':
        pre = True
    except:
        pass
    print 'Preprocess: ', pre
    
    done = []
    old_far = '-'
    validation_query = Query()
    validations = validation_queue.all()
    print 'Validation count: {}'.format(len(validations))
    validations.sort(key = lambda x: x['trace'][0])
    processed_count = 0
    
    
    debug_list = []
    for ent in validations:
        item = []
        item.append(ent['fasn'])
        for m in ent['trace']:
            item.append(m[0])
        item.append(str(ent['day']))
        asn = ent['fasn']
        try:
            item.append(str(ent['uid']))
        except:
            item.append('0')
        if asn in ASdict.keys():
            ASname = ASdict[asn]
        elif asn in ASdictlong.keys():
            ASname = ASdictlong[asn]
        else:
            ASname = 'Unknown'
        item.append(ASname)
        debug_list.append(item)
    debug_list.sort(key = lambda i: i[0])
    for item in debug_list:
        print ' '.join(item)
    
    for ent in validations:
        delete_status = True
        p_dict = {}
        a_dict = {}
        merge_data = {}
        fasn = ent['fasn']
        nasn = ent['nasn']
        gid = ent['gid']
        trace_list = ent['trace']
        starting_day = ent['day']
        uid = ent['uid']
    
        if fasn in ASdict.keys():
            ASname = ASdict[fasn]
        elif fasn in ASdictlong.keys():
            ASname = ASdictlong[fasn]
        else:
            ASname = 'Unknown'
        
        if fasn in ASomit.keys():
            print 'Omitting AS {} {}, {}'.format(fasn, ASomit[fasn], trace_list)
            processed_count += 1
            validation_queue.remove((validation_query.nasn == nasn) &\
             (validation_query.uid == uid) & (validation_query.fasn == fasn) &\
              (validation_query.day == starting_day) )
            continue
        print '\n',  'Processing', fasn ,ASname, uid
        print '   Trace list', trace_list
        #sort by the total number of good daysl; take the mon with the most
        for item in trace_list:
            far = item[0]
            mon = item[1]
            print '   Processing trace ',far,mon
            if (far,fasn,mon) in done:
                print 'Duplicate found:', far, fasn,mon
                continue
            done.append((far,fasn,mon))
           
            delete_status, p_dict,a_dict = validate_one_trace(far, fasn,\
             ASname,\
             mon,starting_day,p_dict,a_dict, client, metaclient, method, merge_data)
            if delete_status == False:
                print 'Process count', processed_count
                return #User typed "q".
     
        if gid > -1:
            merge_days(merge_data,gid,pasn,fasn)
        
        print '{} Removing validation {} {} {}'.format(processed_count,\
         far,fasn,ent['trace'], uid)
        processed_count += 1
        validation_queue.remove((validation_query.nasn == nasn) &\
         (validation_query.uid == uid) & (validation_query.fasn == fasn) &\
          (validation_query.day == starting_day) )
          

    print 'Process count', processed_count
            
            
        
def validate_one_trace(far, asn, ASname,mon, starting_day, prior_p_dict,\
    prior_a_dict,client, metaclient, method, merge_data): 
        test = False
        new_assertion = True
        
        evaluate = True
        if evaluate:
            v_eval = open('v_eval.txt', 'a')
       
        query_string = """SELECT  * FROM "day_data_assertion_ddcm4"  WHERE  "far" = '""" + far + """' AND "mon" = '""" + mon + """' and "asn" = '""" + asn + """'  GROUP BY "day"   """
                #print query_string
        asrt_items = metaclient.query(query_string)
        akeys = asrt_items.keys()
        a_day_dict = {}
        vdays = {}
        if len(akeys) > 0: #already did some days
            for k in akeys:
                day = int(k[1]['day'])
                a_day_dict[day] = k
                old_version = 0
                g = asrt_items[k]    
                for i in g:
                    trial_version = int(i['version'])
                    if trial_version > old_version:
                        old_version = trial_version
                        trial_dict = i
                vdays[day] = trial_dict        
       
        query_string = """SELECT  * FROM "day_data_ddcm7"  WHERE  "far" = '""" + far + """' AND "mon" = '""" + mon + """' and "asn" = '""" + asn + """'  GROUP BY *   """
                #print query_string
        day_items = metaclient.query(query_string)
        dkeys = day_items.keys()
        days = {}
        d_days_found = []
        if len(dkeys) > 0: #already did some days
            for k in dkeys:
                ddict = {}
                ddict.update(k[1])
                ddict.update(day_items[k].next())
                day = int(ddict['day'])
                d_days_found.append(day)
                days[day] = ddict
        #print '   Weeks: {}'.format(w_week_dict.keys())
        

    
        if len(d_days_found) == 0:
            print 'No day data for this trace'
            ending_day = starting_day
        else:
            ending_day =  max(d_days_found)

        print 'Trace {} {} {}, day {} to {}'.format(far, asn, mon, starting_day, ending_day)
        #heading = ['I', 'Day', 'daynum','C', 'Ct', 'Ci','Co', 'Cnt', 'Cni', 'AC3', 'Cnt3', 'Cnt f', 'Cnt n', 'LB f', 'LB n', 'CO f', 'CO n', 'UB f', 'UB n', 'P 1', 'Fr', 'Zr', 'Rcut', 'Q','GdDy', 'CgDy','8days', 'ASRT', 'RULE', 'VASRT', 'VRULE']
        heading = ['I', 'Day', 'daynum','C', 'Ct', 'Ci', 'Cnt', 'Cni',  'Cnt f', 'Cnt n', 'LB f', 'LB n', 'CO f', 'CO n', 'UB f', 'UB n', 'Pear', 'ICent', 'FCent', '50-Cent','Peaks', 'SD','GdDy', 'CgDy','8days', 'ASRT', 'RULE']
 
        do_repeat = False
        assertion_insert = []
        success = 113 #Dummy value :-)
        '''The overall purpose of the following bit of convoluted code is to 
        find the starting date and ending date for the validation process, while
        dealing with various oddities that result from the processing phase,
        including missing 50 day intervals where the monitor recorded no data, 
        and there are no values in the days list.'''
        if starting_day not in days.keys():
            print 'Error--no data for starting day'
            day = starting_day
        else:
            day = int(days[starting_day]['first_day'])
            
        p_dict = {}
        va_dict = {}
        
        for tday in range(day, ending_day + 1):
            if tday in days.keys():
                day_dict = days[tday]
                asrt = int(day_dict['revised-asrt'])
                days[tday]['old-asrt'] = asrt
                #Remember original assertion for plotting
                #Note: might be different from validated assertion if 
                #previously validated. But normally no prior validation.
                if asrt == 8 or asrt == 3:
                    p_dict[tday] = float(day_dict['ftcongestion'])
        if len(prior_p_dict) > 0:
            cor, c_days = compute_correlation(p_dict, prior_p_dict)
            print 'Prior correlation {}, correlated days {}'.format(cor, c_days)
        else: 
            cor = 0
            print 'No prior trace for correlation'
        if cor > .8:
             use_history = True
        else:
            use_history = False
        if cor > .8 and c_days > 7:
            accept_history = True
        else:
            accept_history = False
        
        
                    
        while day <= ending_day:
            if day not in days.keys():
                print 'Error--no data for first day'
            else:
                day = int(days[day]['first_day'])
            #If possible, start validation on the 50 day boundary 
            #used for processing. 
            #find next starting day
            while (day not in days.keys()):
                day += 1
                print 'Missing: ', day
                if day > ending_day:
                    print 'Error: no further days with data'
                    break
            if day > ending_day:
                continue #this will break out of loop
            tday = day #The next day with data, start here
            last_worth_looing = day
            while tday <= ending_day:
                if tday not in days.keys():
                    tday += 1
                elif int(days[tday]['first_day']) <= day:
                    last_worth_looking = tday
                    tday += 1
                else:
                    break
            #That bit of code looks for the first day for which 'first_day' is 
            #greater than the value in the current trace. That is the start of
            #the _next_ set to validate.
            #When we break, last_worth looking should = the last day in this
            #epoch with data. If all days have data, it will equal to tday -1
            
            next_start = tday
            if tday> ending_day:
                print 'Last sequence', day, ending_day
            else:
                print 'Next sequence', day, next_start - 1
                
            isdone = True  #What has already been validated by human?
            for xday in range(day, next_start):
                if xday not in vdays.keys():
                    isdone = False
                    break
                if int(vdays[xday]['status']) < 3 and\
                int(vdays[xday]['asrt']) > 4:
                    isdone = False
                    break
            '''This can happen only if the processing is redone (a better 
            algorithm, perhaps) and it triggers a new set of required
            validations. The validations are in a different measurement exactly
            so that over time, the results of processing can be flushed and 
            redone, but the human validations survive.'''
            if isdone:
                print 'Skipping ', far,asn,mon, day
                print 'Already done'
                day = next_start
                continue
            #Look at first day and check some 50 day parameters
            need_validation = True
            day_dict = days[day]
            if int(day_dict['corr_days']) < 3:
                rule = 61
                need_validation = False
            elif float(day_dict['overall-centrality']) < .3:
                rule = 62
                need_validation = False
            elif int(day_dict['filter_status']) < 2:
                rule = 63
                need_validation = False
            elif int(day_dict['num_c_events']) > 6:
                rule = 65
                need_validation = False
            elif int(day_dict['congested_days']) > \
                int(day_dict['corr_days']) * 2:
                rule = 66
                need_validation = False
            #no validation needed
            if need_validation == False:
                print 'Validation not needed: rule {} {}'.format(rule,day)
                day = next_start
                continue
            if need_validation == True:
                #those tests did not work, look at prior traces in same group
                
                print 'Possible congested period'
                if do_repeat:
                    need_validation = True
                    do_repeat = False
                else:
                    need_validation = False
                # do_repeat is set by the repeat command and forces the
                #redisplay of the current interval.
                
                for tday in range(day, next_start):
                    if use_history and tday in prior_a_dict:
                        old_asrt = prior_a_dict[tday]
                    else:
                        old_asrt = 0
                    if tday not in days.keys():
                        #No day record. Either error or long missing block
                        asrt = 2
                        rule = 21
                        congestion = 0.0
                    else:
                        day_dict = days[tday]
                        asrt = int(day_dict['revised-asrt'])
                        rule = int(day_dict['rule'])
                        congestion = float(day_dict['fcongestion'])
        
                    if asrt == 8:
                        if (old_asrt == 8 or old_asrt == 2) and \
                        accept_history:
                            print 'Accept congestion assertion, day {}'.format(tday)
                        else:
                            need_validation = True
                            break

                if need_validation == False:
                    for tday in range(day, next_start): 
                        if day not in vdays.keys(): 
                            version = 1
                        else: 
                            version = int(vdays[tday]['version']) + 1
                            old_v_asrt = int(vdays[tday]['asrt'])
                            status = int(vdays[tday]['status'])
                        if version == 1 or\
                         (status != 3 and old_v_asrt != asrt): 
                            assertion_insert.append(make_day_assertion(\
                            'day_data_assertion_ddcm4',\
                            far, mon,asn, day, method,\
                            asrt,\
                            rule,\
                            version,\
                            1,\
                            congestion))
                        va_dict[tday] = asrt #For next time.
                        
                        if tday not in merge_data.keys():
                            merge_data[tday] = []
                        merge_data[tday].append([asrt,rule,congestion,\
                         mon, far, days[tday]['fgood_count']])
                         
                    if len(assertion_insert) > 0:
                        success = metaclient.write_points(assertion_insert)
                        print  'History match: writing {} assertions {}, rule {}'.\
                        format(len(assertion_insert), success, rule)
                        assertion_insert = []
                    day = next_start
                    
                        
                else: #we really have to look at it
            
                    temp_ending_day = last_worth_looking
                        #This sometimes elimintes the plotting of empty days 
                    print 'Plot day {} to {}'.format(day, temp_ending_day)

                    table = []
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

                    far_rtts = []
                    near_rtts = []
                    times = []
                    print 'Fetch far trace'    
                    query_string = """SELECT min("rtt") FROM "tsplnk"  WHERE  "target" = '"""+ far + """' AND "ind" = '1' AND "mon" = '""" + mon +"""'AND "asn" = '"""+ asn + """'AND time > """ + start_time_str + """ AND time < """ + end_time_str + """ GROUP BY time(15m) """
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
                    query_string = """SELECT min("rtt") FROM "tsplnk"  WHERE  "link" =~ """+ link_query + """ AND "ind" = '0' AND "mon" = '""" + mon +"""'AND "asn" = '"""+ asn + """'AND time > """ + start_time_str + """ AND time < """ + end_time_str + """ GROUP BY time(15m) """
                    trace = client.query(query_string)
                    try:    
                        g=trace.items()[0][1]
                    except:
                        print 'Error--no rtts in trace'
                        day = day + 50  
                        continue     
                    for x in g:
                        near_rtts.append(x['min'])
       
                    caption_day = day
                    while caption_day not in days.keys():
                        caption_day += 1
                        
                    #This version of method 9 computes some measures of the 50 day sample, but additional research suggests that there are other, better measures. For the moment, until I update the process phase, they are computed here. 
                        
                    if days[caption_day]['f-end'] < days[caption_day]['f-start']:
                        temp_end = days[caption_day]['f-end'] + 96
                    else:
                        temp_end = days[caption_day]['f-end']
                    filter_count = temp_end - days[caption_day]['f-start'] + 1
                    filter_mean = (filter_count/2 + days[caption_day]['f-start'])%96
                    if days[caption_day]['ef-end'] < days[caption_day]['ef-start']:
                        etemp_end = days[caption_day]['ef-end'] + 96
                    else:
                        etemp_end = days[caption_day]['ef-end']
                    efilter_count = etemp_end - days[caption_day]['ef-start'] + 1
                    efilter_mean = (efilter_count/2 + days[caption_day]['ef-start'])%96
                    if days[caption_day]['of-end'] < days[caption_day]['of-start']:
                        otemp_end = days[caption_day]['of-end'] + 96
                    else:
                        otemp_end = days[caption_day]['of-end']
                    ofilter_count = temp_end - days[caption_day]['of-start'] + 1
                    ofilter_mean = (filter_count/2 + days[caption_day]['of-start'])%96
                    q3 = round(days[caption_day]['q3'],3)
    
                    ediff = min(abs(filter_mean - efilter_mean),96 - abs(filter_mean - efilter_mean))
                    odiff = min(abs(filter_mean - ofilter_mean),96 - abs(filter_mean - ofilter_mean))
    
                    diff = max(ediff,odiff)
                    
                    pf = []
                    pn = []
                    
                    for t in range(len(far_rtts)):
                        if far_rtts[t] != None:
                            pf.append(far_rtts[t])
                    fmin = min(pf)
                    for t in range(len(near_rtts)):
                        if near_rtts[t] != None:
                            pn.append(near_rtts[t])
                    nmin = min(pn)
                    
                    plot_ub = int(max(nmin, fmin) + 50)
                    plot_lb = int(max(min(nmin,fmin) - 10,0))
                        
                    for tday in range(day, temp_ending_day + 1):
                        days[tday]['old-asrt'] = \
                           days[tday]['revised-asrt']
                        #remember what the current process routine did
            
                    colors = \
                        ['w', 'm', 'k', 'g','b', 'y', 'y','c','r', 'r', 'k']
                    alphas = \
                        [.4,  .4,  .4,  .5, .4,  .3,  .5, .5, .5,  .3,  .2]
                    #print tabulate(table,tablefmt="grid")
                    #print 'm_cutoff {}'.format(m_cutoff + m_min)
                    ndtime = []
                    firsttime = times[0]
                    ndtime=[dt.datetime.fromtimestamp(ttime) for ttime  in times]
                    lasttime = times[-1]
                    plt.figure(figsize=(18,6))
                    day_count = temp_ending_day + 4 - day -3
                    ax1 = plt.gca()
                    ax1.set_xlim([dt.datetime.fromtimestamp\
                        (get_day_lb(day - 3)),\
                        dt.datetime.fromtimestamp\
                        (get_day_ub(temp_ending_day+3))])
                    ax1.set_ylim([plot_lb,plot_ub])
                    #plot the near side
                    plt.plot_date(ndtime,near_rtts,'g-',xdate=True,\
                     ydate=False, label = 'near',tz="UTC")
                    plt.hold = True 
                    #plot the far side
                    plt.plot_date(ndtime,far_rtts,'b-',xdate=True,\
                     ydate=False, label = 'far',tz="UTC")
                    plt.hold = True 
                    plt.legend(loc = 'upper right')
        
                    old_vasrt = 0
                    old_vstat = 0
                    old_asrt = 0
                    old_oasrt = -1
                    vstart_day = 0
                    start_day = 0
                    ostart_day = 0
                    #Draw the congestion cutoff line for each day
                    for tday in range(day , temp_ending_day + 1):
                        if tday in days.keys():
                            cutoff = int(days[tday]['fcutoff'])
                            interval = tday - (day - 3)
                            plt.plot(\
                             (dt.datetime.fromtimestamp(get_day_lb(tday)),\
                               dt.datetime.fromtimestamp(get_day_ub(tday))),
                               (cutoff,cutoff), linewidth=1, color = 'k')

                    #Plot the prior validation.
                    for tday in range(day , temp_ending_day + 1):
                        try:
                            vasrt = int(vdays[tday]['asrt'])
                            vstatus = int(vdays[tday]['status'])
                        except:
                            vasrt = 0
                            vstatus = 0
                        
                        if vasrt != old_vasrt or old_vstat != vstatus:
                            if vstart_day > 0:
                                plt.axvspan(\
                                dt.datetime.fromtimestamp\
                                (get_day_lb(vstart_day)),\
                                dt.datetime.fromtimestamp\
                                (get_day_ub(tday-1)),\
                                ymin=.66, ymax=ub, 
                                facecolor = colors[old_vasrt], \
                                alpha = alphas[old_vasrt])
                            vstart_day = tday
                            old_vasrt = vasrt
                            old_vstat = vstatus
                            if vstatus == 3:
                                ub = 1.0
                            else:
                                ub = .84
                        
                            
                    #Plot the original assertions from process-days
                    for tday in range(day , temp_ending_day + 1):
                        if tday in days.keys():
                            asrt = int(days[tday]['old-asrt'])
                        else:
                            asrt = 0
                        if asrt != old_asrt:
                            if start_day > 0:
                                plt.axvspan(\
                                dt.datetime.fromtimestamp(get_day_lb(start_day)),\
                                dt.datetime.fromtimestamp(get_day_ub(tday-1)),\
                                ymin=0, ymax=.33, 
                                facecolor = colors[old_asrt],\
                                 alpha = alphas[old_asrt])
                            start_day = tday
                            old_asrt = asrt
                    #plot the current assertions
                    for tday in range(day , temp_ending_day + 1):
                        try:
                            oasrt = int(days[tday]['revised-asrt'])
                        except:
                            oasrt = 0
                        oub = .66
                        
                        if oasrt != old_oasrt:
                            if ostart_day > 0:
                                plt.axvspan(\
                                dt.datetime.fromtimestamp(get_day_lb(ostart_day)),\
                                dt.datetime.fromtimestamp(get_day_ub(tday-1)),\
                                ymin=.33, ymax=oub, 
                                facecolor = colors[old_oasrt],\
                                 alpha = alphas[old_oasrt])
                            ostart_day = tday
                            old_oasrt = oasrt
                    #finish the three plots above
                    if start_day > 0:
                        plt.axvspan(\
                        dt.datetime.fromtimestamp(get_day_lb(start_day)),\
                        dt.datetime.fromtimestamp(get_day_ub(tday)),\
                        ymin=0, ymax=.33,\
                        facecolor = colors[asrt], alpha = alphas[asrt])
                    if vstart_day > 0:
                        plt.axvspan(\
                        dt.datetime.fromtimestamp(get_day_lb(vstart_day)),\
                        dt.datetime.fromtimestamp(get_day_ub(tday)),\
                        ymin=.66, ymax=ub,\
                        facecolor = colors[vasrt], alpha = alphas[vasrt])
                    if ostart_day > 0:
                        plt.axvspan(\
                        dt.datetime.fromtimestamp(get_day_lb(ostart_day)),\
                        dt.datetime.fromtimestamp(get_day_ub(tday)),\
                        ymin=.33, ymax=oub,\
                        facecolor = colors[oasrt], alpha = alphas[oasrt])

        
                    plt.xlabel('Days')
                    plt.ylabel('RTT')
                    ax2 = ax1.twiny()
                    newx = [i for i in range(-3,day_count + 3)]
                    newy = [plot_ub - 20]*(day_count+6) 
                    ax2.plot(newx,newy, '|')
                    ax2.set_xlim([-3,day_count + 3]) 
        
                    ax3 = ax1.twinx()
        
                    incongest_array = []
                    congest_array = []
                    total_array = []
                    time_array = []
                    good_count = 0
                    ax3.set_xlim([dt.datetime.fromtimestamp\
                        (get_day_lb(day - 3)),\
                        dt.datetime.fromtimestamp\
                        (get_day_ub(temp_ending_day+3))])
                    for tday in range(day -3 , temp_ending_day + 4):
                        if tday < day or tday > temp_ending_day:
                            congest_array.append(None)
                            incongest_array.append(None)
                            total_array.append(None)
                        else:
                            if tday in days.keys() and \
                            days[tday]['old-asrt'] == 8 or\
                            days[tday]['revised-asrt'] == 8:
                                incongest_array.append\
                                (float(days[tday]['ficongestion']))
                                congest_array.append\
                                (float(days[tday]['fcongestion']))
                                total_array.append\
                                (float(days[tday]['ftcongestion']))
                                good_count += 1
                            else: 
                                incongest_array.append(None)
                                congest_array.append(None)
                                total_array.append(None)
                        time_array.append(\
                        dt.datetime.fromtimestamp(get_day_lb(tday) + 43200))
                    if good_count > 0:
                        plt.plot_date(time_array,incongest_array,'k-', marker = 'o', linestyle = '-', xdate=True, ydate=False, label = 'C',tz="UTC")
                        plt.plot_date(time_array,total_array,'r-', marker = 'o', linestyle = '-', xdate=True, ydate=False, label = 'C',tz="UTC")
                        plt.plot_date(time_array,congest_array,'g-', marker = 'o', linestyle = '-', xdate=True, ydate=False, label = 'C',tz="UTC")
                    #ax2.set_xlim([-3,day_count + 3])            #plt.tight_layout() 
        
                    caption = '{} {}: F: {} {} {}, {}, peaks {},mean {}, cen {}, Q {}\n'.format(ASname, mon,
                    filter_mean, efilter_mean, ofilter_mean,\
                    filter_count,\
                    days[caption_day]['num_c_events'],\
                    round(days[caption_day]['d-mean'],3),\
                     round(days[caption_day]['overall-centrality'],3), q3)
                    plt.title(caption)
                    plt.gcf().autofmt_xdate()
                    #plt.ylim([-cap*3, cap*5])
                    plt.hold = False
                    plt.show(block = False) 

                    #Once the code displays the 59 day trace and the candidate validation values, it allows the human validator to enter a variety of instructions, to further explore the data and to revise the validations. The final instruction is normally the "k" instruction, which writes out the validations. 
                    not_done = True
                    index = -1
                    new_congestion = None
                    while not_done:
                        arg_list  = raw_input\
                        ('k to commit, e to edit day, q to exit:').split()
                        if len(arg_list) == 0:
                            continue
                        s = arg_list[0]
                        if len(arg_list) > 1:
                            s_param = arg_list[1]
                        else:
                            s_param = '-'
            
                        if s == 'q':
                            not_done = False
                            plt.close()
                            return(False,p_dict, va_dict) 
            
                        if s == 'e':
                            #Set the day to explore or edit
                            if s_param == '-':
                                table_index = int(raw_input('Enter day index: '))
                            else:
                                table_index = int(s_param)
                            e_day = day + table_index
                            trace_day = e_day - day + 3
                            date_str = time.strftime('%b %d',\
                             time.gmtime(get_day_lb(e_day)))
                            print 'Revising day {} {}'.format(e_day, date_str)
                            continue
            
                        if s == 'n': #Move to next day
                            table_index += 1
                            e_day = day + table_index
                            trace_day = e_day - day + 3
                            date_str = time.strftime('%b %d',\
                             time.gmtime(get_day_lb(e_day)))
                            print 'Revising day {} {}'.format(e_day, date_str)
                            s = 'day'
            
            
                        if s == 'p': #Move to previous say
                            table_index += -1
                            e_day = day + table_index
                            trace_day = e_day - day + 3
                            date_str = time.strftime('%b %d',\
                             time.gmtime(get_day_lb(e_day)))
                            print 'Revising day {} {}'.format(e_day, date_str)
                            s = 'day'
            
        
                        if s == 'break':
                            ipdb.set_trace()
                            continue
            
                        if s == 'v': #Change the assertion for a specific day
                            if s_param == '-':
                                new_asrt = int(raw_input\
                            ('Enter new assertion value: '))
                            else:
                                new_asrt = int(s_param)
                            days[e_day]['revised-asrt'] = new_asrt
                            days[e_day]['rule'] = 20
                            if e_day in vdays.keys():
                                vdays[e_day]['status'] = 2 #force replacement
                            date_str = time.strftime('%b %d',\
                             time.gmtime(get_day_lb(e_day)))
                            print 'New code for date {}: {}'.\
                            format(date_str, days[e_day]['revised-asrt'])
                            continue
            
                        if s == 'u': #(Use) Force a revision of the assertion
                            if e_day in vdays.keys():
                                vdays[e_day]['status'] = 2 #force replacement
                            date_str = time.strftime('%b %d',\
                             time.gmtime(get_day_lb(e_day)))
                            print 'New code for date {}: {}'.\
                            format(date_str, days[e_day]['revised-asrt'])
                            continue
            
            
# heading = ['I', 'Day', 'daynum','C', 'Ct', 'Ci', 'Cnt', 'Cni',  'Cnt f', 'Cnt n', 'LB f', 'LB n', 'CO f', 'CO n', 'UB f', 'UB n', 'Pear', 'ICent', 'FCent', '50-Cent','NumSlots', 'SD','GdDy', 'CgDy','8days', 'ASRT', 'RULE']

                        if s == 'day':#Print out values for the day
                            if s_param != '-':
                                table_index = int(s_param)
                                e_day = day + table_index
                                trace_day = e_day - day + 3
                            if e_day in days.keys():
                                day_dict = days[e_day]
                                date_str = time.strftime('%b %d',\
                                 time.gmtime(get_day_lb(e_day))) 
                                table = [] 
                                table.append(heading) 
                                day_list = ([table_index,date_str,e_day,\
                                round(day_dict['fcongestion'],3),\
                                round(day_dict['ftcongestion'],3),\
                                round(day_dict['ficongestion'],3),\
                                round(day_dict['ntcongestion'],3),\
                                round(day_dict['nicongestion'],3),\
                                day_dict['fgood_count'],\
                                day_dict['ngood_count'], \
                                day_dict['fl_bound'],\
                                day_dict['nl_bound'],\
                                day_dict['fcutoff'],\
                                day_dict['ncutoff'], \
                                day_dict['fu_bound'],\
                                day_dict['nu_bound'],\
                                #day_dict['fu_bound_3'],\
                                round(day_dict['pearson_1'],3),\
                                round(day_dict['initial-centrality'],3),\
                                round(day_dict['final-centrality'],3),\
                                round(day_dict['overall-centrality'],3),\
                                round(day_dict['num_c_events'],3),\
                                round(day_dict['d-sd'],3),\
                                day_dict['good_days'],\
                                day_dict['congested_days'],\
                                day_dict['corr_days'],\
                                day_dict['revised-asrt'],\
                                day_dict['rule']
                                ])
                                # if e_day in vdays.keys():
#                                         day_list  = day_list + [
#                                         int(vdays[e_day]['asrt']),\
#                                          int(vdays[e_day]['rule'])]
                    
                                table.append(day_list)
                                print tabulate(table,tablefmt="grid")
                            else:
                                print 'No data for day {}'.format(e_day)
                            continue       
                        
                        if s == 'd': #shift the value of the cutoff for the day
                            if s_param == '-':
                                delta = int(raw_input\
                            ('Enter offset delta: '))
                            else:
                                delta = int(s_param)
                            days[e_day]['fcutoff'] = days[e_day]['fcutoff'] + delta
                            continue
        
                        if s == 'c':#recompute the congestion (after using "d")
                            print 'Recomputing congestion, offset = {}'.\
                            format(days[e_day]['fcutoff'])
                            day_index = (e_day - day  + 3) * 96
                            new_congestion = \
                            recompute_congestion(far_rtts[day_index:day_index +\
                             96],days[e_day]['fcutoff'], False)
                            print 'New congestion: ', new_congestion
                            continue
        
            
                        if s == 'forall': #revise a class of assertion
                            if s_param == '-':
                                old_asrt = int(raw_input\
                            ('Enter old assertion: '))
                            else:
                                old_asrt = int(s_param)
                            new_asrt = int(raw_input('Enter new assertion: '))
                            for tday in range(day , temp_ending_day + 1):
                                if tday not in days.keys():
                                    continue
                                if days[tday]['revised-asrt'] == old_asrt:
                                    days[tday]['revised-asrt'] = new_asrt
                                    days[tday]['rule'] = 20
                                    if tday in vdays.keys():
                                        vdays[tday]['status'] = 2
                                    if new_asrt == 8:
                                        day_index = (tday - day  + 3) * 96
                                        new_congestion = \
                                        recompute_congestion\
                                        (far_rtts[day_index:day_index +\
                                        96],days[tday]['fcutoff'], test)
                                        days[tday]['fcongestion'] =\
                                         new_congestion
                                        print 'New congestion: ',\
                                         new_congestion   
                                    date_str = time.strftime('%b %d',\
                                        time.gmtime(get_day_lb(tday)))
                                    print 'New code for date {}: {}'.\
                                    format(date_str, days[tday]['revised-asrt'])    
                            continue
                
                      
                        if s == '85': #Change all assertions of 8 to 5
                            for tday in range(day , temp_ending_day + 1):
                                if tday not in days.keys():
                                    continue
                                if days[tday]['revised-asrt'] == 8:
                                    days[tday]['revised-asrt'] = 5
                                    days[tday]['rule'] = 20
                                    if tday in vdays.keys():
                                        vdays[tday]['status'] = 2
                                    date_str = time.strftime('%b %d',\
                                        time.gmtime(get_day_lb(tday)))
                                    print 'New code for date {}: {}'.\
                                    format(date_str, days[tday]['revised-asrt'])    
                            continue
                
                        if s == '84':
                            for tday in range(day , temp_ending_day + 1):
                                if tday not in days.keys():
                                    continue
                                if days[tday]['revised-asrt'] == 8:
                                    days[tday]['revised-asrt'] = 4
                                    days[tday]['rule'] = 20
                                    if tday in vdays.keys():
                                        vdays[tday]['status'] = 2
                                    date_str = time.strftime('%b %d',\
                                        time.gmtime(get_day_lb(tday)))
                                    print 'New code for date {}: {}'.\
                                    format(date_str, days[tday]['revised-asrt'])    
                            continue
                            
                        if s == '83':
                            for tday in range(day , temp_ending_day + 1):
                                if tday not in days.keys():
                                    continue
                                if days[tday]['revised-asrt'] == 8:
                                    days[tday]['revised-asrt'] = 3
                                    days[tday]['rule'] = 20
                                    if tday in vdays.keys():
                                        vdays[tday]['status'] = 2
                                    date_str = time.strftime('%b %d',\
                                        time.gmtime(get_day_lb(tday)))
                                    print 'New code for date {}: {}'.\
                                    format(date_str, days[tday]['revised-asrt'])    
                            continue
                            
                        if s == '81':
                            for tday in range(day , temp_ending_day + 1):
                                if tday not in days.keys():
                                    continue
                                if days[tday]['revised-asrt'] == 8:
                                    days[tday]['revised-asrt'] = 1
                                    days[tday]['rule'] = 20
                                    if tday in vdays.keys():
                                        vdays[tday]['status'] = 2
                                    date_str = time.strftime('%b %d',\
                                        time.gmtime(get_day_lb(tday)))
                                    print 'New code for date {}: {}'.\
                                    format(date_str, days[tday]['revised-asrt'])    
                            continue
                            
      
                        if s == 'f': #Recompute the filter
                            f_start,f_end, d_count, d_set,t2p,peak_options, \
                            length_list, fifty_centrality, filter_status, j_days =\
                            make_filter(far_rtts,\
                            near_rtts, day - 3, days ) 
                            print 'Filter range: {}, {} status {}'.\
                            format(f_start,f_end,filter_status)
                            print t2p
                            print d_set
                            print 'Overall centrality', fifty_centrality
                            #print t2p
                            print 'Max days in slot:', max(d_count)
                            print peak_options 
                            continue

                  
                        if s == 'repeat':
                            not_done = False
                            plt.close()
                            do_repeat = True  
                            continue
               
                        if s == 'skip' or s == 's':
                            not_done = False
                            plt.close()
                            day = temp_ending_day + 1
                            trial_start_day = day
                            possible_look_count = 0
                            two_count = 0
                            start_plot = False 
                            continue
                   
                        if s == 'k': #Write out the validations. 
                            not_done = False
                            plt.close() 
                            assertion_insert = []
                            
                            for tday in range(day, temp_ending_day + 1):      
                
                                if tday in days.keys():
                                    asrt = int(days[tday]['revised-asrt'])
                                    rule = int(days[tday]['rule'])
                                    if asrt == 8:
                                        congestion =\
                                        float(days[tday]['fcongestion'])
                                    else:
                                        congestion = 0.0
                                else:
                                    asrt = 2
                                    rule = 21
                                    congestion = 0.0
                                    
                                va_dict[tday] = asrt #for next trace
                            
                                #Fill in the merge data for later merging.
                                if tday not in merge_data.keys():
                                    merge_data[tday] = []
                                merge_data[tday].append([asrt,rule,congestion,\
                                 mon, far,int(days[tday]['fgood_count'])]) 
                                
                                old_version = 0
                                if tday in vdays.keys():
                                    old_version =\
                                     int(vdays[tday]['version'])
                                    status = int(vdays[tday]['status'])
                                    vasrt = int(vdays[tday]['asrt'])
                                else: 
                                    vasrt = 0
                                    status = 0
                                version = old_version + 1

                                if version == 1 or status != 3:
                                    assertion_insert.append\
                                    (make_day_assertion\
                                    ('day_data_assertion_ddcm4',\
                                        far, mon,asn, tday, method,\
                                        asrt,\
                                        rule,\
                                        version,\
                                        3,\
                                        congestion))
                                   # print assertion_insert[-1]
        #                         if tday not in history.keys():
        #                             history[tday] = asrt
                            if evaluate:
                                if s_param == '-':
                                    accept_code = int(raw_input\
                                    ('Enter accept code: '))
                                else:
                                    accept_code = int(s_param)
                                    
                                accept_string = ",".join(map(str, [\
                                far,mon,asn,days[caption_day]['first_day'],\
                                filter_mean, efilter_mean, ofilter_mean,\
                                filter_count,\
                                days[tday]['good_days'],\
                                days[tday]['congested_days'],\
                                days[tday]['corr_days'],\
                                days[caption_day]['num_c_events'],\
                                round(days[caption_day]['d-mean'],3),\
                                round(days[caption_day]\
                                   ['overall-centrality'],3),\
                                q3,\
                                accept_code]
                                )) + '\n'
                                print accept_string
                                v_eval.write(accept_string)
                                #ipdb.set_trace()       
                            if len(assertion_insert) > 0:
                                #success = 47
                                success =\
                                 metaclient.write_points(assertion_insert)
                                print 'Write assertions: Success {}, count {}'.format(success,\
                                len(assertion_insert))
                                assertion_insert = []
                            day = temp_ending_day + 1
                            start_plot = False 
                            continue
        v_eval.close()                    
        return(True,p_dict, va_dict)
        
        
def make_day_entry(measurement,far,mon,asn,ind,day,method, auto_c_3,\
    auto_c_7, bottom, cap,cutoff,congestion,code, good_count, \
    pearson = 0, pearson_3 = 0, pearson_7 = 0):
    time_str = time.strftime('%Y-%m-%dT00:00:00Z',\
     time.localtime(get_day_lb(day)))
    return {"measurement": measurement,   
"tags": 
        {
        "far": far,
        "mon": mon,
        "asn": asn,
        "ind": str(ind),
        "day": str(day),
        "method": method
},
"time": time_str,
"fields": 
        {
        "auto_c_3": auto_c_3,
        "auto_c_7": auto_c_7,
        "l_bound": bottom,
        "u_bound": cap,
        "cutoff": cutoff,
        "congestion": congestion,
        "process_code": code,
        "good_count": good_count,
        "pearson": pearson,
        "pearson_3": pearson_3,
        "pearson_7": pearson_7
}
}
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

    
def find_mean(p):
    c_list = []
    for tday in p.keys():
        c_list.append(p[tday])
    if len(c_list) == 0:
        pmean = 0
    else:
        pmean = st.mean(c_list)
    return min(p.keys()), max(p.keys()), pmean
    
def compute_correlation(p1,p2): 
    #This is a simple correlation that is not scale invariant but absolute.
    #In contrast to Pearson. 
    d1min, d1max, p1mean = find_mean(p1)
    d2min, d2max, p2mean = find_mean(p2)
    dstart = min(d1min, d2min)
    dend = max(d1max, d2max)
    c_days = 0
    diffs = []
    #ipdb.set_trace()
    for tday in range(dstart,dend + 1):
        if (tday in p1.keys()) and (tday in p2.keys()):
            if p1[tday] == 0 and p2[tday] == 0:
                continue
            di = abs(p1[tday] - p2[tday])
            if p1[tday] == 0 or p2[tday] == 0:
                di = di * 3 #An error in estimating the duration is less important
                    #than one day being congested and the other not
            diffs.append(di)
            if p1[tday] > 0 or p2[tday] > 0:
                c_days += 1
    if len(diffs) > 0:
        rval = 1 - st.mean(diffs)*2.8 #arbitrary scale factor to mimic pearson
    else:
        rval = 2.0
    return rval, c_days
                    

        
    
        
def main():
    
    validate_days('ddc-m7')

if __name__ == '__main__':
    main()