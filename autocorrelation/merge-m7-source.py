#This code attemps to combine links into link groups, and computes the resulting assertion about the daily congestion for each day of each group. 
#I acknowledge that this is Very Gnarly Code. Sorry. 

# import modules used here 
import sys
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import cm
#import statistics
from scipy import signal
#from status_defs import * 
#from range_defs import *
#from find_power import compute_pdf_spd, find_sq_power
from weeks import *
import time
import statistics as st
from tabulate import tabulate
from influxdb import InfluxDBClient
from tinydb import TinyDB, Query
import ipdb



def make_merge_groups(method, test):

    fixup_options = {}
        
    def compare_cp_options(primary_merge_groups,trace_info):
        result_list = []
        for i in range(len(primary_merge_groups)):
            merge_group = merge_group_list[primary_merge_groups[i]]
            day_set = set()
            for tr in merge_group:
                tmon = tr[0]
                tfar = tr[1]    
                old_trace_info = trace_info_dict[(tmon,tfar)]
                for tday in old_trace_info['day-list']:
                    day_set.add(tday)
            if three_overlap(day_set, trace_info['day-list']) > 5:
                continue
                # Do not do a CP merge into a trace with fewer days 
                #than the CP option. Anything can happen, but...
            tmon = merge_group[0][0]
            tfar = merge_group[0][1]
            old_trace_info = trace_info_dict[(tmon,tfar)]
            if old_trace_info['c-count'] > 0 and trace_info['c-count'] > 0:
                pco = compute_correlation(old_trace_info['p-data'],\
                 trace_info['p-data'])
                if pco == 2.0:
                    pco = -10
                #print 'CP option: i {}, P {}    '.\
                            #format(primary_merge_groups[i],pco, type)
                if pco > 0: #there has to be overlap in this case
                    result_list.append([primary_merge_groups[i],pco])
        if len(result_list) > 0:
            result_list.sort(key = lambda i: i[1], reverse = True)
        return result_list
                
    def make_group(trial_group, merge_group_list,\
        primary_merge_groups,trace_info_dict,\
        fixup_options):
        #check options for a CP merge (type 3)
        trial_group.sort(key = lambda ind: ind[2], reverse = True)
            #pick the one with the most days. 
        tfar = trial_group[0][1] # pick option from LAG with the most days
        trial_trace_info = trace_info_dict[(mon,tfar)]
        cp_options = \
        compare_cp_options(primary_merge_groups,trial_trace_info)
        if len(cp_options) > 0:
            max_cp = cp_options[0][1]
        else:
            max_cp = 0.0
            
        #Now check options for a cross-monitor merge (type 2)
        groupindex = set()
        norm_options = []
        for t in trial_group:
            tempfar = t[1] #get the far
            for tmon in mon_far_dict.keys():
                if tmon == mon: #don't compare with yourself...
                    continue
                if (tmon,tempfar) in trace_info_dict.keys():
                    #There is a trace with these params
                    target_trace_info = trace_info_dict[(tmon,tempfar)]
                    t_g_index = target_trace_info['merge-group']
                    t_g_type = target_trace_info['merge-type']
                    #print 'Found normal: far {}, mon {}, group {}, type {}'.\
                    #format(tempfar, tmon, t_g_index,t_g_type) 
                    if t_g_type == 0: # not yet merged.  I do 
            #normal merges into groups that are already merged (e.g., the first merge
            #is just a LAG merge). But I need to check here to see if the normal option 
            #(even if I do the actual merge later) is preferable to a CP merge.
                        trace_info = trace_info_dict[(mon,tempfar)]
                        
                        if target_trace_info['c-count'] > 0 or\
                            trace_info['c-count'] > 0:
                            pco = compute_correlation(target_trace_info\
                            ['p-data'], trace_info['p-data'])
                            if pco == 2.0:
                                pco = -10
                            #print 'Normal merge option: trial: {} {}, target {} {}, P {}, type {}'.\
                               #format(tmon,tempfar, mon, tempfar, pco,\
                               
                               #target_trace_info['merge-type'])
                            norm_options.append([tmon,pco, -1, tempfar])
                        else:
                            #neither new or candidate has any congestion. 
                            #print 'Normal merge option (no C): trial: {} {}, target {} {}, P {}, type {}'.\
                               #format(tmon,tempfar, mon, tempfar, 2.0,\
                               #target_trace_info['merge-type'])
                            norm_options.append([tmon, -10, -1,tempfar])   
                    
                    elif t_g_type < 3:
                        #that trace has not been merged into a CP group
                        groupindex.add(t_g_index)
                        
            
        if len(groupindex) > 0:
            #for some far in the trial_group, we found a trace with that same far
            # from another monitor. 
            
            #If the other candidate is in a CP merge, I already compared it to this one
            #when I did that merge. 
            
            #If we found more than one, we have an oddity. This means that for two mons,
            #we formed separate (not CF) LAG groups, which means they did not seem
            #similar, which would not normally happen.
            #The most probabl reason is that one CP returns over a path for which we 
            #have no forward probes.
            #This can only happen for traces with congestion. Traces without congestion
            #will always be merged.  
            for ti in groupindex:  #So there should be only one, but...
                #print 'Group {} candidate for normal merge'.format(ti)
                merge_group = merge_group_list[ti] #The candidate group
                for tr in merge_group: #for each trace in that group
                    tmon = tr[0]
                    tfar = tr[1]
                    target_trace_info = trace_info_dict[(tmon,tfar)]
                    for t in trial_group:
                        trialmon = t[0]
                        trialfar = t[1]
                        trace_info = trace_info_dict[(trialmon,trialfar)]
                        
                        if target_trace_info['c-count'] > 0 or\
                         trace_info['c-count'] > 0:
                            pco = compute_correlation\
                               (target_trace_info['p-data'],\
                                trace_info['p-data'])
                            if pco == 2.0:
                                pco = -10
                           # print 'Normal merge option: trial: {} {}, target {} {}, P {}, type {}'.\
                               #format(trialmon,trialfar, tmon, tfar, pco,\
                               #target_trace_info['merge-type'])
                            norm_options.append([tmon,pco, ti, tfar])
                        else:
                            #neither new or candidate has any congestion. 
                            #print 'Normal merge option (no C): trial: {} {}, target {} {}, P {}, type {}'.\
                               #format(trialmon,trialfar, tmon, tfar, 2.0,\
                               #target_trace_info['merge-type'])
                            norm_options.append([tmon, -10.0, ti, tfar])
        
        if len(norm_options) > 0:
            norm_options.sort(key = lambda i: i[1], reverse = True) #best first
        
        #print 'Norm options', norm_options
        if len(norm_options) > 0:
            max_normal = norm_options[0][1]
        else:
            max_normal = 0.0
            
            #Do not use lack of overlap or lack of congestion as basis for a CP merge.

        if ((max_cp > max_normal) and max_cp > .8):
            #CP merge into existing group for this monitor.
            group_index = cp_options[0][0]
            print 'CP merge into group ', group_index
            for g in trial_group:
                trace_info_dict[(mon,g[1])]['merge-group'] =\
                    group_index
                trace_info_dict[(mon,g[1])]['merge-type'] = 3
                g[3] = max_cp
            merge_group_list[group_index] += trial_group
            merge_group_list[group_index].sort\
                (key = lambda i: i[2], reverse = True)
        else:
            #candidate for cross-monitor merge
            nmerge = False 
            best_fixup_option = [-1,0.0]   
            for option in norm_options:
                if option[2] >= 0: #Already in group.
                    if option[1] <= .7 and option[1] > -10:
                        best_fixup_option = [option[2], option[1]]
                        break #Poor fit with a trace that is already in the group.
                    t_g_index = option[2]
                    print 'Normal merge into index ', t_g_index
                    for g in trial_group:
                        trace_info_dict[(mon,g[1])]['merge-group'] =\
                          t_g_index
                        trace_info_dict[(mon,g[1])]['merge-type'] = 2
                        g[3] = option[1]
                    merge_group_list[t_g_index] += trial_group
                    merge_group_list[t_g_index].sort\
                    (key = lambda i: i[2], reverse = True)
                    nmerge = True
                    if trace_info_dict[(mon,trial_group[0][1])]['diff-max']-\
                        trace_info_dict[(mon,trial_group[0][1])]['diff-min'] <= 3:
                        #just look at the first one, which has the most days 
                        #print 'Making primary, i = ',t_g_index
                        primary_merge_groups.append(t_g_index)
                    break
            
            if nmerge == False:
                if max_normal > 0.0 and norm_options[0][2] >= 0:
                    # Possible error. There is a trace from another monitor, in a normal
                    # group (not a CP group) that is a poor match with this trace. 
                    # Probably means that one should be in a CP group, but we did not
                    # find it.
                    print 'Possible match error', trial_group, mon, norm_options[0][0],\
                        #max_normal
                
                # max(max_cp, min_normal) < .8 and no suitable normal group yet
                #make a new group--a LAG merge group
                trial_group.sort(key = lambda i: i[2], reverse = True)
                merge_group_list.append(trial_group)
                group_index = len(merge_group_list) - 1 
                if len(norm_options) > 0:
                    print 'No viable merge candidate. Making new group, index ',\
                    group_index
                else:
                    print 'Merge group not yet formed. Making new group, index ',\
                    group_index
                #the just created
                for g in trial_group:
                    trace_info_dict[(mon,g[1])]['merge-group'] =\
                        group_index
                    trace_info_dict[(mon,g[1])]['merge-type'] = 1
                if trace_info_dict[(mon,trial_group[0][1])]['diff-max']-\
                    trace_info_dict[(mon,trial_group[0][1])]['diff-min'] <= 3:
                    #just look at the first one, which has the most days 
                    print 'Making primary, i = ',group_index
                    primary_merge_groups.append(group_index)
                if len(cp_options) > 0 or best_fixup_option[0] >= 0:
                    #make a makeup entry--perhaps merge later.
                    if len(cp_options) > 0:
                        best_cp_option = [cp_options[0][0],cp_options[0][1]]
                    else:
                        best_cp_option = [0.0,-1]
                    if (best_cp_option[1] >= .7 or best_fixup_option[1] >= .65):
                        fixup_options[group_index] =\
                         [best_cp_option, best_fixup_option] 
                                            
                
 #main code starts here.                               
    
    debug_mode = False
    error_log = []
    #args are source as (mon), optional dest AS, optional "1" for doplot
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
    
    ASo = open('AS-omit.txt', 'rU')
    ASomit = {}
    for lines in ASo:
        if len(lines) > 0 and lines[0] != '#':
            ASnumstr, ASoname = lines.strip('\n').split(',') #throw away the AS
            ASomit[ASnumstr] = ASoname
    ASo.close()
    
    ASd = open('default-as.txt', 'rU')
    default_as = {}
    for lines in ASd:
        if len(lines) > 0 :
            mon,dasn = lines.strip('\n').split(',')
            default_as[mon] = dasn
    ASd.close()
    
    
    
    client = InfluxDBClient('beamer.caida.org', 8086, NAME,PASSWORD, 'tspmult', ssl = True, verify_ssl=True)
    metaclient = InfluxDBClient('beamer.caida.org', 8086, NAME,PASSWORD, 'tspmeta', ssl = True, verify_ssl=True)
    
    #Warning--slightly gnarly code. The idea is I associate one near As with a #monitor, but the user might not type that number (the other numbers are the #siblings). I need that number for the sequence retrieval. 
    
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
    mon_list = list(mon_set)
    #Now find all the far AS for this source AS
    
    AS_set = set()
    for mon in mon_list:
        print 'Fetching far AS from {}'.format(mon)
        
        #Get the set of near side ASN for this monitor
        
        query_string = """show tag values from "sequence_data_1" with key = "fasn" where "mon" = '""" + mon + """'"""
        
        as_returns = metaclient.query(query_string)
        
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
           
    try:
        nAS_name = ASdict[pasn]
    except:
        try:
            nAS_name = ASdictlong[pasn]
        except:
            nAS_name = 'Unknown'

    print pasn,nAS_name, mon_list
    
    report_list = [] #used to accumulate the final data report

    for fasn in AS_set:
        if fasn in ASomit.keys():
            continue
    
        try:
            AS_name = ASdict[fasn]
        except:
            try:
                AS_name = ASdictlong[fasn]
            except:
                AS_name = 'Unknown'
        print 'Merge groups to ', fasn, AS_name
    
        #remove the old merge data. versioning does not work
        query_string = """delete from "merge_group_traces_ddc1" where "fasn" = '""" + fasn + """' AND "nasn" = '""" + pasn + """'"""
            #query_string = """show tag values from "merged_assertions_ddcm6_2" with key = "asn" """
        deletes = metaclient.query(query_string)
        
        query_string = """delete from "group_day_data_ddc1" where "fasn" = '""" + fasn + """' AND "nasn" = '""" + pasn + """'"""
            #query_string = """show tag values from "merged_assertions_ddcm6_2" with key = "asn" """
        deletes = metaclient.query(query_string)
        
        
        plot_dates = []
        plot_colors = []
        num_lines = 0.0
        lines = []
        dlines = []
        tags = []
        taglines = []
        index = -1

        ''' mon_dict: keyed by mon, has dict of each trace
        mon_far_dict: keyed by mon, list of fars. 
        mon_dict_sort: keyed by mon, has list of form [far, min], each list sorted
             by min
        primary_group dict [mon] list of merge_group_index
        merge_group: list of traces to merge into a group, where entry is [mon, far, numdays, pco]
        merge_group_list: list of merge groups. index is stored in primary_merge_groups, used in mon_dict
        primary_merge_groups: list (for each mon) of groups that are identified as having symmetric return path. Candidates for further CP merging. List of group indices. 
        merge_entry: select by near and far AS: merge_group plus params
        trace_info_dict[(mon,far)]: holds the trace info for each trace
        trace_info: dict of each trace.
            day-list, p-data (dict key of day),day_data {}, min-near, clipped-min-near,diff-min, diff-max (both clipped)
            day_data: asrt, congestion,tcongestion,  n-f-diff  (not done?)
        merge-type: 0 =  not done, 1 = lag, 2 = normal, 3 = CP
        Group index: 
        '''
        
    
    
        day_count = 0
        mon_count = 0
        mon_far_dict = {}
        mon_dict = {}
        mon_dict_sort = {}
        merge_group_list = []
        far_rtt_data = {}
        fixup_options = {}
        trace_insert = []
        #Get a list of all far IPs in the far AS for each monitor
        if len(mon_list) == 0:
            continue  
        for mon in mon_list: 
            print 'Processing ',mon   
            far_set = set()
            query_string = """show tag values from "sequence_data_1" with key = "far" where "fasn" = '""" + fasn + """' AND "mon" = '""" + mon + """'"""
            #query_string = """show tag values from "merged_assertions_ddcm6_2" with key = "asn" """
            far_returns = metaclient.query(query_string)
    
            far_items = far_returns.items()
            try:
                for ta in far_items[0][1]:
                    tfar = ta['value']
                    #print fasn
                    far_set.add(tfar)
                print 'Far count: {}'.format(len(far_set))
            except:
                print 'Error: no fars for mon ', mon
                continue
            far_list = list(far_set)
            mon_far_dict[mon] = sorted(far_list,reverse = True)
            #mon_far_dict: keyed by mon, list of fars. 
        if len(mon_far_dict) == 0: # no data for this AS
            continue   
        #Now do some work.
        #For each mon, for each far, get day data
        trace_info_dict  = {}
        far_set = set()
        mon_count = 0
        if len(mon_far_dict.keys()) == 0: # Nothing, at all
            continue #On to next far AS
        for mon in mon_far_dict.keys(): #only look at mons with any fars
            mon_count += 1
            far_count = 0
            sort_list = []
            for far in  mon_far_dict[mon]:
                far_count += 1
                #if far_count > 5:
                    #break
                far_set.add(far)
                if far not in far_rtt_data.keys():
                    far_rtt_data[far] = {}
                
                print "Processing ", far, mon
                query_string = """SELECT  * FROM "day_data_assertion_ddcm4"  WHERE  "far" = '""" + far + """' AND "mon" = '""" + mon + """' and "asn" = '""" + fasn + """'  GROUP BY "day"   """
                #print query_string
                trace_info_dict_items = metaclient.query(query_string)
                akeys = trace_info_dict_items.keys()
                a_day_dict = {}
                vdays = {}
                print 'Vdays ', len(akeys)
                if len(akeys) > 0:
                    for k in akeys:
                        day = int(k[1]['day'])
                        a_day_dict[day] = k
                        old_version = 0
                        g = trace_info_dict_items[k]    
                        for i in g:
                            trial_version = int(i['version'])
                            if trial_version > old_version:
                                old_version = trial_version
                                trial_dict = i
                        vdays[day] = trial_dict
                        
                                   
                    
                query_string = """SELECT  * FROM "day_data_ddcm7"  WHERE  "far" = '""" + far + """' AND "mon" = '""" + mon + """' and "asn" = '""" + fasn + """'  GROUP BY *   """
                    #print query_string
                day_items = metaclient.query(query_string)
                dkeys = day_items.keys()
                days = {}
                print 'Ddays ', len(dkeys)
                d_days_found = []
                if len(dkeys) > 0: 
                    for k in dkeys:
                        ddict = {}
                        ddict.update(k[1])
                        ddict.update(day_items[k].next())
                        day = int(ddict['day'])
                        d_days_found.append(day)
                        days[day] = ddict 
                    
            
                if len(days) == 0: #No days? Odd...
                    continue #to next far
                trace_key = (far,mon)
                trace_info= {}
                valid_days = []
                colors = []
                good_days = 0
                nears = []
                day_list = []
                day_data = {}
                p_data = {}
                diffs = []
                
                #Now process each day and gather day data
            
                for tday in days.keys():
                    good_count = int(days[tday]['fgood_count'])
                    if good_count == 0:
                        continue
                    if int(days[tday]['ngood_count']) > 30:
                        nears.append(days[tday]['nl_bound'])
                    day_list.append(tday) #Right? Even if no nl_bound?
                if len(nears) == 0:
                    continue
                nears.sort()
                trace_info['min-near'] = min(nears)
                trace_info['clipped-min-near'] =\
                 nears[int(len(nears)/10)] #clip the bottom 10%
                trace_info['day-list'] = day_list
            
            
            #At this point, the overall min is known
                c_count = 0
                good_day_count = 0 
                for tday in day_list:
                    this_day_data = {}
                    ddict = days[tday]
                    if tday in vdays.keys():
                        vdict= vdays[tday]
                        asrt = int(vdict['asrt'])
                    else:
                        asrt = int(days[tday]['revised-asrt'])
                    this_day_data['asrt'] = asrt
                    if asrt > 2:
                        good_day_count += 1
                    if asrt == 8:  
                        congestion = float(ddict['fcongestion'])
                        c_count += 1
                    else:
                        congestion = 0.0
                    this_day_data['congestion'] = congestion
                    if asrt == 3 or asrt == 8:
                        p_data[tday] = congestion
                    diffs.append(days[tday]['fl_bound'] - \
                        days[tday]['nl_bound'])
                    day_data[tday] = this_day_data
                if good_day_count >= 7 and len(p_data) > 0:
                    diffs.sort()
                    trace_info['diff-min'] =\
                     diffs[int(len(diffs)/10)] #clip the bottom 10%
                    trace_info['diff-max'] =\
                     diffs[-int(len(diffs)/10)] #clip the top 10% 
                    trace_info['p-data'] = p_data
                    trace_info['merge-group'] = -1
                    trace_info['merge-type'] = 0
                    trace_info['c-count'] = c_count
                    trace_info['day-data'] = day_data
                    trace_info['far'] = far
                    trace_info_dict[(mon,far)] = trace_info
                    sort_list.append([far,trace_info['clipped-min-near']])
                    far_rtt_data[far][mon]= trace_info['clipped-min-near']
                    #if far == '66.110.56.125':
                        #ipdb.set_trace() 
                else: #less than seven days, but need to do something 
                    near_rtt = trace_info['clipped-min-near']
                    trace_insert.append(define_merge_element(\
                                    'merge_group_traces_ddc1',\
                                    far, mon, pasn,fasn,-2,\
                                    0, near_rtt, method, 1))             
            #At this point, the merge data for each trace is computed
            sort_list.sort(key = lambda i: i[1])
            mon_dict_sort[mon] = sort_list
        if len(trace_insert) > 0:
                #success = 24
                success = metaclient.write_points(trace_insert)
                print 'Write data for un-merged fars: Success {}, {} {} count {}'\
                .format(success, AS_name, -2, len(trace_insert))
        #Now form groups. Do this one monitor at a time.
        #But need data from other monitors, so the previous code must
        #compute data for all the mons
        for mon in mon_dict_sort.keys(): #only look at mons with fars
            print "\nPass 1, mon ", mon
            #Pass 1, Find primary merge groups and CP merges for each mon
    
            trial_group = []
            primary_merge_groups = [] #different for each monitor

            sort_list = mon_dict_sort[mon]
            print '{} traces'.format(len(sort_list))
            while len(sort_list) > 0:
                #Make a lag group, and then see what to do with it.
        
        
                far,first_trace_min = sort_list[0] #take the current first item.
        
                if (mon,far) not in trace_info_dict.keys():
                    print 'Skip {}, no trace info'.format (far)
                    del sort_list[0]
                    continue
    
                first_trace_info = trace_info_dict[(mon,far)]
                print '\n'
                print 'Start new group: far IP ', far, mon, first_trace_min 
        
                if first_trace_info['merge-group'] >= 0: #already done
                    #should not happen
                    del sort_list[0]
                    continue
                # if trace_info['diff-max']-\
    #                 trace_info['diff-min'] > 3:
    #                 no += 1
        
                trial_group.append([mon,far,len(first_trace_info['day-list']), -1])
                del sort_list[0]
                do_more = True
                    # Now see if more traces should be merged into lag
                no = 0
                yes = 0
                pco = 0
                for item in sort_list[:]: 
                    if do_more == False:
                        break
                    no = 0
                    yes = 0
                    pco = 0
                    tfar = item[0]
                    tmin = item[1]
                    trace_info = trace_info_dict[(mon,tfar)]
                    print 'Trying ',tfar, tmin
            
                    # if first_trace_info['diff-max']-\
    #                  first_trace_info['diff-min'] > 3:
    #                     no += 1
                    if close(tmin,first_trace_min, 0):
                        yes += 4
                    elif close(tmin,first_trace_min, 1):
                        yes += 2
                    elif close(tmin, first_trace_min, 2) == False:
                        no += 10
                        do_more = False #sort_list is ordered, so give up here
                        #print '   Near RTT not similar', tmin, first_trace_min
                        break
                    temp_rtt_list = []
                    #this bit of code checks to see whether traces that are the same 
                    # distance from this monitor are also the same distance from
                    # other monitors. Do not merge unless this test is passed. 
                    for tmon in mon_dict_sort.keys():
                        if tmon == mon:
                            continue
                        if tmon in far_rtt_data[tfar].keys() and \
                            tmon in far_rtt_data[far].keys():
                            if close(far_rtt_data[far][tmon],\
                            far_rtt_data[tfar][tmon], 2) == False:
                                no += 6
                                #print '  {} not close for {}'.format(tfar,tmon)
                                break # if one other mon fails, no need to keep looking
            
                    if IP_similar(far, tfar):
                        yes += 2
                    P_options = []
                    D_options = []
                    done_matching = False
                    for trace in trial_group: #Look at all the traces so far
                        old_trace_info = trace_info_dict[(trace[0], trace[1])]
                        if old_trace_info['c-count'] > 0 or trace_info['c-count'] > 0:
                            pco = compute_correlation(old_trace_info['p-data'],\
                             trace_info['p-data'])
                            if pco == 2.0:
                                pco = -10.0 #make the illegal value the min
                            P_options.append(pco)
                        else:
                            D_options.append(compute_day_match\
                            (first_trace_info['day-list'],\
                             trace_info['day-list']))
                    if len(P_options) > 0: #if any P, ignore D
                        #print 'P options : ',P_options    
                        if max(P_options) > .9:
                            yes += 4
                            done_matching = True
                        elif max(P_options) > .8:
                            yes += 2
                            done_matching = True
                        elif max(P_options) < .7 and max(P_options) > -10:
                         # got a match but a poor one
                            no += 4
                            done_matching = True
                    if done_matching == False:
                        #either no P options or there was  no overlap (-10)    
                        trial_group.sort\
                            (key = lambda i: i[2], reverse = True)
                        #restrict ourselves to the one with the most days
                        c_trace = trial_group[0]
                        c_trace_info  = trace_info_dict[(c_trace[0], c_trace[1])]
                        if overlap(c_trace_info['p-data'],\
                            trace_info['p-data']) == 0:
                            yes += 2 #if no overlap, one might replace the other
        
                        # elif (c_trace_info['c-count'] > 5 and trace_info['c-count'] == 0)\
    #                           or (c_trace_info['c-count'] == 0 and trace_info['c-count'] > 5):
    #                             no += 4 # No Pearson if only one congested, but not equal
                        else:
                            if len(D_options) > 0 and  max(D_options) > .5:
                                print 'D options: ', D_options
                                yes += 2
                    if len(P_options) > 0:
                        pco = round(max(P_options),3)
                    else:
                        pco = 2.0
                
                    if (yes - no) >= 4 : #Include in group
                        trial_group.append([mon,tfar,\
                            len(trace_info['day-list']), pco])
                        sort_list.remove(item)
                        print '  Add to group: ', tfar, mon, tmin, yes, \
                        no, pco
                    else:
                        print '   Poor match: ', tfar, mon, tmin, yes, \
                        no, pco
                
            
                print 'Made LAG group :',trial_group,\
                         yes, no, pco
                make_group(trial_group, merge_group_list, primary_merge_groups, \
                        trace_info_dict,fixup_options)
                trial_group = []
                     

            
            if len(trial_group) > 0:
                print 'Make final group.',trial_group,\
                         yes, no
                make_group(trial_group, merge_group_list, primary_merge_groups, \
                trace_info_dict, fixup_options)
                trial_group = []

        # Fixup phase--look at stray unmatched fars.
            #Are there fars from multiple monitors that I did not merge?
            #Wait to see if there are CP merges before doing the marginal cases.
        print 'Fixup phase'

        three_count = 0
        for group in  merge_group_list:
            if len(group) == 0:
                continue #Caused by fixup process
            for tr in group:
                tmon = tr[0]
                tfar = tr[1]
                trace_info = trace_info_dict[(tmon,tfar)]
                if trace_info['merge-type'] == 3: 
                    three_count += 1
    #See whether there have been any "good" CP merges. Don't do marginal if not.
        
        for tgid in fixup_options.keys():
    
            type_count = [0,0,0,0]
    
            
            new_tgid = -1
            mg = merge_group_list[tgid]
            for tr in mg: #each trace in the merge troup
                tmon = tr[0]
                tfar = tr[1]
                trace_info = trace_info_dict[(tmon,tfar)]
                mtype = trace_info['merge-type']
                type_count[mtype] += 1

            if type_count[0] == 0 and type_count[2] == 0 and type_count[3] == 0:
                #if [0] not = 0 an error. Unclassified trace.
                #if [2] = 0 then the group in which this trace sits has no merged
                #traces from another group. Its an outlier. Too poor to merge. 
                #If it is a candidate for a fixup CP merge, do it.
        
                options = fixup_options[tgid]
                if three_count > 0: #CP ok
                    if options[0][1] > options[1][1] and options[0][1] > .7:
                        new_tgid = options[0][0]
                        new_pco = options[0][1]
                        merge_type = 3
                        print 'CP fixup: group {} into group {}'.\
                        format(tgid, new_tgid)
                    elif options[1][1] > .65:
                        new_tgid = options[1][0]
                        new_pco = options[1][1]
                        merge_type = 2
                        print 'Normal fixup: group {} into group {}'.\
                        format(tgid, new_tgid)
                else:
                    if options[1][1] > .65:
                        new_tgid = options[1][0]
                        new_pco = options[1][1]
                        merge_type = 2
                        print 'Normal fixup: group {} into group {}'.\
                        format(tgid, new_tgid)

                if new_tgid >= 0: #found a valid merge
                    for ti in range(len(mg)):
                        trace_info_dict\
                        [(mg[ti][0],mg[ti][1])]['merge-group'] =\
                        new_tgid
                        trace_info_dict[(mg[ti][0],mg[ti][1])]['merge-type'] = \
                           merge_type
                        mg[ti][3] = new_pco
                    merge_group_list[new_tgid] += mg
                    merge_group_list[new_tgid].sort\
                        (key = lambda i: i[2], reverse = True)
                    del fixup_options[tgid]
                    merge_group_list[tgid] = []
                        
                        
        # Record all the traces in all the groups
        
        for group in merge_group_list:
            if len(group) == 0:
                continue #Caused by fixup process
            trace_insert = [] #list of the trace assertions to add to influx        
            for tr in group:
                tmon = tr[0]
                tfar = tr[1]
                trace_info = trace_info_dict[(tmon,tfar)]
                mtype = trace_info['merge-type']
                gid = trace_info['merge-group']
                near_rtt = trace_info['clipped-min-near']
                
                trace_insert.append(define_merge_element(\
                                'merge_group_traces_ddc1',\
                                tfar, tmon, pasn,fasn,gid,\
                                mtype, near_rtt, method, 1)) 
                if gid == 0:
                    print trace_insert
                
            if len(trace_insert) > 0:
                #success = 24
                success = metaclient.write_points(trace_insert)
                print 'Write merge group data: Success {}, {} {} count {}'\
                .format(success, AS_name, gid, len(trace_insert))

        #Compute some stats for this far AS        
        trace_count = 0
        group_count = 0
        one_count = 0
        three_count = 0
        merge_failures = 0
        for group in  merge_group_list:
            if len(group) == 0:
                continue #Caused by fixup process
            group_count += 1
            for tr in group:
                tmon = tr[0]
                tfar = tr[1]
                trace_count += 1
                trace_info = trace_info_dict[(tmon,tfar)]
                if trace_info['merge-type'] == 3: 
                    three_count += 1
                if trace_info['merge-type'] == 1: 
                    one_count += 1
                    
        for far in far_set:
            far_groups = set()
            for mon in mon_dict_sort.keys():
                if (mon,far) in trace_info_dict.keys():
                    trace_info = trace_info_dict[(mon,far)] 
                    mtype = trace_info['merge-type']
                    if mtype == 1:
                        far_groups.add(trace_info['merge-group'])
        
            if len(far_groups) > 1: #failure to merge
                merge_failures += 1
        print '\nTraces: {}, Groups: {}, "one count": {}, CP merges: {}, merge failures: {}\n'.\
           format(trace_count, group_count, one_count, three_count,merge_failures)
        report_list.append\
              ([nAS_name,AS_name,trace_count,\
              group_count,three_count,merge_failures]) 
                             
        #Now compute the merged value for each day for each group
    
        plot_data = {} #for each (far,mon) a dict of days
        merged_group_data = {}
        
        for group in merge_group_list:
            day_dict = {}  #data for each trace in the group, indexed by day
            m_result_dict = {} #the results for each day [asrt,congestion]
    
            #Reconstruct the group id from the traces. intermental program
            #must work this way.
            group_id = -1
            for tr in group:
                tmon = tr[0]
                tfar = tr[1]
                trace_info = trace_info_dict[(tmon,tfar)]
                trial_id = trace_info['merge-group']
                if group_id == -1:
                    group_id = trial_id
                if trial_id != group_id:
                    print 'Malformed group--more than one group id'
                    ipdb.set_trace()
            if group_id == -1: #empty group
                continue 
            print 'Merging group', group_id
            for trace in group:  #mon, far, numdays, pco
                mon = trace[0]
                far = trace[1] 
        
                if (mon,far) not in plot_data.keys():
                    plot_data[(mon,far)] = {}   
                #fasn from input arg.
        
                #print "   Processing ", far, mon
                query_string = """SELECT  * FROM "day_data_ddcm7"  WHERE  "far" = '""" + far + """' AND "mon" = '""" + mon + """' and "asn" = '""" + fasn + """'  GROUP BY *   """
                    #print query_string
                day_items = metaclient.query(query_string)
                dkeys = day_items.keys()
                days = {}
                print 'Ddays ', len(dkeys)
                d_days_found = []
                if len(dkeys) > 0: 
                    for k in dkeys:
                        ddict = {}
                        ddict.update(k[1])
                        ddict.update(day_items[k].next())
                        day = int(ddict['day'])
                        d_days_found.append(day)
                        days[day] = ddict
                
        
                query_string = """SELECT  * FROM "day_data_assertion_ddcm4"  WHERE  "far" = '""" + far + """' AND "mon" = '""" + mon + """' and "asn" = '""" + fasn + """'  GROUP BY "day"   """
                #print query_string
                trace_info_dict_items = metaclient.query(query_string)
                akeys = trace_info_dict_items.keys()
                a_day_dict = {}
                vdays = {}
                print 'Vdays ', len(akeys)
                if len(akeys) > 0:
                    for k in akeys:
                        day = int(k[1]['day'])
                        a_day_dict[day] = k
                        old_version = 0
                        g = trace_info_dict_items[k]    
                        for i in g:
                            trial_version = int(i['version'])
                            if trial_version > old_version:
                                old_version = trial_version
                                trial_dict = i
                        if day not in day_dict.keys():
                            day_dict[day] = []
                        if day not in days.keys():
                            good_count = 0
                        else:
                            good_count = int(days[day]['fgood_count'])
                        
                        tcongestion = float(trial_dict['congestion'])
                        day_dict[day].append([int(trial_dict['asrt']),\
                         int(trial_dict['rule']), int(trial_dict['status']),\
                        mon, far,tcongestion,good_count])
                        #m7 removes any need for two values and use_total. 
                        #[5] is the congestion value from validation
   
            #Now we have all the traces. Merge them.
            days_found = sorted(day_dict.keys())
            if len(days_found) == 0:
                print 'Error--no days for {}'.format(far)
                continue
            starting_day = min(days_found)
            ending_day = max(days_found)
            if (ending_day - starting_day) < 10:
                print 'Skipping {}, insufficient days {}'.\
                format(far, ending_day - starting_day)
    
            test_day_c = []
            test_day = []
            test_flag = 0
            good_mdays = 0
            day_assertion = []
            for tday in days_found:
                far_count_set = set()
                if tday not in day_dict.keys():
                #Nothing for today?? Should not happen. 
                    print 'Error: no data for day {}, {}'.format(tday, far)
                    error_log.append([far,fasn,cmon, tday,101])
                day_list = day_dict[tday]
                # old[asrt,rule,cmon,congestion,\
                    #incongestion, tcongestion, use_total,
                    #days[tday]['good_count']]
                #New[asrt, rule,status,mon,far,congestion,good_count,
                # use_total, tcongestion]
                day_list.sort(key = lambda x: x[5], reverse = True) #congestion
                day_list.sort(key = lambda x: x[0], reverse = True) #assertion
                if len(day_list) > 0:
                    good_mdays += 1
                
                is_congestion = False
                congestion_values = []
                summary = 0
                mversion = 0
                good_meas_list = []
                for c in day_list:
                    good_meas_list.append(c[6])
                    pmon = c[3]
                    pfar = c[4]
                    trace_info = trace_info_dict[(pmon,pfar)]
                    mtype = trace_info['merge-type']
                    if mtype == 1 or mtype == 2:#Added 7/24/18. Don't count CP 
                        far_count_set.add(pfar) 
                    
                    if c[0] == 8:
                        is_congestion = True
                        congestion_values.append(c[5])
                        plot_data[(pmon,pfar)][tday] = [c[0], c[5]]
                        summary = 8
                        continue
                    if c[6] > 0: #Partial days plot grey, empty days white.
                        plot_data[(pmon,pfar)][tday] = [c[0], c[5]]
                    if c[0] == 5:
                        #if there is an 8, this is odd. Flag it.
                        #Most likely near side congestion
                        if is_congestion:
                            if c[5] > .05:
                                error_log.append([far,fasn,c[3], tday,102])
                                summary = 10 #temp value
                            #else summary remains at 8
                        else: 
                            summary = 5
                        continue
                    if c[0] == 4:
                        if is_congestion:
                            #both congestion and near side is ok.
                            #Accept the congestion
                            continue
                        else:
                            if summary == 0:
                                summary = 4
                            continue
                    if c[0] == 3:
                        if is_congestion:
                            trial_congestion = st.median_low(congestion_values)
                            if trial_congestion > .1 and c[6] == 96:
                                #if only a partial day, may have missed the
                                #congestion. Forget about it...
                                print 'Both congested and uncongested {} {} {} {}'.\
                                format(far,fasn, mon, tday)
                                error_log.append([far,fasn,c[3], tday,103])
                        elif (summary == 5 or summary == 4) and\
                            (c[6] > 85 or c[6] > max(good_meas_list) -10 ): 
                            #One clean no congestion
                            #Potentially daring rule
                            summary = 3
                        elif (summary == 5 or summary == 4):
                            if debug_mode:
                                summary = 11
                                print far, fasn, day,summary,day_list
                        else:
                            if summary == 0:
                                summary = 3
                        continue
                    if c[0] <= 2:
                        if summary == 0:
                            summary = 2
                if max(good_meas_list) == 0:
                    summary = 1
                c_val = 0.0
                if summary == 8:
                    c_val = st.median_low(congestion_values)
                elif summary == 10:
                    summary = 8
                    c_val = st.median_low(congestion_values)
                if summary == 0:
                    ipdb.set_trace()
                #mstatus = max(mstatus_list)
                m_result_dict[tday] = [summary,c_val,len(far_count_set)] 
                #write group to influx
                
                day_assertion.append(make_day_merged\
                    ('group_day_data_ddc1', group_id,pasn,fasn,tday,method,\
                     summary, 1, c_val, len(far_count_set)))
                #if good_mdays < 10:
                    #print day_assertion[-1]
            merged_group_data[group_id] = m_result_dict
            if len(day_assertion) > 0:
                #success = 24
                success = metaclient.write_points(day_assertion)
                print 'Write merge group day: Success {}, {} {} count {}'\
                .format(success, AS_name, gid, len(day_assertion)) 
    print 'AS groups from AS ', nAS_name
    for r in report_list:
        print '  {} {} traces, {} groups, {} CP traces, {} merge failures'.\
           format(r[1], r[2],r[3],r[4],r[5])
                  
 
def make_day_merged(measurement,group_id,nasn,fasn,day,method, assertion, version, congestion, far_count):
    time_str = time.strftime('%Y-%m-%dT00:00:00Z',\
     time.localtime(get_day_lb(day)))
    return {"measurement": measurement,   
"tags": 
        {
        "group_id": group_id,
        "nasn": nasn,
        "fasn": fasn,
        "day": str(day),
        "method": method,
        "assertion": assertion,
        "version": version
        
},
"time": time_str,
"fields": 
        {
        "congestion": congestion,
        "far_count": far_count
}
}

def define_merge_element(measurement, far, mon,pasn, fasn,  group_id, mtype, near_rtt,method, version):
    time_str = time.strftime('%Y-%m-%dT00:00:00Z',\
     time.localtime())
    return {"measurement": measurement,   
"tags": 
        {
        "far": far,
        "mon": mon,
        "fasn": fasn,
        "nasn": pasn,
        "group_id": group_id,
        "method": method,
       "version": version
      
},
"time": time_str,

"fields":
        {
        "merge-type": mtype,
        "near-rtt": near_rtt
        }
}



def overlap(p1, p2):
    if len(p1) == 0 or len(p2) == 0:
        return 0
    d1min, d1max, p1mean = find_mean(p1)
    d2min, d2max, p2mean = find_mean(p2)
    count = 0
    dstart = min(d1min, d2min)
    dend = max(d1max, d2max)
    c_days = 0
    #ipdb.set_trace()
    for tday in range(dstart,dend + 1):
        if (tday in p1.keys()) and (tday in p2.keys()): 
            count += 1
    #ipdb.set_trace()
    return count
    
def three_overlap(s1, d3):
    count = 0
    #ipdb.set_trace()
    for tday in d3:
        if tday not in s1: 
            count += 1
    #ipdb.set_trace()
    return count
    
    
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
    #Pearson,is scale-invariant, which is not what we want here. 
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
        #was 12 when including 0. 4 a little too big
    else:
        rval = 2.0
    if c_days < 10: #Arbitrary--deals with failure of c to overlap
        rval = 2.0 # Invalid value, indicating no overlap. Not the same as zero
    #ipdb.set_trace()
    return rval
           
def compute_day_match(d1, d2):
    d1min = min(d1)
    d1max = max(d1)
    d2min = min(d2)
    d2max = max(d2)
    dstart = min(d1min, d2min)
    dend = max(d1max, d2max)
    
    match_count = 0
    for tday in range(dstart,dend + 1):
        if (tday in d1) and (tday in d2): 
            match_count += 1
    #ipdb.set_trace()
    return float(match_count)/(dend-dstart)  
    
def IP_similar(ip1, ip2):
    i11,i12,i13,i14 = ip1.split('.')
    i21,i22,i23,i24 = ip2.split('.')
    if i11 == i21 and i12 == i22 and IP_close(i13,i23):
        return True
    else:  
        return False 
        
def IP_close(a, b):
    if close(int(a), int(b), 3):
        return True
    else:
        return False

def close(a,b,d):
    if abs(a-b) <= d:
        return True
    else:
        return False  
        


def main():   
    make_merge_groups('ddc-merge-1', test = False)

if __name__ == '__main__':
    main()