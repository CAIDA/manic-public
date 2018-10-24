#This module contains many of the routines that do the actual processing of a trace. 

import math as m 
import statistics as st
import ipdb 
import time 
from weeks import *
        
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

#This routine scans through the days in a 50 day trace looking for steps up or down in the near RTT, and rejects them from the sample, since steps can confound the subsequent processing. 

def identify_step(days, first_day, far_rtts):
    num_days = len(far_rtts)/96
    stable_lb = 0
    last_lb = 0
    stable = True
    good_count = 0
    new_count = 0
    bad_count = 0
    for tday in range(first_day, first_day + num_days):
        day_index = (tday-first_day) * 96
        if tday in days.keys():
            lb = days[tday]['fl_bound']
            ub = days[tday]['fu_bound']
            if days[tday]['revised-asrt'] == 2 or\
               days[tday]['revised-asrt'] == 1:
                continue
            if stable_lb == 0:
                stable_lb = lb
                last_lb = lb
                good_count += 1
            else: 
                if close(lb, last_lb, 2):
                    good_count += 1
                    bad_count = 0
                    new_count = 0
                    last_lb = lb
                    if good_count > 3: #three _days_
                        stable = True
                        stable_lb = lb
                elif last_lb - lb > 3: #Downward step
                    stable = False
                    last_lb = lb
                    new_count += 1
                    days[tday]['revised-asrt'] = 1
                    days[tday]['rule'] = 80
                    print 'Reject day {} {}, rule {}'.format(tday,tday \
                     -3 -first_day, 80)
                elif lb - last_lb > 3: #upward step
                    days[tday-1]['revised-asrt'] = 1
                    if days[tday-1]['rule'] == 80:
                        days[tday]['rule'] = 82
                    else:
                        days[tday-1]['rule'] = 81
                    stable = False
                    last_lb = lb
                    new_count += 1
                    print 'Reject day {} {}, rule {}'.format(tday-1, tday - 1\
                     - 3 -first_day, days[tday-1]['rule'])
                else: #not close to new or old, but not by 4
                    bad_count += 1
                    last_lb = lb 
                    if bad_count > 3:
                        stable = False # bad news. Jumping all around.
            #print tday - first_day -3, lb, stable_lb, last_lb, good_count, bad_count, new_count, stable
        #sipdb.set_trace()            
                    
        else:
            print 'Find step: missing day', tday        
    
        

    #Goal of this code is to detect days in which there are step changes in RTT,
    #or downward transients in the RTT. (upward transitins are managed by the
    #normal code, but downward spikes can confuse it. 
    
    return

#This routine takes a 50 day sample, and computes the correlation filter. It calls  pick_peak to do some of the work.        
def make_filter(far_rtts, near_rtts, first_day, days):

    num_days = len(far_rtts)/96
    j_days = 0
    d_count = [0 for x in xrange(96)] 
    d_set = [set() for x in range(96)]             
    n_day_range = []
    zero_runs = []
    day_slots = {}
    num_e_days = 0
    for tday in range(first_day, first_day + num_days):
        
        if tday not in days.keys():
            print '{} missing from days',format(tday)
            continue 
        days[tday]['ftcongestion'] = 0.0
        days[tday]['initial-centrality'] = 1.0
        days[tday]['final-centrality'] = 1.0
        days[tday]['num-slots'] = 0
        days[tday]['num-gaps'] = 0
        
        
        if days[tday]['pearson_1'] > .7: 
            continue 
        if days[tday]['fgood_count'] < 30:
            days[tday]['revised-asrt'] = 2
            days[tday]['rule'] = 2
            continue 
        if (days[tday]['fu_bound'] - \
        days[tday]['fl_bound'] < 7):
            continue 
        if days[tday]['revised-asrt'] == 1:
            continue
        #for this day to be one of the days correlated
        #it must pass the rules above.            
        day_index = (tday-first_day) * 96
        iterate = True
        cutoff = days[tday]['fcutoff']
        more_cutoff = 0
        junk = False
        while iterate:
            day_slots[tday] = []
            for i in range(day_index, day_index+ 96):
                frtt = far_rtts[i]
                nrtt = near_rtts[i]
                index = i%96
                if frtt != None and nrtt != None and\
                    frtt > days[tday]['fcutoff'] and\
                    nrtt < days[tday]['nl_bound'] + max(.5 *\
                    (frtt - days[tday]['fl_bound']),10):
                    day_slots[tday].append(index)
            centrality, num_gaps  = \
                 isjunk(day_slots[tday],days[tday]['fgood_count'])
            if more_cutoff == 0: #first or only time around:
                days[tday]['initial-centrality'] = centrality
            if centrality < .4 or num_gaps > 3:
                more_cutoff += 1
                days[tday]['fcutoff'] = cutoff + more_cutoff
                #print 'day {} {} looks like junk: cen {}, gaps {}, cutoff {}'\
                   #.format(tday, tday - first_day -3, \
                   #centrality, num_gaps, more_cutoff)
                if more_cutoff >= 15:
                    iterate = False
            else:
                iterate = False
            
            if iterate == False: #last time around
                days[tday]['final-centrality'] = centrality
        if days[tday]['final-centrality'] != \
             days[tday]['initial-centrality']:
             j_days += 1
        days[tday]['ftcongestion'] = float(len(day_slots[tday]))/96.0  
        days[tday]['num-slots'] = len(day_slots[tday])
        days[tday]['num-gaps'] = num_gaps
        if len(day_slots[tday]) > 0:
            num_e_days += 1
        #This iteration is a bit of hack. I know...          
        
    #d_count is the number of elevated days in each slot.

    '''Detect and throw out bad days. A day with very high congestion,
    (high index count in the day slot) is probably an error, especially if there
    are few congested days. I will tune this rule up once I run lots of training 
    runs. This is a crude place-holder.
    '''
    
    for tday in day_slots.keys():
        # if len(day_slots[tday]) > 10* num_e_days:
#         #if there are a small number of congested days, 
#         #a day with lots of congestion is probably an error.
#         #This is a rough rule until I get better training data.
#             print 'Rejecting day {} {}, {} congested days, day count {}'\
#                .format(tday, tday-first_day - 3, num_e_days,\
#                 len(day_slots[tday]))
#             day_slots.pop(tday, None)
#             days[tday]['revised-asrt'] = 1
#             days[tday]['rule'] = 33
        if days[tday]['final-centrality'] < .2:
            #spikes or horrible jitter. Reject. .2 is temp
            print 'Rejecting day {} {}, final centrality {}'\
               .format(tday, tday-first_day - 3, days[tday]['final-centrality'])
            day_slots.pop(tday, None)
            days[tday]['revised-asrt'] = 1
            days[tday]['rule'] = 34
            
            #throws out the day
    
    #Now that we have thrown out odd days:
    for tday in range(first_day, first_day + num_days):
        if tday in day_slots.keys():
            for i in range(96):
                if i in day_slots[tday]:
                    d_count[i] += 1
                    d_set[i].add(tday)
    f_start, f_end, filter_status, npid,n_t2p,centrality, peak_options = \
        pick_peak(d_count, d_set)
        
        #Now compute the two splits:
    even = 0
    ed_count = [0 for x in xrange(96)] 
    ed_set = [set() for x in range(96)]
    od_count = [0 for x in xrange(96)] 
    od_set = [set() for x in range(96)]          
    for tday in range(first_day, first_day + num_days):
        if tday in day_slots.keys():
            if even%2 == 0:
                for i in range(96):
                    if i in day_slots[tday]:
                        ed_count[i] += 1
                        ed_set[i].add(tday)
            else:
                for i in range(96):
                    if i in day_slots[tday]:
                        od_count[i] += 1
                        od_set[i].add(tday)
            even += 1
    ef_start, ef_end,efilter_status, enpid,en_t2p,ecentrality, epeak_options = \
        pick_peak(ed_count, ed_set)
    of_start, of_end,ofilter_status, onpid,on_t2p,ocentrality, opeak_options = \
        pick_peak(od_count, od_set) 
    print f_start, f_end,ef_start,ef_end,of_start,of_end  
    #The pick_peak code is separated since I call it three times, once for all the days, and once for the even and odd days. I do this to see how stable the prediction is of the best filter. If the filter is very different for even and odd, a later stage may reject this filter as invalid. 
   
    #ipdb.set_trace()
    length_list = []             
    for tday in day_slots.keys():
        length_list.append(len(day_slots[tday]))
    
    #ipdb.set_trace()            
    return f_start,f_end,ef_start,ef_end, of_start, of_end, d_count, d_set, n_t2p, peak_options,\
     length_list, centrality, filter_status,efilter_status, ofilter_status, j_days
     
     
def pick_peak(d_count, d_set):
    d_max = max(d_count) # The largest number of days in any 15 min slot.
    d_min = min(d_count)
    
    pid_set = [set() for x in xrange(20)] #more than enough?
    
    #pids counted from 1, indexing from 0. Sigh...
    start_low = True #So long as first slots are low
    first_run = 0 #If we start low, length of that run
    last_run = 0 # Trick variable to hold first run
    current_run = 0 #Length of current low period
    n_zero_runs = [] #list of lengths
    f_count = 0
    start = True #Deals with wrapped event
    lap_start_count = 0 #Deals with wrapped event
    len_peaks = {} #index by pid, now many slots in this interval
    best_slot = {} #Slot with highest number of days contributing
    best_slot[1] = 0
    max_day_count = {} #how many days in each p2ds[]
    max_day_count[1] = 0
    n_t2p = [0 for x in xrange(96)] #time->pid
    npid = 1 #Index of current elevated period
    new_filter = [0 for x in xrange(96)]
    f_start = 97
    f_end = -1
    
    starting_count = 0
    peak_options = []
    final_pid = 0
    
    d_diff = d_max -d_min
    d_thresh = d_min + d_diff//3
    
    #Now find periods of elevated latency. If more than one, pick the 
    #"best one".
    filter_status = 0
    centrality = 1.0
    if d_max > 0:
        for i in range(len(d_count)): #all the intervals
            if d_count[i] > d_thresh: #How many days: Tuning param
                start_low = False
                f_count += 1
                if current_run > 0: #This ends a low period
                    n_zero_runs.append(current_run)
                    if not start and current_run > 3:
                        npid += 1
                        max_day_count[npid] = 0 #Initalize to dummy val
                    current_run = 0

                elif first_run > 0:
                    last_run = first_run
                    first_run = 0
                start = False
                n_t2p[i] = npid
                pid_set[npid].update(d_set[i])
            else:
                if start_low: #started off with no congestion
                    first_run += 1
                else:
                    current_run += 1
        if current_run + last_run + first_run > 0:  #final run
                n_zero_runs.append(current_run +\
                 first_run + last_run)
        if first_run + last_run + current_run <= 3: # a peak lapped
            for i in range(len(d_count)):
                if n_t2p[i] == npid:
                    n_t2p[i] = 1
            pid_set[1].update(pid_set[npid])
            if npid > 1:
                npid += -1
        if d_max == 0:
            npid = 0
        
        #This code finds the slot with the most days, and for each pid,
        #the number of slots in the peak. 
        for i in range(len(d_count)):
            tpid = n_t2p[i]
            if tpid > 0:
                if d_count[i] > max_day_count[tpid]:
                    max_day_count[tpid] = d_count[i]
                    best_slot[tpid] = i
                if tpid not in len_peaks.keys():
                    len_peaks[tpid] = 0
                len_peaks[tpid] += 1
        if len(n_zero_runs) == 0: #congested all day
            ave_nzero_run = 0
            max_nzero_run = 0
            centrality = 1.0
        else:
            ave_nzero_run = mean(n_zero_runs) # Mean best measure?
            max_nzero_run = max(n_zero_runs)
            centrality = float(max_nzero_run)/(96-f_count)

        #Now pick the "best peak" if there is more than one.                   
        
        for pi in range(1,npid+1):
            peak_options.append([len(pid_set[pi]),len_peaks[pi], pi])
            
        if len(peak_options) == 0:
            #did not find anything 
            final_pid = 0
        else:
            peak_options.sort\
                (key = lambda i: i[1]) #shortest first
            peak_options.sort\
                (key = lambda i: i[0], reverse = True) #most days first
            #if len(winning_pid) > 1:
            #Odd. Two sets with the same max days.
            #Take the shortest one.
            
            final_pid = peak_options[0][2]
    
        if peak_options[0][0] < 3: #less than three days in the best option
            final_pid = 0 #Ignore it.
        filter_status = 1
        if final_pid > 0:
            filter_status = 2
            start_si = best_slot[final_pid]
            
            for tday in pid_set[final_pid]:
                si = start_si
                zero_count = 0
                loop_catcher = 0
                while (n_t2p[si%96] == final_pid or n_t2p[si%96] == 0)\
                 and (zero_count <= 3 or  \
                     tday in d_set[si%96])\
                 and loop_catcher < 96:
                    new_filter[si%96] = 1
                    f_end = max(si,f_end)
                    if tday not in d_set[si%96]:
                        zero_count += 1
                    else:
                        zero_count = 0
                    si = (si+1)
                    loop_catcher += 1
                #Now go backward
                si = start_si - 1
                zero_count = 0
                while (n_t2p[si%96] == final_pid or n_t2p[si%96] == 0)\
                 and (zero_count <= 3 or  \
                     tday in d_set[si%96])\
                 and loop_catcher < 96:
                    new_filter[si%96] = 1
                    f_start = min(si, f_start)
                    if tday not in d_set[si%96]:
                        zero_count += 1
                    else:
                        zero_count = 0
                    si = (si - 1)
                    loop_catcher += 1
    f_end = f_end%96
    f_start = f_start%96
    return f_start, f_end, filter_status,npid,n_t2p,centrality, peak_options

def find_total_elevated(far_rtts, near_rtts, tday, days, eight_days):

    day_slots =  [] 
    d_set = [set() for x in range(96)]  
        #Needed to call compute_filtered_congestion. 
          
    if tday not in days.keys():
        print '{} missing from days',format(tday)
        return 1,0.0 ,d_set
    days[tday]['ftcongestion'] = 0.0
    days[tday]['initial-centrality'] = 1.0
    days[tday]['final-centrality'] = 1.0
    days[tday]['num-slots'] = 0
    
    if days[tday]['revised-asrt'] == 2:
        return 2, 0.0 , d_set
        #in this case, only consequence of call is to fill in variables.   
    
    if days[tday]['pearson_1'] > .7: 
        days[tday]['revised-asrt'] = 4
        days[tday]['rule'] = 6
        return 4,0.0, d_set 
    if days[tday]['fgood_count'] < 30:
        days[tday]['revised-asrt'] = 2
        days[tday]['rule'] = 2
        return 2,0.0, d_set 
    if (days[tday]['fu_bound'] - \
       days[tday]['fl_bound'] < 7):
        days[tday]['revised-asrt'] = 3
        days[tday]['rule'] = 11
        return 3,0.0, d_set 
    if days[tday]['revised-asrt'] == 1:
        return 1, 0.0, d_set
    #for this day to be one of the days correlated
    #it must pass the rules above.            
    iterate = True
    cutoff = days[tday]['fcutoff']
    more_cutoff = 0
    junk = False
    while iterate:
        day_slots = []
        for i in range(96):
            frtt = far_rtts[i]
            nrtt = near_rtts[i]
            if frtt != None and nrtt != None and\
                frtt > days[tday]['fcutoff'] and\
                nrtt < days[tday]['nl_bound'] + max(.5 *\
                (frtt - days[tday]['fl_bound']),10):
                day_slots.append(i)
        centrality, num_gaps  = \
             isjunk(day_slots,days[tday]['fgood_count'])
        if more_cutoff == 0: #first or only time around:
            days[tday]['initial-centrality'] = centrality
        if centrality < .4 or num_gaps > 3:
            junk = True
            more_cutoff += 1
            days[tday]['fcutoff'] = cutoff + more_cutoff
            #print 'day {} {} looks like junk: cen {}, gaps {}cutoff {}'\
               #.format(tday, tday - first_day -3, \
               #centrality, num_gaps, more_cutoff)
            if more_cutoff >= 15:
                iterate = False
        else:
            iterate = False
        
        if iterate == False: #last time around
            days[tday]['final-centrality'] = centrality
            
    tcongestion  = float(len(day_slots))/96.0         
    if len(day_slots) > 10* eight_days:
            #If few elevated days, big congestion unlikely. 
        days[tday]['revised-asrt'] = 1
        days[tday]['rule'] = 33
        print 'Rejecting day {}, {} congested days, day count {}'\
               .format(tday, eight_days,\
                len(day_slots))
        return 1,tcongestion, d_set
    elif days[tday]['final-centrality'] < .2:
        #spikes or horrible jitter. Reject. .2 is temp
        print 'Rejecting day {}, final centrality {}'\
           .format(tday,\
            days[tday]['final-centrality'])
        days[tday]['revised-asrt'] = 6
        days[tday]['rule'] = 34
        return 6,tcongestion, d_set
    days[tday]['ftcongestion'] = tcongestion
    days[tday]['num-slots'] = len(day_slots)
    days[tday]['num-gaps'] = num_gaps 
    for i in day_slots:
        d_set[i].add(tday)  #So can later compute congestion    
    #d_count is the number of elevated days in each slot.
    return 0, tcongestion, d_set #0 means not yet classified.
   
             
def compute_filtered_congestion(f_start, f_end,tday, d_set, total_count):
    if f_start == -1:
        #Should not have been called, but...
        return 0.0
    slot_count = [0 for x in xrange(96)]
    congested_slots = 0    
    zero_count = 0
    loop_catcher = 0
    if f_end < f_start:
        f_end = f_end + 96
    #first do the in-filter range
    for si in range(f_start,f_end+1):
        loop_catcher += 1
        if tday in d_set[si%96]:
            congested_slots += 1
            slot_count[si%96] += 1
    if congested_slots == 0: #no part of this day in the filter
        return 0.0
    #now go forward from end of filter
    do_next = True
    si = f_end + 1
    while do_next:
        if loop_catcher >= 96:
            do_next = False
        elif tday in d_set[si%96]:
            congested_slots += 1
            slot_count[si%96] += 1
            zero_count = 0
        elif zero_count > 3:
            do_next = False
        else:
            zero_count += 1
        loop_catcher += 1
        si += 1
        
    #Now go backward
    si = f_start - 1
    zero_count = 0
    do_next = True
    while do_next:
        if loop_catcher >= 96:
            do_next = False
        elif tday in d_set[si%96]:
            congested_slots += 1
            slot_count[si%96] += 1
            zero_count = 0
        elif zero_count > 3:
            do_next = False
        else:
            zero_count += 1
        loop_catcher += 1
        si += -1
    #ipdb.set_trace() 
    if max(slot_count) > 1:
        print 'Slot count error'
        ipdb.set_trace()
    return float(congested_slots)/float(total_count)
    
def isjunk(d_slots,good_count):
    junk = False
    zero_runs = []
    correction = 96 - good_count
    if len(d_slots) == 0:
        return 1.0, 0
    d_slots.sort()
    begin_run = d_slots[0]
    for si in d_slots[1:]:
        if si < begin_run+3: #two are near
            begin_run = si
        else:
            zero_runs.append(si -begin_run -1)
            begin_run = si
    if d_slots[0] + 95 - d_slots[-1] > 0:
        zero_runs.append(d_slots[0] + 95 - d_slots[-1])
    if len(zero_runs) == 0: #Congested all day
        return 1.0, 0
    max_run = max(zero_runs)
    for i in range(len(zero_runs)):
        if zero_runs[i] == max_run:
            this_correction = min(zero_runs[i], correction)
            zero_runs[i] += -this_correction
            correction += -this_correction
            #approximation will not give precise answer if multiple day outages
            #break your brain
    zeros = sum(zero_runs)
    if zeros > 0:   
        centrality = float(max(zero_runs))/zeros
    else: #no zeros--100% congestion or a mess.
        centrality = 1.0 #hope its valid
    #print 'Junk test', centrality, d_slots, zero_runs
    num_gaps = 0
    for run in zero_runs:
        if run > 3:
            num_gaps +=1  
    if centrality > 1.0:
        ipdb.set_trace()
    return centrality, num_gaps
    
def compute_q(d_count, eight_days,f_start, f_end):
#NOT CURRENTLY USED
    if f_start == -1: #should not be called, but...
        return 0.0
    if eight_days == 0:
        return 0.0,0.0
    if f_end < f_start:
        f_end = f_start + 96
    in_count = []
    for i in range(f_start,f_end + 1):
        in_count.append(d_count[i%96])
    if f_end < f_start:
        temp_end = f_end + 96
    else:
        temp_end = f_end
    filter_count = temp_end - f_start + 1
    q1 = st.mean(in_count)/eight_days
    q2 = float(max(in_count))/eight_days
    '''If every day in the filter contributed to the congestion in each slot,
    this number would be 1'''
    #ipdb.set_trace()
    return q1,q2
    
        
    

           

def findlimits(far_rtts, near_rtts, total_count, nearfar = 'near', ncutoff = 1000):
    bucket=[]
    clipped = []
    okcount = 0
    overruncount = 0
    count = len(far_rtts)
    for i in range(0,550): #offset of 50 so between -50 and 500 ms buckets.
        bucket.append(0)  #make the buckets
    for traceindex in range(0,count):
        frtt = far_rtts[traceindex]
        nrtt = near_rtts[traceindex]
        if nrtt != None and frtt != None\
        and (nearfar == 'near' or nrtt < ncutoff):
            index = int(frtt)
            if index < -50:index= -50  #difs can be negative 
            if index >499: 
                index = 499
                overruncount += 1
            bucket[index+50] = bucket[index+50]+1
            okcount += 1
    if okcount < 10:
        print "Warning: Insufficient good points"
        return okcount, 0, 0, 0
    if overruncount > okcount - 4:
        print "Warning: Data out of range"
        okcount = 0
        return okcount, 0, 0, 0
    
    
    highlimit = 0  #get 0% of the samples--exclude from upper limit
    lowlimit = okcount-(4 * okcount)//count  #get 95% of the samples --exclude from lower limit
    samplecount = 0
    for indexr in range(0,550):
        samplecount = samplecount + bucket[549-indexr]
        if samplecount > highlimit:
            cap = 499-indexr
            highlimit = len(far_rtts)*2  #Don't pass this test more than once
        if samplecount >= lowlimit:
            bottom = 499-indexr
            break
    if okcount > 20:
        drange = cap - bottom
    else:
        drange =  0
    if drange > 5:
        #cutoff = bottom + min(7,int(drange * .5))
        cutoff = bottom + 7
    else:
        cutoff = bottom 
    return  okcount, bottom, cap, cutoff
    
    
def compute_near_congestion(rtts,cutoff, f_start, f_end):
    if f_start == -1: #should not be called, but...
        return 0.0
    denom = float(len(rtts))
    if f_end < f_start:
        f_end = f_start + 96
    total_count = 0
    in_filter_count = 0
    for i in range(len(rtts)):
        rtt = rtts[i]
        if rtt != None and rtt > cutoff:
            total_count += 1
    for i in range(f_start,f_end + 1):
        rtt = rtts[i%96]
        if rtt != None and rtt > cutoff:
            in_filter_count += 1
    return float(total_count)/denom,float(in_filter_count)/denom
    

def compute_far_congestion(frtts,nrtts,fcutoff,nlb, flb, f_start,f_end):
    denom = float(len(frtts))
    if f_start == -1: #should not be called, but...
        return 0.0, 0.0
    if f_end < f_start:
        f_end = f_start + 96
    total_count = 0
    in_filter_count = 0
    for i in range(len(frtts)):
        nrtt = nrtts[i]
        frtt = frtts[i]
        if nrtt != None and frtt != None: 
            if frtt > fcutoff and \
              nrtt < nlb + .8 *(frtt - flb):
                total_count += 1
        
    for i in range(f_start,f_end + 1):
        nrtt = nrtts[i%96]
        frtt = frtts[i%96]
        if nrtt != None and frtt != None: 
            if frtt > fcutoff and \
              nrtt < nlb + .8 *(frtt - flb):
                in_filter_count += 1
    return float(total_count)/denom,float(in_filter_count)/denom             
    
def merge_days(merge_data,group_id,pasn,fasn):
    good_mdays = 0
    day_assertion = []
    #debug = True
    method = 'ddc-m7'
    for tday in merge_data.keys():
        far_count_set = set()
        day_list = merge_data[tday]
        day_list.sort(key = lambda x: x[2], reverse = True) #congestion
        day_list.sort(key = lambda x: x[0], reverse = True) #assertion
        merged_use_total = False
        if len(day_list) > 0:
            good_mdays += 1
        
        is_congestion = False
        congestion_values = []
        summary = 0
        mversion = 0
        good_meas_list = []
        for c in day_list:
            #[asrt,rule,congestion, mon, far, merge_type,
            #int(days[tday]['fgood_count'])]) 
            asrt = c[0]
            rule = c[1]
            congestion = c[2]
            pmon = c[3]
            pfar = c[4]
            mtype = c[5]
            good_count = c[6]
            
            good_meas_list.append(good_count)
            
            if mtype == 1 or mtype == 2:# Don't count CP 
                far_count_set.add(pfar) 
            #NOTE: this code works because the entries are ordered!
            if asrt == 8 or asrt == 9:
                is_congestion = True
                congestion_values.append(congestion)
                summary = 8
                continue
            elif asrt == 5 or asrt == 6 or asrt == 7:
                #if there is an 8, this is odd. Flag it.
                #Most likely near side congestion
                if is_congestion:
                    if c[5] > .05:
                        #error_log.append([far,fasn,c[3], tday,102])
                        summary = 10 #temp value
                    #else summary remains at 8
                else: 
                    summary = 5
                continue
            elif asrt == 4:
                if is_congestion:
                    #both congestion and near side is ok.
                    #Accept the congestion
                    continue
                else:
                    if summary == 0:
                        summary = 4
                    continue
            elif asrt == 3:
                if is_congestion:
                    trial_congestion = st.median_low(congestion_values)
                    if trial_congestion > .1 and good_count == 96:
                        #if only a partial day, may have missed the
                        #congestion. Forget about it...
                        print 'Both congested and uncongested {} {} {} {}'.\
                        format(pfar,fasn, pmon, tday)
                        #error_log.append([far,fasn,c[3], tday,103])
                elif (summary == 5 or summary == 4) and\
                    (good_count > 85 or good_count > max(good_meas_list) -10 ): 
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
            elif asrt <= 2:
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
        #write group to influx
        
        day_assertion.append(make_day_merged\
            ('group_day_data_ddc1', group_id,pasn,fasn,tday,method,\
             summary, 1, c_val, len(far_count_set)))
        #print day_assertion[-1]
        #if good_mdays < 10:
            #print day_assertion[-1]
    if len(day_assertion) > 0:
        success = 24
        #success = metaclient.write_points(day_assertion)
        print 'Write merge group day: Success {}, {} {} count {}'\
        .format(success, fasn, group_id, len(day_assertion))            
           
 

                     
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
        

def close(a,b,d):
    if abs(a-b) <= d:
        return True
    else:
        return False     
        
def main():
    
    validate_days('ddc-m8')

if __name__ == '__main__':
    main()