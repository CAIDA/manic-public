from __future__ import division
#usage python run_daily_averages.py >> supplemental_data/daily_log.txt 2>&1
#gets list of ipmap files to process from hardcoded list below
#list created by running:
#ls rtt_and_loss_data/rtt/book_keeping/*/*/*ipmap* > supplemental_data/list_of_ipmaps.txt

import subprocess
import matplotlib.pyplot as plt
import os
import csv
import sys
import numpy as np

output_dir = "/project/comcast-ping/kabir-plots/loss_data/question_two/"

excluded_asns = [1280, 4837, 4134, 4637, 5511, 2516]
#agg_file = "/project/comcast-ping/kabir-plots/loss_data/supplemental_data/atl2-us_october.csv"
#agg_file = str(sys.argv[1])
#agg_file = '/project/comcast-ping/kabir-plots/loss_data/merged_data/allmonitors_sorted_1.csv'
agg_file = '/project/comcast-ping/kabir-plots/loss_data/merged_data/allmonitors_all_access.csv'
filename = agg_file.split('/')[-1]

def plot_asn_congestion(asn_dict):
    max_x = 40
    max_y = 40
    output_csv = output_dir + filename + '_summary.csv'
    f = open(output_csv,'w+')
    ASType = read_astypes()
    ASName = read_asnames()
    fig = plt.figure(1, figsize=(6, 6))
    ax = fig.add_subplot(111)
    c_first = True
    t_first = True
    e_first = True
    for key in asn_dict:
        s_label = ''
        value = asn_dict[key]
        total_days = value[0]
        days_congested = value[1]
        avg_congestion = value[2]
        asn_type = ASType[key]
        if asn_type == 1: #content provider
            s_color = 'r'
            if c_first:
                s_label = 'Content'
                c_first = False
        elif asn_type == 0: #transit provider
            s_color = 'b'
            if t_first:
                s_label = 'Transit'
                t_first = False
        else: #Enterprise
            s_color = 'g'
            if e_first:
                s_label = 'Enterprise'
                e_first = False
    
        perc_days_congested = 100.000 * float(days_congested) / float(total_days)
        max_x = max(max_x, int(perc_days_congested + 10.0))
        max_y = max(max_y, int(avg_congestion + 10.0))

        f.write( str(key) + ',' + ASName[str(key)] + ',' + str(round(avg_congestion,3)) + ',' \
                + str(round(perc_days_congested)) + ',' + str(days_congested) +','+\
                str(total_days) + '\n' )
        if len(s_label) > 0:

            ax.scatter(perc_days_congested, avg_congestion, color = s_color, alpha = 1, \
                    marker = ".", s = 50, label = s_label)
        else:
            ax.scatter(perc_days_congested, avg_congestion, color = s_color, alpha = 1, \
                    marker = ".", s = 50)
    output = output_dir + filename + '.pdf'
    sys.stderr.write("saving " + output + "\n")
    ax.set_xlabel('Percentage of days congested')
    ax.set_ylabel('Average daily percentage congestion')
    x_title = filename.replace('.csv','') + ' to peers and providers'
    ax.set_title(x_title)
    ax.set_xlim([0,max_x])
    ax.set_ylim([0,max_y])
    #ax.legend(loc=4)
    fig.savefig(output)

def systemCall(parameter):
    os.system(parameter)

def init_dict(asrtn_dict, key):
    asrtn_dict[key] = []
    far_ips = set()
    asrtn_dict[key].append(far_ips)
    return asrtn_dict

def month_to_integer(human_time):
    year = human_time.split('-')[0]
    month = human_time.split('-')[1]
    if year == '2016':
        offset = 0
    elif year == '2017':
        offset = 10
    index = offset + (int(month) % 3) + 1 #integer to plot assertions

def read_ddc(input_file):
    asrtn_dict = {}
    value_list = []
    uptime_list = []
    keys = set()
    neighbor_cong = {}
    with open (input_file,'rb') as f:
        reader = csv.reader(f, delimiter=',')
        for row in reader:
            cong_day = False
            
            key='' #get key
            nasn = row[0]
            asn = row[1]
            if int(asn) in excluded_asns:
                continue
            far_ip = row[2]
            day = row[3]
            asrtn = int(row[5])

            if asrtn != 3 and asrtn != 8: #code isn't "uncongested" or "congested"
                continue
            
            human_time = row[8]            
            key = asn + ',' + nasn
            
            if asrtn == 8 and float(row[6]) >= 0.04: #is this day congested at least 4% of the time?
                cong_day = True
                estimate = float(row[6])
                #add ASN to congestion list
                if asn in neighbor_cong:
                    neighbor_cong[asn].add(nasn)
                else:
                    neighbor_cong[asn] = set()
                    neighbor_cong[asn].add(nasn)

            if key not in keys: #first value in this nasn-asn pair, initialize
                keys.add(key)
                asrtn_dict = init_dict(asrtn_dict, key)
                asrtn_dict[key][0].add(far_ip) #add IP to set of IPs in nasn,asn pair
                if cong_day:
                    asrtn_dict[key].append([far_ip,0,1, [estimate]])
                else:
                    asrtn_dict[key].append([far_ip,1,0, []])

            if key in keys: #there's already data for this nasn,asn (the usual case)
                far_set = asrtn_dict[key][0]
                if far_ip in far_set: #already data for this far_IP, need to find list of data

                    for i in range(len(asrtn_dict[key])):
                        if i == 0:
                            continue #skip first value which is set of IPs
                        
                        if asrtn_dict[key][i][0] == far_ip:
                            current_list = asrtn_dict[key][i]
                            break
                    if cong_day:
                        current_list[2] = current_list[2] + 1 #increase congestion day counter
                        current_list[3].append(estimate) 
                    else:
                        current_list[1] = current_list[1] + 1
                
                else: #no data for this far_ip yet
                    asrtn_dict[key][0].add(far_ip)
                    if cong_day:
                        asrtn_dict[key].append([far_ip,0,1, [estimate]])
                    else:
                        asrtn_dict[key].append([far_ip,1,0, []])

    return asrtn_dict, neighbor_cong

def stat_generation(in_list):
    if len(in_list) == 0:
        return '0.00', '0.00', '0.00', '0.00'

    if len(in_list) == 1:
        element = str(round(in_list[0],2))
        return element, element, element, element

    elif len(in_list) > 1:
        min_in = str(round(100.0*min(in_list),2))
        med = str(round(100.0*np.median(in_list),2))
        mean = str(round(100.0*np.mean(in_list),2))
        max_in = str(round(100.0*max(in_list),2))
        return min_in, med, mean, max_in

def parse_asrtn_dict(asrtn_dict, neighbor_cong, AS):

    output_file = '/project/comcast-ping/kabir-plots/loss_data/merged_data/access_to_all.csv'
    try:
        f = open(output_file, 'w+')
    except EnvironmentError:
        sys.stderr.write('could not open output file')
        exit()
    f.write('Far ASN,Far Name,Near ASN,Near Name,Congested Days, Total days\n')   
    for key in asrtn_dict:
        asn = str(key).split(',')[0]
        try:
            congested_peers = len(neighbor_cong[asn])
        except KeyError:
            continue

        if congested_peers < 2:
            continue #print only ASes congested to two providers or more
        nasn = str(key).split(',')[1]
        number_links = len(asrtn_dict[key][0])
        total_days = 0
        cong_days = 0
        cong_perc_dl = [] #congestion percentage day-links
        cong_days_link = [] #percentage days congested per link
        total_days_link = [] #total days with data per link
        cong_perc_link = [] #avg. congestion percentage per link
        
        try:
            line = asn + ',' + AS[asn] + ',' 
        except KeyError:
            line = asn + ',unknown,'

        try:
            line = line + nasn + ',' + AS[nasn] + ',' #+ str(number_links) + ','
            #skipping number of links for now
        except KeyError:
            line = line + nasn + ',unknown,' #+ str(number_links) + ','
        for i in range(len(asrtn_dict[key])):
            if i == 0:
                continue
            link_cong_days = asrtn_dict[key][i][2]
            link_uncong_days = asrtn_dict[key][i][1]
            link_total_days = link_cong_days + link_uncong_days
            link_perc_days = round(100.0 * link_cong_days / (link_total_days),2)
            link_cong_list = asrtn_dict[key][i][3]

            total_days = total_days + link_cong_days + link_uncong_days
            cong_days = cong_days + link_cong_days
            cong_perc_dl = cong_perc_dl + link_cong_list
            cong_days_link.append(link_perc_days)
            total_days_link.append(link_total_days)
            if len(link_cong_list) > 0:
                cong_perc_link.append(np.mean(link_cong_list))
            else:
                cong_perc_link.append(0.0)
        overall_days_cong = str(round(100.0 * cong_days / total_days,2))
        line = line + str(cong_days) + ',' + str(total_days) + '\n'
        f.write(line)

def read_asnames():

    AS = open('/home/agamerog/plots/ddc/AS-table.txt', 'rU')
    ASdict = {}
    for lines in AS:
        if len(lines) > 0 and lines[0] == 'A':
            ASnumstr = lines.split()[0][2:] #throw away the AS
            AStextlist = lines.split()[1:10]
            AStextlist = " ".join(AStextlist).replace(',','')
            AStextlist = AStextlist[:22]
            #ASdict[ASnumstr] = " ".join(AStextlist).replace(',','')
            ASdict[ASnumstr] = AStextlist
    AS.close()
    return ASdict

def read_astypes():
    AT = open('/data/external/as-rank-ribs/20171001/asclass/20171001.as2types.txt', 'rU')
    ATdict = {}
    for lines in AT:
        if len(lines) > 0 and lines[0] != '#':
            ASnumstr = int(lines.split('|')[0]) #grab ASN
            AStype = str(lines.split('|')[2])
            if 'Transit' in AStype:
                ATdict[ASnumstr] = 0
            elif 'Content' in AStype:
                ATdict[ASnumstr] = 1
            else:
                ATdict[ASnumstr] = 3
    AT.close()
    return ATdict

def main():
    
    AS = read_asnames()
    asrtn_dict = {}
    #read agg congestion inferences, keys
    asrtn_dict, neighbor_cong = read_ddc(agg_file)
    #print asrtn_dict
    
    parse_asrtn_dict(asrtn_dict, neighbor_cong, AS)

    #create dict of [ASNumber]::[ASName, ASType, percentage days congestion, average congestion]
                                    #0      1               
    #asn_dict = create_asn_array(agg_key, agg_values)

    #plot_asn_congestion(asn_dict)
main()
