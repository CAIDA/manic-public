from __future__ import division
#usage python run_daily_averages.py >> supplemental_data/daily_log.txt 2>&1
#gets list of ipmap files to process from hardcoded list below
#list created by running:
#ls rtt_and_loss_data/rtt/book_keeping/*/*/*ipmap* > supplemental_data/list_of_ipmaps.txt

import subprocess
import matplotlib
import matplotlib.pyplot as plt
import os
import csv
import sys
import numpy as np

output_dir = "/project/comcast-ping/kabir-plots/loss_data/question_two/"

#current_nasn = ['6079', '7922', '7018', '7843', '209', '701', '22773', '20115']
current_nasn = ['6079', '7922', '7018', '7843', '209', '701', '22773', '20115']
#comcast, ATT, TWC

plotting_fasns = [6453,15169]
hardcoded_AS_Dict = {'15169':'Google','6453':'Tata','2914':'NTT','2828':'XO','2906':'Netflix','3356':'Level3','4637':'Telstra','1273':'Vodafone','1280':'ISC','5511':'Orange','1299':'Telia','36040':'YouTube','6461':'Zayo','7922':'Comcast','209':'CenturyLink','6079':'RCN','20115':'Charter','7018':'ATT','22773':'Cox','701':'Verizon','7843':'TWC'}
#manually removed 4837, 2516, both transpacific networks where bordermap gets confused
#agg_file = "/project/comcast-ping/kabir-plots/loss_data/supplemental_data/atl2-us_october.csv"
#agg_file = str(sys.argv[1])
#agg_file = '/project/comcast-ping/kabir-plots/loss_data/merged_data/allmonitors_sorted_1.csv'
agg_file = '/project/comcast-ping/kabir-plots/loss_data/merged_data/allmonitors_all_access.csv'
filename = agg_file.split('/')[-1]

def color_choose(asn):
    modifier = ''
    for i in range(len(current_nasn)):
        if current_nasn[i] == asn:
            j = i
            break
    if j == 0: 
            s_color = 'r' + modifier
            marker = 'o'
    elif j == 1: #Decision tree for series colors
            s_color = 'g' + modifier
            marker = '*'
    elif j== 2:
            s_color = 'g' + modifier
            marker = 's'
    elif j == 3:
            s_color = 'k' + modifier
            marker = '8'
    elif j == 4:
            s_color = 'c' + modifier
            marker = 'D'
    elif j == 5:
            s_color = 'm' + modifier
            marker = 'p'
    elif j == 6:
            s_color = 'r' + modifier
            marker = '^'
    elif j == 7: #Decision tree for series colors
            s_color = 'b' + modifier
            marker = '<'
    elif j== 8:
            s_color = 'b' + modifier
            marker = 'v'
    elif j == 9:
            s_color = 'k' + modifier
            marker = '^'
    elif j == 10:
            s_color = 'c' + modifier
            marker = '^'
    elif j == 11:
            s_color = 'm' + modifier
            marker = '^'
    elif j == 12:
            s_color = 'r' + modifier
            marker = 's'
    return s_color, marker

def plot_asn_congestion(asn_dict, ATdict, type_plot, save_bool, current_nasn):
    #dictionary of assertions, dictionary of asn types, which type to plot now (0 transit or 1 content)
    plotted_congestion = 0
    max_x = 21.5
    max_y = 102
    color_counter = 0
    ASType = read_astypes()
    ASName = read_asnames()
    fig = plt.figure(1, figsize=(32, 16))
    #ax = fig.add_subplot(121)
    if type_plot == 0:
        #ax = fig.add_subplot(2, 2, 1) #HERE NO LEGEND
        ax = fig.add_subplot(2, 1, 1)
        net_string = 'Google'
        current_plotting_asn = '15169'
        
    else:
        #ax = fig.add_subplot(2, 2, 3) #HERE NO LEGEND
        ax = fig.add_subplot(2, 1, 2)
        net_string = 'Tata'
        current_plotting_asn = '6453'
    c_first = True
    for k_asn in asn_dict:
        fasn = k_asn.split(',')[0]
        nasn = k_asn.split(',')[1]
        if fasn != current_plotting_asn:
            continue
        c_first = True
        plotx = []
        ploty = []
        s_label = ''
        for key in asn_dict[k_asn]:
            value = asn_dict[k_asn][int(key)]
            int_month = key
            days_congested = value[0]
            congestion_list = value[2]
            ploty.append(100.0*np.mean(congestion_list))
            print '##########################'
            print nasn
            print fasn
            print int_month
            print np.mean(congestion_list)
            print '##########################'
            plotx.append(int_month)
            if c_first:
                try:
                    s_label = hardcoded_AS_Dict[nasn]
                except KeyError:
                    s_label = nasn
                c_first = False
                s_color, marker_s = color_choose(nasn)
                color_counter = color_counter + 1
            #else:
                #ax.plot(int_month, perc_days_congested, 'r-', alpha = 1, \
                #        marker = ".", lw = 2)
        #print s_label
        ax.plot(plotx, ploty, s_color, alpha = 1, \
                    marker = marker_s, markersize = 22, lw = 0, label = s_label)
    output = output_dir + ASName[current_nasn].replace(' ','') + '.pdf'
    sys.stderr.write("saving " + output + "\n")
    ax.set_ylabel('Mean cong. %')
    #ax.set_xlabel('Months since March 2016')
    x_title = net_string
    ax.set_title(x_title)
    ax.set_xlim([-0.5,max_x])
    ax.set_ylim([-2,max_y])
    #ax.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.) #HERE NO LEGEND
    fig.tight_layout()
    if save_bool:
        plt.xticks(np.arange(0,22,2))
        ax.set_xlabel('Month and Year')
        labels = [item.get_text() for item in ax.get_xticklabels()]
        labels[0] = '3/16'
        labels[1] = '5/16'
        labels[2] = '7/16'
        labels[3] = '9/16'
        labels[4] = '11/16'
        labels[5] = '1/17'
        labels[6] = '3/17'
        labels[7] = '5/17'
        labels[8] = '7/17'
        labels[9] = '9/17'
        labels[10] = '11/17'
        #labels[11] = 'February 2017'
        #labels[12] = 'March 2017'
        #labels[13] = 'April 2017'
        #labels[14] = 'May 2017'
        #labels[15] = 'June 2017'
        #labels[16] = 'July 2017'
        #labels[17] = 'August 2017'
        #labels[18] = 'September 2017'
        #labels[19] = 'October 2017'
        #labels[20] = 'November 2017'
        #labels[21] = 'December 2017'
        ax.set_xticklabels(labels)
        #plt.setp(xtickNames, rotation=45, fontsize=8)
        matplotlib.rcParams.update({'font.size': 40})
        plt.rcParams["font.family"] = "Times New Roman"
        fig.tight_layout()
        fig.savefig(output)
        plt.clf()
    elif type_plot == 0:
        plt.xticks(np.arange(0,22,2))
        labels = [item.get_text() for item in ax.get_xticklabels()]
        labels[0] = '3/16'
        labels[1] = '5/16'
        labels[2] = '7/16'
        labels[3] = '9/16'
        labels[4] = '11/16'
        labels[5] = '1/17'
        labels[6] = '3/17'
        labels[7] = '5/17'
        labels[8] = '7/17'
        labels[9] = '9/17'
        labels[10] = '11/17'
        ax.set_xticklabels(labels)
    return plotted_congestion

def systemCall(parameter):
    os.system(parameter)

def init_dict(asrtn_dict, key):
    asrtn_dict[key] = {}
    return asrtn_dict

def month_to_integer(human_time):
    year = human_time.split('-')[0]
    month = human_time.split('-')[1]
    if year == '2016':
        offset = -3
    elif year == '2017':
        offset = 9
    int_month = int(month) + offset
    return int_month

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
            far_ip = row[2]
            day = row[3]
            asrtn = int(row[5])

            if asrtn != 3 and asrtn != 8: #code isn't "uncongested" or "congested"
                continue
            
            human_time = row[8]
            int_month = month_to_integer(human_time)
            key = asn + ',' + nasn
            
            if asrtn == 8 and float(row[6]) >= 0.04: #is this day congested?
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
                 
                if cong_day:
                    asrtn_dict[key][int_month] = [1,1, [estimate]]
                else:
                    asrtn_dict[key][int_month] = [0,1, []]

            if key in keys: #there's already data for this nasn,asn (the usual case)
                try:
                    current_list = asrtn_dict[key][int_month]

                    if cong_day:
                        current_list[0] = current_list[0] + 1 #increase congestion day counter
                        current_list[2].append(estimate)
                    current_list[1] = current_list[1] + 1 #increase total days counter

                except KeyError:
                    if cong_day:
                        asrtn_dict[key][int_month] = [1,1, [estimate]]
                    else:
                        asrtn_dict[key][int_month] = [0,1, []]

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

    output_file = '/project/comcast-ping/kabir-plots/loss_data/merged_data/parsed_assertions.csv'
    try:
        f = open(output_file, 'w+')
    except EnvironmentError:
        sys.stderr.write('could not open output file')
        exit()
    f.write('FASN,FNAME,NASN,NNAME,#Day-Links,%Congested Days,Median Congestion,Mean Congestion\n')   
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
        line = line + str(total_days) + ',' 
        overall_days_cong = str(round(100.0 * cong_days / total_days,2))
        line = line + overall_days_cong + ','
        minimum, median, mean, maximum = stat_generation(cong_perc_dl)
        line = line + median + ',' + mean + '\n'
        ''' #these below are the per-link metrics which we're ignoring for now...
        #When/if I readd will have to make diff stats function for percentages
        #given that assertions have fractions
        line = line + minimum + ',' + median + ',' + mean + ',' + maximum + ',' #day-links
        minimum, median, mean, maximum = stat_generation(cong_perc_link)
        line = line + minimum + ',' + median + ',' + mean + ',' + maximum + ',' #percentage cong links
        minimum, median, mean, maximum = stat_generation(cong_days_link)
        line = line + minimum + ',' + median + ',' + mean + ',' + maximum + ',' #cong days per link
        minimum, median, mean, maximum = stat_generation(total_days_link) # total days per link
        line = line + minimum + ',' + median + ',' + mean + ',' + maximum + '\n'
        '''
        f.write(line)

def read_asnames():

    AS = open('/home/agamerog/plots/ddc/AS-table.txt', 'rU')
    ASdict = {}
    for lines in AS:
        if len(lines) > 0 and lines[0] == 'A':
            ASnumstr = lines.split()[0][2:] #throw away the AS
            AStextlist = lines.split()[1:10]
            AStextlist = " ".join(AStextlist).replace(',','')
            AStextlist = AStextlist[:18]
            #ASdict[ASnumstr] = " ".join(AStextlist).replace(',','')
            ASdict[ASnumstr] = AStextlist
    AS.close()
    return ASdict

def read_astypes():
    AT = open('../data/20171001.as2types.txt', 'rU')
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
    AT = read_astypes()

    asrtn_dict = {}
    #read agg congestion inferences, keys
    asrtn_dict, neighbor_cong = read_ddc(agg_file)
    hard_fasn = '15169'

    temp1 = plot_asn_congestion(asrtn_dict, AT, 0, False, hard_fasn)

    hard_fasn = '6453'
    temp2 = plot_asn_congestion(asrtn_dict, AT, 1, True, hard_fasn)


main()
