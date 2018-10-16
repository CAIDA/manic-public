#usage: 
#python oneoff_loss_plotter.py "/project/comcast-ping/kabir-plots/loss_data/rtt_and_loss_data/rtt/levelshift/dca2-us/12_01_2017_12_31_2017/dca2-us.15169.12_01_2017.12_31_2017.96.236.104.66.ts ddc_periods/dca2-us.96.236.104.66.win3.txt /project/comcast-ping/kabir-plots/loss_data/rtt_and_loss_data/rtt/levelshift/dca2-us/12_01_2017_12_31_2017/dca2-us.15169.12_01_2017.12_31_2017.96.236.104.66.near.ts /project/comcast-ping/kabir-plots/loss_data/rtt_and_loss_data/loss/dca2-us/12_01_2017_12_31_2017/96.236.104.66.ts /project/comcast-ping/kabir-plots/loss_data/rtt_and_loss_data/loss/dca2-us/12_01_2017_12_31_2017/140.222.236.137.ts" success "far cong mean = 0.509| far uncong mean = 0.142| near cong mean = 0.003| near uncong mean = 0.001| far cong vs. far uncong p = 0.0| far cong vs. near cong p = 0.0" "GOOGLE - Google Inc."
#.ts and .out respectively
#Plots time-series files and filtered congestion windows. Used to calibrate levelshift -B and -L parameters
#outputs plot with first filename and .out.png extension
import csv , matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
from pylab import rcParams, figure, axes, pie, title, show
import sys
import os
import datetime as dt
import matplotlib.dates as mdate
from scipy import optimize, stats
from sys import float_info
import datetime
import time
import math

matplotlib.rcParams.update({'font.size': 24})
plt.rcParams["font.family"] = "Times New Roman"
plt.rcParams['text.usetex'] = True

year = 2017
first_month = 12
second_month = 12
first_day = 7
second_day = first_day+2

year = 2017
#first_month = 12
#second_month = 12
#first_day = 3
#second_day = 14

def proportion_confint(count, nobs): #find the 95% confidence interval using the binomial proportion
        alpha=0.05
        q_ = count * 1. / nobs
        alpha_2 = 0.5 * alpha

# inverting the binomial test
        def func(qi):
                return stats.binom_test(q_ * nobs, nobs, p=qi) - alpha
                        #Look for conf interval of count that will give you a 0.05 chance
                        #of observing that outcome with a count/nobs biased coin
        if count == 0:
                ci_low = 0
        else:
                ci_low = optimize.brentq(func, float_info.min, q_)#find inverse of funciton between 0 and observed
                #value. Interval for inversion starts at float_info.min
        if count == nobs:
                ci_upp = 1
        else:
                ci_upp = optimize.brentq(func, q_, 1. - float_info.epsilon)#find inverse of function between
                #observed value and 1 (well, 1-float_info.epsilon, which is very close to 0)

        return ([ci_low, ci_upp])
        #returning values that can be already plotted

def central_limit(count,nobs):
    z = 1.96
    p = float(count)/float(nobs)
    interval = z * math.sqrt(p * (1 - p) / nobs)
    return ([interval, interval])

def is_in_window(timestamp, starts, ends):
    if timestamp <= ends and timestamp >= starts:
        return True
    return False

def filter_minimums(data, first_second, last_second):

    #compute uptime of both near- and far-side raw rtt data within window limits
    filtering_window = 900
    comparer_buffer = 0
    uptime = 0

    rtts = []
    output_plotx = [] #store current rtt values
    output_ploty = []
    start_time = int(int(data[0].split(' ')[0]) / filtering_window) * filtering_window
    end_time = int(int(data[-1].split(' ')[0]) / filtering_window) * filtering_window

    # break ARTIFICIAL END TIME HERE
    start_time = int(first_second / filtering_window) * filtering_window
    end_time = int(last_second /filtering_window) * filtering_window
#    first_second = start_time
#    last_second = end_time

    number_bins = int((end_time - start_time) / filtering_window)
    mins = [0] * number_bins
    #print "number_bins = " + str(number_bins)
    
    for times in range(len(mins)):
        #THIS LINE WILL ALSO HAVE TO GOA
        index = start_time + times * filtering_window
        if index < first_second:
            continue
        if index > last_second:
            break
        output_plotx.append(mdate.epoch2num(start_time + times * filtering_window))

    

    for line in range(len(data)):
        comparer = int(data[line].split(' ')[0])
        if comparer < first_second:
            continue
        if comparer > last_second:
            break

        time_loop = int(int(data[line].split(' ')[0]) / filtering_window) * filtering_window #get the quotient of the start time (5 min bin)
        #ignore repeated measurements for the same 5-minute cycle
        if comparer_buffer == comparer:
                continue
        #print "CP1"
        comparer_buffer = comparer
        j = 0
        while j < number_bins:

            lower = start_time + j*filtering_window
            upper = lower + filtering_window
            if (comparer <= upper) and (comparer >= lower ):
                if mins[j] == 0:
                    mins[j] = int(data[line].split(' ')[1])
                else:
                    temp = int(data[line].split(' ')[1])
                    mins[j] = min(mins[j], temp)
            j = j + 1
    
    return (mins, output_plotx)
#number_files = len(sys.argv) - 1
#Plots 1-6 time series files, latency vs. time

def convert_to_ht(unix_time):
    input_int = float(unix_time)
    return str(time.strftime('%Y-%m-%d %H:%M', time.gmtime(input_int)))

def start_end_second():
    far_loss = str(sys.argv[1]).split(' ')[0]
    try:
        os.system('gunzip ' + far_loss)
    except:
        print 'file not gzipped ' + far_loss
    with open (far_loss,'rb') as f:
        data = f.readlines()
        first_second = int(data[0].split(' ')[0]) 
        last_second = int(data[-1].split(' ')[0])  #last packet loss day + 2 days
        return first_second, last_second

def main():
    first_second, last_second = start_end_second()
    intervals = [] #used to separate packet loss between congested and uncongested periods
    raw_ts_far = []
    raw_ts_near = []

    path = ''
    x_label = 'UTC Time'
    y_label = 'RTT 15-min \nminimum (ms)'
    yaxislow = 10
    yaxishigh = 90
    in_files = str(sys.argv[1])
    number_files = in_files.count(' ') + 1
    packet_high = 5
    packet_low = -0.1
    msize = 12
    alfa = 1
    #first_second = 0
#    last_second = 1494201600 + int(5.75* 86400) #end time x axis
    if (number_files == 0):
            sys.stderr.write('No files were provided\n') 
    elif (number_files > 5):
            sys.stderr.write('more than 5 files provided. Will plot first 2. files provided:')
            sys.stderr.write(str(sys.argv[1]))
            number_files = 5

    #Read all filenames provided
    for j in range(number_files): 
            #print "j = " + str(j)
            plotx = []
            ploty = []
            #Get information about file from inputs
            filename = in_files.split(' ')[j]
            
            #series labels
            if j == 0: 
                    out_png = filename.split('/')[-1]
                    s_label = 'Far RTT'
                    month = filename.split('/')[-1].split('.')[2]
                    monitor = filename.split('/')[-1].split('.')[0]
                    #create output directory
                    output_dir = "/project/comcast-ping/kabir-plots/loss_data/supplemental_data/ddc/ndt"
                    #dir_creation = "mkdir -p " + output_dir
                    #raw_plot_data = output_dir + '/' + in_files.split(' ')[0].split('/')[-1] + '.txt'
                    #os.system(dir_creation)
                    #raw = open (raw_plot_data, 'w+')
                    #sys.stderr.write('opened ' +  + ' for writing \n')
            elif j == 1:
                    s_label = 'congestion periods'
            elif j == 2:
                    s_label = 'Near RTT'
            elif j == 3:
                    s_label = 'Far Loss'
            else:
                    s_label = 'Near Loss'

            #file_path = path + monitor + '/' + month + '/'
            file_path = ''
            filename = file_path + in_files.split(' ')[j]
            
            if j == 1 or j == 3 or j == 4:
                f = open(filename, 'rb')#import file
                g = open(filename, 'rb')#import fi
            
            if j == 0 or j == 2: #The time series files
                command = "gunzip -f " + filename
                os.system(command)
                f = open(filename, 'rb')#import file1
                g = open(filename, 'rb')#import fi
                data = g.readlines()
                ploty, plotx = filter_minimums(data, first_second, last_second)
                #print plotx[0]
                #print plotx[-1]
                #print "y values"
                #print ploty[0]
                #print ploty[-1]
                command = "gzip -f " + filename
                os.system(command)
            elif j > 2:
                plotx = []
                ploty = []

            sys.stderr.write('reading time series file %s\n' % filename)
            
            if j == 0: #space-separated file
                    reader = csv.reader(f, delimiter=' ') #read file into variable reader
                    fig = plt.figure(1, figsize=(12, 9))
            else: 
                    reader = csv.reader(f, delimiter=' ')
            #Read values from file
            for row in reader: 
                    if float(row[0]) < first_second:
                            continue
                    if float(row[0]) > last_second:
                            break
                    secs = mdate.epoch2num(float(row[0]))
                    
                    if j == 1:
                            secs2 = mdate.epoch2num(float(row[1]))
                            #plt.axvline(x=secs, color='g', alpha = alfa)
                            #plt.axvline(x=secs2, color='r', alpha = alfa)
                            plt.axvspan(secs, secs2, facecolor='gray', alpha=0.5)
                            intervals.append([int(row[0]), int(row[1])]) #append intervals to compute loss

                    elif j == 2: #near ts
                            secs3 = mdate.epoch2num(float(row[0]))
                            y = float(row[1])
                            #plotx.append(secs3)
                            #ploty.append(y)                            
                    elif j == 3: #far loss

                            secs3 = mdate.epoch2num(float(row[0]))
                            y = float(row[1])
                            raw_ts_far.append(int(row[0]))
                            plotx.append(secs3)
                            ploty.append(y)
                    elif j == 4: #near loss
                            secs3 = mdate.epoch2num(float(row[0]))
                            y = float(row[1]) #upload throughput
                            raw_ts_near.append(int(row[0]))
                            plotx.append(secs3)
                            ploty.append(y)
            if j == 0: 
                    #fig = plt.figure(1, figsize=(50, 6))
                    ax = fig.add_subplot(211)
                    title = in_files.split(' ')[0].split("/")[-1]
                    title = title + '\n' + sys.argv[3] + ' ' + sys.argv[4]

                    #ax.set_title(title)
                    s_color = 'b-'
                    y_far_max = np.percentile(ploty, 98.0) 
                    y_far_min = min(ploty)

                    #bigger marker for levelshift
                    ax.plot_date(plotx, ploty, s_color, alpha = alfa, \
                    marker = ".", markersize = msize,  label = s_label, lw=0)
                    ax.set_xlabel(x_label)
                    ax.set_ylabel(y_label)
                    #ax.set_ylim([yaxislow, yaxishigh])
                    ax.autoscale_view()
                    #artificial limit below
                    ax.set_xlim([datetime.datetime(year, first_month, first_day, 6), datetime.datetime(year, second_month, first_day+3, 6)])    
                    date_fmt = '%m-%d-%y'

                    # Use a DateFormatter to set the data to the correct format
                    date_formatter = mdate.DateFormatter(date_fmt)
                    ax.xaxis.set_major_formatter(date_formatter)
                    
            if j == 2:
                    s_color = 'r-'
                    y_near_max = np.percentile(ploty, 98.0)
                    y_near_min = min(ploty)
                    yaxislow = int(min(y_near_min, y_far_min)) - 5
                    yaxislow = max(yaxislow, 0)
                    ax.plot_date(plotx, ploty, s_color, alpha = alfa, \
                        marker = ".", markersize = msize,  label = s_label, lw=0)
                    yaxishigh = int(max(y_far_max, y_near_max)) + 20
                    yaxishigh = 90
                    ax.set_ylim([yaxislow, yaxishigh])
                    ax.set_xlim([datetime.datetime(year, first_month, first_day,6), datetime.datetime(year, second_month, (first_day+3), 6)])
                    ax.legend(fontsize=15, framealpha=0.8)
            if j == 3:

                    ax = fig.add_subplot(212)
                    s_color = 'k-'
                    ax.plot_date(plotx, ploty, s_color, alpha = alfa, \
                        marker = ".", markersize = msize,  label = s_label, lw=0)
                    ax.set_xlabel(x_label)
                    ax.set_ylabel("Loss rate \n(5-min average)")
                    #yaxishigh = packet_high
                    far_loss_max = np.percentile(ploty, 98.0)
                    #yaxishigh = y_loss_max
                    yaxislow = packet_low
                    #ax.set_ylim([yaxislow, yaxishigh])
                    ax.autoscale_view()
                    # Choose your xtick format string
                    date_fmt = '%H:%M'
                    #date_fmt = '%m-%d-%y'
                    # Use a DateFormatter to set the data to the correct format
                    date_formatter = mdate.DateFormatter(date_fmt)
                    ax.xaxis.set_major_formatter(date_formatter)
                    ax.set_xlim([datetime.datetime(year, first_month, first_day, 6), datetime.datetime(year, second_month, first_day+3, 6)])    
                    
                    for k in range(len(intervals)):
                        secs = mdate.epoch2num(float(intervals[k][0]))
                        secs2 = mdate.epoch2num(float(intervals[k][1]))
                        #plt.axvline(x=secs, color='g', alpha = alfa)
                        #plt.axvline(x=secs2, color='r', alpha = alfa)
                        plt.axvspan(secs, secs2, facecolor='gray', alpha=0.5)
                    '''
                        string = 'far'
                        plt.axvline(x=mdate.epoch2num(intervals[k][0]), color='g')
                        plt.axvline(x=mdate.epoch2num(intervals[k][1]), color='r')
                        this_period_lost = 0
                        this_period_sent = 0
                        this_period_loss = 0
                        next_period_lost = 0
                        next_period_sent = 0
                        next_period_loss = 0
                        next_interval = []
                        
                        for l in range(len(raw_ts_far)):
                                        
                            if is_in_window(raw_ts_far[l], intervals[k][0], intervals[k][1]):
                                this_period_lost = this_period_lost + ploty[l][0]
                                this_period_sent = this_period_sent + ploty[l][1]
                            if k < (len(intervals)-1): #all but last congestion interval
                                if is_in_window(raw_ts_far[l], intervals[k][1], intervals[k+1][0]):
                                        next_period_lost = next_period_lost + ploty[l][0]
                                        next_period_sent = next_period_sent + ploty[l][1]

                        string = string + ',' + convert_to_ht(intervals[k][0])#save interv begin/end
                        string = string + ',' + convert_to_ht(intervals[k][1])
                        string = string + ',' + str(this_period_lost)
                        string = string + ',' + str(this_period_sent)
                        if this_period_sent > 0 and this_period_lost > 0:
                            this_period_loss = 100*float(this_period_lost) / float(this_period_sent)
                            this_central_interval = central_limit(this_period_lost, this_period_sent)
                            this_interval = proportion_confint(this_period_lost, this_period_sent)
                        else: 
                            this_period_loss = 0.0
                            this_central_interval = [0.0, 0.0]
                            this_interval = [0.0, 0.0]
                        string = string + ',' + str(round(this_period_loss,3))
                        
                        upper = 100 * this_interval[1] + this_period_loss
                        lower = -100. * this_interval[0] + this_period_loss
                        #print this_interval
                        string = string + ',' + str(round(upper,3))
                        string = string + ',' + str(round(lower,3))
                         #save central limit intervals for forensics
                        string = string + ',' + \
                                str(round( 100*(this_central_interval[1]) + this_period_loss ,3))
                        string = string + ',' + \
                                str(round(( -100.*this_central_interval[0] + this_period_loss ),3))
                        this_x = mdate.epoch2num(float(intervals[k][0]+ 0.5*(intervals[k][1]-intervals[k][0])))
                        raw.write(string + '\n')
                        if k < (len(intervals)-1):
                                #Save next interval info
                                string = 'far'
                                string = string + ',' + convert_to_ht(intervals[k][1])
                                string = string + ',' + convert_to_ht(intervals[k+1][0])
                                string = string + ',' + str(next_period_lost)
                                string = string + ',' + str(next_period_sent)
                                if next_period_sent > 0 and next_period_lost > 0:
                                    next_period_loss = 100*float(next_period_lost) \
                                        / float(next_period_sent)
                                    next_central = central_limit(next_period_lost, next_period_sent)
                                    next_interval = proportion_confint(next_period_lost, next_period_sent)
                                else:
                                    next_period_loss = 0.0
                                    next_central = [0.0, 0.0]
                                    next_interval = [0.0, 0.0]
                                string = string + ',' + str(round(next_period_loss,3))
                                upper_n = 100 * next_interval[1] + next_period_loss
                                lower_n = -100. * next_interval[0] + next_period_loss
                                string = string + ',' + str(round(upper_n,3))
                                string = string + ',' + str(round(lower_n,3))
                                upper_n_central = 100 * next_central[1] + next_period_loss
                                lower_n_central = -100. * next_central[0] + next_period_loss
                                string = string + ',' + str(round(upper_n_central,3))
                                string = string + ',' + str(round(lower_n_central,3))
                                raw.write(string + '\n')
                        #print this_interval
                                next_x = mdate.epoch2num(float(intervals[k][1]+ 0.5*(intervals[k+1][0] - intervals[k][1])))
                        if k == 0:
                                ax.plot_date(this_x, this_period_loss, \
                                        color = s_color, alpha = 1, \
                                        marker = "^", markersize = msize,  label = s_label)
                                label_s = 'far conf. interval'
                                ax.plot_date([this_x,this_x], [lower, upper], \
                                color = s_color, alpha = 1, \
                                marker = "+", markersize = msize, label = label_s)
                        else:
                                ax.plot_date(this_x, this_period_loss, \
                                        color = s_color, alpha = 1, \
                                        marker = "^", markersize = msize)                     
                                ax.plot_date([this_x,this_x], [lower, upper], \
                                color = s_color, alpha = 1, \
                                marker = "+", markersize = msize)
                        if k < len(intervals)-1:
                                ax.plot_date([next_x,next_x], [lower_n, upper_n], \
                                        color = s_color, alpha = 1, \
                                        marker = "+", markersize = msize)
                                ax.plot_date(next_x, next_period_loss, \
                                        color = s_color, alpha = 1, \
                                        marker = "^", markersize = msize)

                    #plot triangle and bars at 1/3 of distance between beginning and end of interval
                    ax.set_xlim([datetime.date(year, month_p, first_day), datetime.date(year, month_p, last_day)])
                    '''
            if j == 4:
                    s_color = 'g-'
                    ax.plot_date(plotx, ploty, s_color, alpha = alfa, \
                                                    marker = ".", markersize = msize,  label = s_label, lw=0)
                    near_loss_max = np.percentile(ploty, 98.0)
                    yaxishigh = max(far_loss_max, near_loss_max)
                    yaxislow = -0.1
                    yaxishigh = 4
                    ax.set_ylim([yaxislow, yaxishigh])
                    '''
                    for k in range(len(intervals)):
                        this_period_lost = 0
                        this_period_sent = 0
                        this_period_loss = 0
                        next_period_lost = 0
                        next_period_sent = 0
                        next_period_loss = 0
                        next_interval = []
                        for l in range(len(raw_ts_near)):
                                        
                            if is_in_window(raw_ts_near[l], intervals[k][0], intervals[k][1]):
                                this_period_lost = this_period_lost + ploty[l][0]
                                this_period_sent = this_period_sent + ploty[l][1]
                            if k < (len(intervals)-1):
                                if is_in_window(raw_ts_near[l], intervals[k][1], intervals[k+1][0]):
                                        next_period_lost = next_period_lost + ploty[l][0]
                                        next_period_sent = next_period_sent + ploty[l][1]

                        string = 'near'
                        if this_period_sent > 0 and this_period_lost > 0:
                            this_period_loss = 100*float(this_period_lost) / float(this_period_sent)
                            this_central_interval = central_limit(this_period_lost, this_period_sent)
                            this_interval = proportion_confint(this_period_lost, this_period_sent)
                        else:
                            this_period_loss = 0.0
                            this_central_interval = [0.0, 0.0]
                            this_interval = [0.0, 0.0]
                        upper = 100 * (this_interval[1]) + this_period_loss
                        lower =  (-100. * this_interval[0]) + this_period_loss
                        string = string + ',' + convert_to_ht(intervals[k][0])#save interv begin/end
                        string = string + ',' + convert_to_ht(intervals[k][1])
                        string = string + ',' + str(this_period_lost)
                        string = string + ',' + str(this_period_sent)
                        string = string + ',' + str(round(this_period_loss,3))
                        string = string + ',' + str(round(upper,3))
                        string = string + ',' + str(round(lower,3))
                        string = string + ',' + \
                            str(round( 100*(this_central_interval[1] + this_period_loss) ,3))
                        string = string + ',' + \
                            str(round((-100.*this_central_interval[0] + this_period_loss),3))
                        raw.write(string + '\n')
                        #print this_interval
                        this_x = mdate.epoch2num(float(intervals[k][0]+ 0.5*(intervals[k][1]-intervals[k][0])))
                        if k < (len(intervals)-1):   
                                string = 'near'
                                if next_period_sent > 0 and next_period_lost > 0:
                                    next_period_loss = 100*float(next_period_lost) / float(next_period_sent)
                                    next_interval = proportion_confint(next_period_lost, next_period_sent)
                                    next_central_interval = central_limit(next_period_lost, next_period_sent)
                                else:
                                    next_period_loss = 0.0
                                    next_central_interval = [0.0, 0.0]
                                    next_interval = [0.0, 0.0]
                                upper_n = 100 * next_interval[1] + next_period_loss
                                lower_n = -100. * next_interval[0] + next_period_loss
                                upper_central_n = 100 * next_central_interval[1] + next_period_loss
                                lower_central_n = -100. * next_central_interval[0] + next_period_loss

                                string = string + ',' + convert_to_ht(intervals[k][1])#save interv begin/end
                                string = string + ',' + convert_to_ht(intervals[k+1][0])
                                string = string + ',' + str(next_period_lost)
                                string = string + ',' + str(next_period_sent)
                                string = string + ',' + str(round(next_period_loss,3))
                                string = string + ',' + str(round(upper_n,3))
                                string = string + ',' + str(round(lower_n,3))
                                string = string + ',' + str(round(upper_central_n,3))
                                string = string + ',' + str(round(lower_central_n,3))
                                raw.write(string + '\n')
                                #print this_interval
                                next_x = mdate.epoch2num(float(intervals[k][1]+ 0.5*(intervals[k+1][0] - intervals[k][1])))
                        if k == 0:
                                ax.plot_date(this_x, this_period_loss, \
                                        color = s_color, alpha = 1, \
                                        marker = "^", markersize = msize,  label = s_label)
                                label_s = 'near conf. interval'
                                ax.plot_date([this_x,this_x], [lower, upper], \
                                color = s_color, alpha = 1, \
                                marker = "+", markersize = msize, label = label_s)
                        else:
                                ax.plot_date(this_x, this_period_loss, \
                                        color = s_color, alpha = 1, \
                                        marker = "^", markersize = msize)                     
                                ax.plot_date([this_x,this_x], [lower, upper], \
                                color = s_color, alpha = 1, \
                                marker = "+", markersize = msize)
                        if k < (len(intervals)-1):
                                ax.plot_date([next_x,next_x], [lower_n, upper_n], \
                                        color = s_color, alpha = 1, \
                                        marker = "+", markersize = msize)
                                ax.plot_date(next_x, next_period_loss, \
                                        color = s_color, alpha = 1, \
                                        marker = "^", markersize = msize)
                    '''
                    #plot triangle and bars at 1/3 of distance between beginning and end of interval
                    #ARTIFICIAL LIMIT BELOW
                    ax.set_xlim([datetime.datetime(year, first_month, first_day, 6), datetime.datetime(year, second_month, first_day+3, 6)])
                    plt.axhline(y=0, color='gray')
                    ax.legend(fontsize=15, framealpha=0.8)
    fig.autofmt_xdate()
    fig.tight_layout()
    output_dir = "/project/comcast-ping/kabir-plots/loss_data/supplemental_data/new_loss_charts"#+ monitor + '/' + month
    dir_creation = "mkdir -p " + output_dir
    os.system(dir_creation)
    code = str(sys.argv[2])
    output = output_dir + '/' + code + '.' + out_png + '.pdf'
    sys.stderr.write("saving " + output)
    fig.savefig(output, transparent=True)
                            
main()
