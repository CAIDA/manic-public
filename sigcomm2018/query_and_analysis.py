from __future__ import division
import csv
import os

excluded_asns = set([1280, 4837, 4134, 4637, 5511, 2516, 4766, 3215, 577, \
        3491, 680, 3320, 286, 12956, 9929,\
        10026, 17676, 3786, 38861, 4589, 6762, 6830, 7474, 7473, 812, 852, 6327, 5400, 8881, 7738, 3303])

month_to_first_day = {\
        4: 17257,\
        5: 17287,\
        6: 17318,\
        7: 17348,\
        8: 17379,\
        9: 17410,\
        10: 17440,\
        11: 17471,\
        12: 17501}

month_to_last_day = {\
        4: 17286,\
        5: 17317,\
        6: 17347,\
        7: 17378,\
        8: 17409,\
        9: 17439,\
        10: 17470,\
        11: 17500,\
        12: 17531}
command = "python /project/comcast-ping/kabir-plots/loss_data/querying_ddc_assertions_loss.py "
log = " >> log.txt 2>&1 "

def read_summary():
    filename = 'filtered_binomial_withdataonly_nointernational.csv'
    with open (filename, 'rb+') as h:
        reader = csv.reader(h, delimiter=',')
        already_parsed = set()
        farp_greater_point_zerofive = 0
        bottom_row = 0
        top_row = 0
        mid_row = 0
        overall = 0
        header = True
        dups = 0
        allrows = 0
        bed = 0
        nodata = 0
        internationals = 0
        new_internationals = set()

        for row in reader:
                if header:
                    header = False #skip first line
                    continue
                allrows = allrows + 1
                monitor = row[0]
                if monitor == 'bed-us':
                    bed = bed + 1
                    continue
                asn = int(row[2])
                if asn in excluded_asns:
                    internationals = internationals + 1
                    new_internationals.add(asn)
                    #print asn
                    continue
                farip = row[3]
                if row[10] == '-1' or row[11] == '-1' or row[12] == '-1' or row[13] == '-1':
                    nodata = nodata + 1
                    continue
                far_loss_rate = row[10] + '-' + row[11]
                far_ip = row[3]
                near_ip = row[4]
                farp = float(row[5])
                nearp = float(row[6])
                fardiff = float(row[7])
                #neardiff = float(row[8])
                overall = overall + 1
                neardiff = (float(row[10]) - float(row[12]))
                    #print "problem"
                month = int(row[14].split('_')[0])
                first_day = month_to_first_day[month]
                last_day = month_to_last_day[month]
                if farp >= 0.05:
                    farp_greater_point_zerofive = farp_greater_point_zerofive + 1
                    code = "text" + ',' + near_ip + ',' + far_loss_rate
                elif fardiff < 0:
                    bottom_row = bottom_row + 1
                    code = "bottom" + ',' + near_ip + ',' + far_loss_rate
                elif farp < 0.05 and fardiff >= 0.0 and nearp < 0.05 and neardiff >= 0.0:
                    top_row = top_row + 1
                    code = "top" + ',' + near_ip + ',' + far_loss_rate
                elif farp < 0.05 and fardiff >= 0:
                    mid_row = mid_row + 1
                    code = "middle" + ',' + near_ip + ',' + far_loss_rate
                checking = "bottom" + ',' + near_ip + ',' + far_loss_rate
                if code == checking:
                    command_os = command + monitor + ' ' + str(asn) + ' ' + far_ip + ' ' + str(first_day) + ' ' + str(last_day) + ' ' + code + log
                #print command_os
                    os.system(command_os)
    print "number of rows"
    print allrows
    print "no data discarded "
    print nodata
    print "bed-us discarded"
    print bed
    print "international far ASN discarded - previously missed: " + str(new_internationals)
    print internationals
    print "With data"
    print overall
    print "no significance far-end test"
    print farp_greater_point_zerofive
    print "total in table"
    table = top_row + mid_row + bottom_row
    print str(table)
    print "top row and percentage"
    print str(top_row) + ' ' + str(round(100.0*float(top_row)/float(table),2))
    print "mid row and percentage"
    print str(mid_row) + ' ' + str(round(100.0*float(mid_row)/float(table),2))
    print "bottom row and percentage"
    print str(bottom_row) + ' ' + str(round(100.0*float(bottom_row)/float(table),2))


def main():

    read_summary()

main()
