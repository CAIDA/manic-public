from __future__ import division
import csv
excluded_asns = set([1280, 4837, 4134, 4637, 5511, 2516, 4766, 3215, 577, \
        3491, 680, 3320, 286, 12956, 9929,\
        10026, 17676, 3786, 38861, 4589, 6762, 6830, 7474, 7473, 812, 852, 6327, 5400, 8881, 7738, 3303])
monitor_dict = {'atl2-us':7922, \
        'wbu2-us':7922, \
        'pao-us':7922, \
        'mry-us':7922, \
        'gai-us':7922, \
        'bos5-us':7922, \
        'bed-us':7922, \
        'bna-us':7922, \
        'sts-us':7922, \
        'sea2-us':7922, \
        'lke-us':7922, \
        'iad2-us':7922, \
        'cld2-us':7018, \
        'tul2-us':7018, \
        'oak3-us':7018, \
        'oak5-us':7018, \
        'san7-us':7018, \
        'san6-us':7018, \
        'atl3-us':7018, \
        'msn3-us':7018, \
        'lwc2-us':7018, \
        'san4-us':7843, \
        'lex-us':7843, \
        'ith-us':7843, 'igx-us':5650,'dal-us':5650,'avl-us':20115,'msn2-us':20115,'rno2-us':20115,'bed2-us':701,'mnz-us':701,'bos2-us':701,'dca2-us':701,'bos6-us':701,'dca3-us':701,'msy-us':22773,'tul-us':22773,'san2-us':22773,'tul3-us':22773,'bed3-us':6079,'aza-us':209,'bfi-us':209,'las-us':209 }

def read_summary():
    filename = 'newtestlosssig.csv'
    with open (filename, 'rb+') as h:
        reader = csv.reader(h, delimiter=',')
        skipped = 0
        nodata = 0
        finaltop = 0
        finalmid = 0
        finalbottom = 0
        finaltext = 0
        accesses = set()
        links = set()
        transits = set()
        lossrates = []
        counter = 0
        weirdrates = []
        for row in reader:
                #if header:
                #    header = False #skip first line
                #    continue
                mon = row[0]
                congested_days = int(row[4])
                uncongested_days = int(row[5])
                code = row[7]
                if congested_days == 0:
                    skipped = skipped + 1
                    continue
                if uncongested_days == 0 and congested_days == 0:
                    nodata = nodata + 1
                    continue
                accesses.add(monitor_dict[mon])
                transits.add(row[1]) #far ASN
                links.add(row[2])
                #lossrates.append(float(row[-1]))
                if code == 'top':
                    if float(row[-1]) > 50.0:
                        counter = counter + 1
                        lossrates.append(float(row[-1]))
                    finaltop = finaltop+1
                    continue
                if code == 'bottom':
                    weirdrates.append(float(row[-1]))
                    finalbottom = finalbottom + 1
                    continue
                if code == 'middle':
                    finalmid = finalmid + 1
                    continue
                if code == 'text':
                    finaltext = finaltext + 1
                    continue
                
    print "no data discarded "
    print nodata
    print "no congestion discarded "
    print skipped
    print "top"
    print finaltop
    print "middle"
    print finalmid
    print "bottom"
    print finalbottom
    print "text"
    print finaltext
    print "number of access"
    print len(accesses)
    #print (accesses)
    print "number of links"
    print len(links)
    #print (links)
    print "number of transit providers"
    print len(transits)
    print "top row loss rates over 50"
    print counter
    print sorted(lossrates)
    print "loss rates during uncongested for those same 5 (fetched from csv manually)"
    print "63.692 73.933 79.636 82.605 84.274"
    print "bottom loss rates"
    print sorted(weirdrates)
    #print (sorted(lossrates))
    #print len(lossrates)
    #print (transits)
def main():

    read_summary()

main()
