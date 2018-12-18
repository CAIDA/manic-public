from collections import Counter
from collections import defaultdict
import sys
import datetime

# NOTES
# genpdf_congestion_windows.py
# What does this file do?
#    - Generate pdf of congestion windows for links from a VP
# What do you need access to?
#    - Require access to beamer://project/comcast-ping/gautam-cw
#    - The files contain congestion windows for different links as seen from different VP
# How does it do this?
#    - At a high level we take all of the congestion windows and bin them based on the hour.
#    - For the paper, we bin them based on hour and weekend or weekday
#    - There is commented code that you can use to bin based on hour and day of the week
# How to run this?
#    - Execute: python genpdf_congestion_windows.py VP files_with_congestion_windows_from_VP
#    - VP stands for vantage points. The paper has a graph for VP bed-us
#    - We have data for many vantage points - las-us, atl2-us
#    - If you wanted to recreate the data for the paper you would run the following
#    - python genpdf_congestion_windows.py bed-us /path/to/files/bed-us.*
#    - The input files in this case are *all links* seen from bed-us VP
#    - Make sure that the VP and input files match
# Why do we need the VP?
#    - We want to make sure that we get the pdf based on local time
#    - So knowing the VP allows us to build pdf based of the right time zone


def diff_from_unix_epoch(ts):
    diff = datetime.datetime.strptime(str(ts), "%Y-%m-%dT%H:%M:%SZ") - datetime.datetime(1970,1,1)
    return int(diff.total_seconds())


def get_utc_hr_min(epoch_ts, offset):
        interval_hours = []
        start_time = datetime.datetime.utcfromtimestamp(float(epoch_ts[0]))
        end_time = datetime.datetime.utcfromtimestamp(float(epoch_ts[1]))
        itr_time = start_time
        while(itr_time <= end_time):
                time = itr_time - datetime.timedelta(hours=offset)
                day_no = time.weekday()
                if time.weekday() < 5:
                        day = "Weekday"
                else:
                        day = "Weekend"
                hr = "{}-{}".format(day, time.strftime("%H"))#, time.strftime("%M"))
                interval_hours.append(hr)
                itr_time = itr_time + datetime.timedelta(minutes=15)
        return interval_hours
        #return "{}-{}_{}".format(time.strftime("%A"), time.strftime("%H"), time.strftime("%M"))

        #return "{}-{}".format(time.strftime("%A"), time.strftime("%H"))#, time.strftime("%M")a

interval_count = Counter()

if len(sys.argv) < 3:
        print "program monitor files"
        exit()

time_zone = {"bed-us": "east", "atl2-us": "east", "dca2-us": "east",
             "gai-us": "east", "bos5-us":"east", "lke-us":"west", "pao-us":"west",
             "las-us": "west", "wbu2-us": "mtn",
             "san2-us": "west", "san4-us": "west", "san7-us": "west",
             "tul-us": "central", "tul3-us": "central", "aza-us": "mtn", "ith-us": "east", "lex-us": "east",
             "mnz-us": "east", "mry-us": "west", "oak5-us": "west"}

for fname in sys.argv[2:]:
        tz = time_zone[sys.argv[1]]
        if tz == "west":
                offset = 8
        elif tz == "east":
                offset = 5
        elif tz == "central":
                offset = 6
        elif tz == "mtn":
                offset = 7
        else:
                print "Default to west"
                offset = 8
        with open(fname) as f:
                data = map(lambda x: x.split(), f.readlines())
                data = map(lambda x: get_utc_hr_min(x, offset), data)
        data = [item for intervals in data for item in intervals]
        interval_count.update(Counter(data))

mega_interval_count = {}

hours = []
tmp_d = datetime.datetime(1970,1,1)
for i in range(0, 96):
        #tstr = "{}_{}".format(tmp_d.strftime("%H"), tmp_d.strftime("%M"))
        tstr = "{}".format(tmp_d.strftime("%H"))#, tmp_d.strftime("%M"))
        if tstr not in hours:
                hours.append(tstr)
        mega_interval_count[tstr] = {}
        mega_interval_count[tstr]["Weekday"] = 0.0
        mega_interval_count[tstr]["Weekend"] = 0.0
        mega_interval_count[tstr]["Monday"] = 0.0
        mega_interval_count[tstr]["Tuesday"] = 0.0
        mega_interval_count[tstr]["Wednesday"] = 0.0
        mega_interval_count[tstr]["Thursday"] = 0.0
        mega_interval_count[tstr]["Friday"] = 0.0
        mega_interval_count[tstr]["Saturday"] = 0.0
        mega_interval_count[tstr]["Sunday"] = 0.0
        tmp_d = tmp_d + datetime.timedelta(minutes=15)


day_dict = {}
day_dict["Weekday"] = 0.0
day_dict["Weekend"] = 0.0
day_dict["Monday"] = 0.0
day_dict["Tuesday"] = 0.0
day_dict["Wednesday"] = 0.0
day_dict["Thursday"] = 0.0
day_dict["Friday"] = 0.0
day_dict["Saturday"] = 0.0
day_dict["Sunday"] = 0.0

for interval in interval_count:
        x = interval.split("-")
        day_dict[x[0]] += interval_count[interval]
        mega_interval_count[x[1]][x[0]] += interval_count[interval]

print >> sys.stderr, "{},{},{}".format("Hour", "Weekday", "Weekend")
for tstr in mega_interval_count:
        wkday = mega_interval_count[tstr]["Weekday"] / day_dict["Weekday"]
        wkend = mega_interval_count[tstr]["Weekend"] / day_dict["Weekend"]
        print "{},{},{}".format(tstr, wkday, wkend)

