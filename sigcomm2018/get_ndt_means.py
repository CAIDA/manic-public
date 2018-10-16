import sys
import statistics

from datetime import datetime
from influxdb import InfluxDBClient
from scipy import stats


def diff_from_unix_epoch(ts):
    diff = datetime.strptime(str(ts), "%Y-%m-%dT%H:%M:%SZ") - datetime(1970,1,1)
    return int(diff.total_seconds())

def is_congested_interval(timestamp, intervals):
    def is_in_window(timestamp, starts, ends):
        if int(timestamp) <= int(ends) and int(timestamp) >= int(starts):
            return True
        return False

    for interval in intervals:
        if is_in_window(timestamp, interval[0], interval[1]):
            return True
    return False


# Step 1: Decide start and end point in epoch seconds
# Step 2: Find all congestion intervals
# Step 3: Get all data points from ndt tests
# Step 4: Get mean/max/min for congestion interval and uncongested intervals

client = InfluxDBClient('localhost', 0, '', '', '', ssl=True)


# Step 1 + 2: Ideally we want to do this dynamically
fname = sys.argv[1]
fd = open(fname)
intervals = map(lambda x: x.split(" "), fd.read().splitlines())

mon = fname.split("/")[-1].split(".")[0]
target = ".".join(fname.split(".")[1:5])
#start_ts = "1512086400" #17501
#end_ts = "1513209600" #17514
start_ts = "1510704000" # 11/15
end_ts = "1514764800" # 01/01

query_string = """SELECT "mon","srvname","target","tput_down","tput_up" FROM "ndt" WHERE "mon" =~ /^""" \
        	+ mon + """$/ AND "target" =~ /^""" + target + """$/ AND time <= """ + end_ts + """s AND time >= """ + start_ts + """s"""

print query_string
results = client.query(query_string)

all_points = []
for result in results:
    for r in result:
        all_points.append([r["tput_up"], r["tput_down"], diff_from_unix_epoch(r["time"])])

congested_tput_up = []
uncongested_tput_up = []
congested_tput_down = []
uncongested_tput_down = []

for point in all_points:
    if is_congested_interval(point[2], intervals):
        congested_tput_up.append(point[0])
        congested_tput_down.append(point[1])
    else:
        uncongested_tput_up.append(point[0])
        uncongested_tput_down.append(point[1])

print "Congested UPLOAD Data Points\n"
print "NUM: Congested Upload Throughput Points: {}".format(len(congested_tput_up))
print "MEAN: Congested Upload Throughput Points: {}".format(statistics.mean(congested_tput_up))
print "MAX: Congested Upload Throughput Points: {}".format(max(congested_tput_up))

print "Uncongested UPLOAD Data Points\n"
print "NUM: Uncongested Upload Throughput Points: {}".format(len(uncongested_tput_up))
print "MEAN: Uncongested Upload Throughput Points: {}".format(statistics.mean(uncongested_tput_up))
print "MAX: Uncongested Upload Throughput Points: {}".format(max(uncongested_tput_up))
fisher_coeff, pvalue = stats.ttest_ind(congested_tput_up, uncongested_tput_up)
print "Fisher_Coeff: {}, Pvalue: {}".format(fisher_coeff, float(pvalue))


print "Congested DOWNLOAD Data Points\n"
print "NUM: Congested download Throughput Points: {}".format(len(congested_tput_down))
print "MEAN: Congested download Throughput Points: {}".format(statistics.mean(congested_tput_down))
print "MAX: Congested download Throughput Points: {}".format(max(congested_tput_down))

print "Unongested DOWNLOAD Data Points\n"
print "NUM: Uncongested download Throughput Points: {}".format(len(uncongested_tput_down))
print "MEAN: Uncongested download Throughput Points: {}".format(statistics.mean(uncongested_tput_down))
print "MAX: Uncongested download Throughput Points: {}".format(max(uncongested_tput_down))
fisher_coeff, pvalue = stats.ttest_ind(congested_tput_down, uncongested_tput_down)
print "Fisher_Coeff: {}, Pvalue: {}".format(fisher_coeff, float(pvalue))

