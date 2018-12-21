package main

/*
This script is for retrieving the YouTube streaming performance from the InfluxDB for congested/non-congested time periods.
Require access to beamer.caida.org:/project/comcast-ping/kabir-plots/loss_data/ddc_periods/ and MANIC InfluxDB

The results will be written to two CSV files cong.csv and noncong.csv.

The columns of output files are as follows:
1.time - the measurement timestamp
2.ArkMon - the Ark monitor that measure the interdomain link
3.Mon - the Ark/Samknows monitors that measure the YouTube performance
4.FarIP - the Far IP of the interdomain link
5.Error - Error code reported by the YouTube test
6.Abitrate - Audio bitrate of the downloaded video in the YouTube test
7.Bytessec - The overall video download throughput 
8.Ddur - The total download duration in the YouTube test
9.FarRtt - The RTT from the monitor to the Far IP measured in the YouTube test
10.Maxbitrate - The maximum video bitrate
11.Predur - The prebuffering duration (start-up delay + 3 seconds of video)
12.Sevt - Number of stalling event
13.Stdelay - Video start-up delay
14.Stotal - The total stalling time
15.Success - Indicate if the test was success
16.Vbitrate - The video bitrate of the downloaded video
17.Vconnect - The 3WHS time between the monitor and the YouTube video cache
18.Vserver - The IP of the video cache
19.Vth - The average video download rate
20.Webconnect - The 3WHS time between the monitor and the YouTube front-end website
21.Vbyte - The total number of bytes of downloaded video data 
22.Abyte - The total number of bytes of downloaded audio data 

*/
import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"manicinflux"
	"os"
	"regexp"
	"time"

	"github.com/influxdata/influxdb/client/v2"
)


const (
	CONGTIMEDIR = "/project/comcast-ping/kabir-plots/loss_data/ddc_periods/"
)

type CongPeriod struct {
	start, end int64
}
type DataInfo struct {
	mon, asn, ip           string
	startperiod, endperiod time.Time
}
type ManicResult struct {
	setting DataInfo
	result  client.Result
}

/*
func InfluxClient() (clnt client.Client) {
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     "https://beamer.caida.org:8086",
		Username: username,
		Password: password,
	})
	if err != nil {
		log.Fatal("Error in creating Influx client", err.Error())
	} else {
		return c
	}
	return nil
}

func queryDB(clnt client.Client, db, cmd string) (res []client.Result, err error) {
	q := client.Query{
		Command:  cmd,
		Database: db,
	}
	if response, err := clnt.Query(q); err == nil {
		if response.Error() != nil {
			return res, response.Error()
		}
		res = response.Results
	} else {
		return res, err
	}
	return res, nil
}

func convts(t string) time.Time {
	tsint, _ := strconv.ParseInt(t, 10, 64)
	return time.Unix(tsint, 0)
}
*/
//read the files for the beginning/ the end of congested time period
func readCongestionDir(dir string) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		log.Fatal(err)
	}
	N := 5
	congchan := make(chan ManicResult, 10)
	noncongchan := make(chan ManicResult, 10)
	writechan := make(chan interface{}, 2)
	sem := make(chan interface{}, N)
	congfile := "cong.csv"
	ncongfile := "ncong.csv"
	go outputPerf(congfile, congchan, writechan)
	go outputPerf(ncongfile, noncongchan, writechan)

	for _, f := range files {
		log.Println(f.Name())
		setting, err := extractLink(f.Name())
		setting.startperiod = time.Unix(1462060800, 0)
		setting.endperiod = time.Unix(1513252799, 0)
		if err == nil {
			fullpath := dir + f.Name()
			fptr, err := os.Open(fullpath)
			if err != nil {
				log.Fatal(err)
			}
			//			congfile := "./cong_" + setting.ip + ".csv"
			//			ncongfile := "./ncong_" + setting.ip + ".csv"

			var ctimeperiod []CongPeriod
			for ok := false; !ok; {
				var ts_start, ts_end int64
				item, _ := fmt.Fscanln(fptr, &ts_start, &ts_end)
				if item == 0 {
					ok = true
				} else {
					ctimeperiod = append(ctimeperiod, CongPeriod{start: ts_start * 1000000000, end: ts_end * 1000000000})
				}
				//	sem <- true

			}
			getPerformance(setting, ctimeperiod, congchan, noncongchan, sem, "ytcombine")
			getPerformance(setting, ctimeperiod, congchan, noncongchan, sem, "arkytperf")
			/*for n := 0; n < N; n++ {
				sem <- true
			}*/
			fptr.Close()
		}
	}
	close(congchan)
	close(noncongchan)
	for n := 0; n < 2; n++ {
		writechan <- true
	}

}

func extractLink(fname string) (DataInfo, error) {
	filereg, _ := regexp.Compile(`(\S+)\.(\d+\.\d+\.\d+\.\d+)\.win2\.txt`)
	if filereg.MatchString(fname) {
		param := filereg.FindStringSubmatch(fname)
		return DataInfo{mon: param[1], ip: param[2]}, nil
	}
	return DataInfo{}, errors.New("file name not match")

}

/*
func readCongestionTime(dir string) {
	//read the result dir
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		log.Fatal(err)
	}
	N := 10
	congchan := make(chan client.Result, 10)
	noncongchan := make(chan client.Result, 10)
	writechan := make(chan interface{}, 2)
	sem := make(chan interface{}, N)
	go outputPerf("./congperf.csv", congchan, writechan)

	go outputPerf("./noncongperf.csv", noncongchan, writechan)
	for _, f := range files {
		log.Println(f.Name())
		setting, err := extractSetting(f.Name())
		if err == nil {
			fullpath := dir + f.Name()
			fptr, err := os.Open(fullpath)
			if err != nil {
				log.Fatal(err)
			}
			var ctimeperiod []CongPeriod
			for ok := false; !ok; {
				var ts_start, ts_end int64
				var f1, f2, f3 float32
				item, _ := fmt.Fscanln(fptr, &ts_start, &ts_end, &f1, &f2, &f3)
				if item == 0 {
					ok = true
				} else {
					ctimeperiod = append(ctimeperiod, CongPeriod{start: ts_start * 1000000000, end: ts_end * 1000000000})
				}
				sem <- true
				go getPerformance(setting, ctimeperiod, congchan, noncongchan, sem)
				//			log.Println(ts_start, ts_end)
			}

			fptr.Close()
		}

	}
	for n := 0; n < N; n++ {
		sem <- true
	}
	close(congchan)
	close(noncongchan)
	for n := 0; n < 2; n++ {
		writechan <- true
	}
}
*/
func getPerformance(datasetting DataInfo, congperiod []CongPeriod, congchan chan ManicResult, noncongchan chan ManicResult, sem chan interface{}, meas string) {
	//generate conditions
	if len(congperiod) > 0 {
		clnt, err := manicinflux.ManicClient()
		if err != nil {
			log.Fatal("error client")
		}
		defer manicinflux.ManicClose(clnt)
		//timeq := ""
		//timenormal := ""
		noncstart := datasetting.startperiod.UnixNano()
		var endctime int64
		for _, period := range congperiod {
			congtimeq := fmt.Sprintf("(time >= %d AND time< %d) ", period.start, period.end)
			querycongcond := fmt.Sprintf("select Abitrate, Error, Bytessec, Ddur,FarIP,FarRtt,Maxbitrate,Mon,Predur, Sevt, Stdelay, Stotal, Success, Vbitrate, Vconnect, Vserver, Vth, Webconnect,Vbyte,Abyte from %s WHERE FarIP='%s' AND (%s)", meas, datasetting.ip, congtimeq)
			res, err := manicinflux.QueryManic(clnt, manicinflux.TESTTSPDB, querycongcond)
			if err != nil {
				log.Fatal(err)
			}
			congchan <- ManicResult{setting: datasetting, result: res[0]}

			noncongtimeq := fmt.Sprintf("(time >= %d AND time< %d) ", noncstart, period.start)
			endctime = period.end
			noncstart = period.end
			querynoncongcond := fmt.Sprintf("select Abitrate, Error, Bytessec, Ddur,FarIP,FarRtt,Maxbitrate,Mon,Predur, Sevt, Stdelay, Stotal, Success, Vbitrate, Vconnect, Vserver, Vth, Webconnect,Vbyte,Abyte from %s WHERE FarIP='%s' AND (%s)", meas, datasetting.ip, noncongtimeq)
			res, err = manicinflux.QueryManic(clnt, manicinflux.TESTTSPDB, querynoncongcond)
			if err != nil {
				log.Fatal(err)
			}
			noncongchan <- ManicResult{setting: datasetting, result: res[0]}

			/*			if len(timeq) == 0 {
							timeq = fmt.Sprintf("(time >= %d AND time< %d) ", period.start, period.end)
							timenormal = fmt.Sprintf(" AND time<%d) OR (time>=%d ", period.start, period.end)
						} else {
							timeq = timeq + fmt.Sprintf("OR (time >= %d AND time< %d) ", period.start, period.end)
							timenormal = timenormal + fmt.Sprintf(" AND time<%d) OR (time>=%d", period.start, period.end)
						}*/
		}
		lastcongtimeq := fmt.Sprintf("(time >= %d AND time< %d) ", endctime, datasetting.endperiod.UnixNano())

		lastnoncongcond := fmt.Sprintf("select Abitrate, Error, Bytessec, Ddur,FarIP,FarRtt,Maxbitrate,Mon,Predur, Sevt, Stdelay, Stotal, Success, Vbitrate, Vconnect, Vserver, Vth, Webconnect,Vbyte,Abyte from %s WHERE FarIP='%s' AND (%s)", meas, datasetting.ip, lastcongtimeq)
		res, err := manicinflux.QueryManic(clnt, manicinflux.TESTTSPDB, lastnoncongcond)
		if err != nil {
			log.Fatal(err)
		}
		noncongchan <- ManicResult{setting: datasetting, result: res[0]}

		//		log.Println(querycongcond)
		/*		res, err := queryDB(clnt, TESTTSPDB, querycongcond)
				if err != nil {
					log.Fatal(err)
				}
				congchan <- res[0]
				//output to congestion
				querynoncongcond := fmt.Sprintf("select Abitrate, Allth, Bytessec, Ddur,FarIP,FarRtt,Maxbitrate,Mon,Predur, Sevt, Stdelay, Stotal, Success, Vbitrate, Vconnect, Vserver, Vth, Webconnect from ytcombine WHERE FarIP='%s' AND ((time>=%d %s AND time<%d)) ", datasetting.ip, datasetting.startperiod.UnixNano(), timenormal, datasetting.endperiod.UnixNano())
				log.Println(querynoncongcond)
				res, err = queryDB(clnt, TESTTSPDB, querynoncongcond)
				if err != nil {
					log.Fatal(err)
				}
				noncongchan <- res[0]*/
	}
	//	<-sem

}

func outputPerf(outputfile string, congchan chan ManicResult, writechan chan interface{}) {
	writechan <- true
	fcong, err := os.Create(outputfile)
	if err != nil {
		log.Fatal(err)
	}
	//write header
	fcong.WriteString("time,ArkMon,Mon,FarIP,Error,Abitrate,Bytessec,Ddur,FarRtt,Maxbitrate,Predur,Sevt,Stdelay,Stotal,Success,Vbitrate,Vconnect,Vserver,Vth,Webconnect,Vbyte,Abyte\n")
	defer fcong.Close()
	for cg := range congchan {
		c := cg.result
		if len(c.Series) > 0 {
			for _, v := range c.Series[0].Values {
				ostr := fmt.Sprintf("%s,%s,%s,%s,%s,%f,%f,%f,%f,%f,%f,%d,%f,%f,%s,%f,%f,%s,%f,%f,%f,%f\n",
					v[0].(string),
					cg.setting.mon,
					v[8].(string),
					v[5].(string),
					v[2].(string),
					manicinflux.JSONNumtoFloat(v[1]),
					manicinflux.JSONNumtoFloat(v[3]),
					manicinflux.JSONNumtoFloat(v[4]),
					manicinflux.JSONNumtoFloat(v[6]),
					manicinflux.JSONNumtoFloat(v[7]),
					manicinflux.JSONNumtoFloat(v[9]),
					manicinflux.JSONNumtoInt(v[10]),
					manicinflux.JSONNumtoFloat(v[11]),
					manicinflux.JSONNumtoFloat(v[12]),
					v[13].(string),
					manicinflux.JSONNumtoFloat(v[14]),
					manicinflux.JSONNumtoFloat(v[15]),
					v[16].(string),
					manicinflux.JSONNumtoFloat(v[17]),
					manicinflux.JSONNumtoFloat(v[18]),
					manicinflux.JSONNumtoFloat(v[19]),
					manicinflux.JSONNumtoFloat(v[20]))
				_, err = fcong.WriteString(ostr)
			}
		}
	}
	fcong.Sync()
	<-writechan
}

/*
func influxfloat(v interface{}) float64 {
	res, _ := v.(json.Number).Float64()
	return res
}

func influxint(v interface{}) int64 {
	res, _ := v.(json.Number).Int64()
	return res
}
*/
//extract the parameter from file name
//output: monitor, asn, farip, startdate, enddate
/*func extractSetting(fname string) (DataInfo, error) {
	filereg, _ := regexp.Compile(`(\S+)\.(\d+)\.(\S+)\.(\S+)\.(\d+\.\d+\.\d+\.\d+)`)
	if filereg.MatchString(fname) {
		param := filereg.FindStringSubmatch(fname)
		sdatearr := strings.Split(param[3], "_")
		edatearr := strings.Split(param[4], "_")
		syr, _ := strconv.Atoi(sdatearr[2])
		smonth, _ := strconv.Atoi(sdatearr[0])
		sday, _ := strconv.Atoi(sdatearr[1])

		eyr, _ := strconv.Atoi(edatearr[2])
		emonth, _ := strconv.Atoi(edatearr[0])
		eday, _ := strconv.Atoi(edatearr[1])

		sdate := time.Date(syr, time.Month(smonth), sday, 0, 0, 0, 0, time.UTC)
		edate := time.Date(eyr, time.Month(emonth), eday, 23, 59, 59, 0, time.UTC)
		return DataInfo{mon: param[1], asn: param[2], ip: param[5], startperiod: sdate, endperiod: edate}, nil
	}
	return DataInfo{}, errors.New("file not match")
}
*/
func main() {
	//	fmt.Println("vim-go")
	log.Println("Start")
	readCongestionDir(CONGTIMEDIR)
	log.Println("End")
}
