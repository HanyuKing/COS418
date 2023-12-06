package mapreduce

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"sort"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Use checkError to handle errors.
	totalKVMap := make(map[string][]string)
	for i := 0; i < nMap; i++ {
		mapFileName := reduceName(jobName, i, reduceTaskNumber)
		bytesData, err := ioutil.ReadFile(mapFileName)

		checkError(err)

		var kvs []KeyValue
		json.Unmarshal(bytesData, &kvs)

		debug("doReduce phase. mapFileName: %s, len(kv): %d", mapFileName, len(kvs))

		combinedData := combine(kvs)

		for key, values := range combinedData {
			if _, ok := totalKVMap[key]; ok {
				totalKVMap[key] = append(totalKVMap[key], values...)
			} else {
				totalKVMap[key] = values
			}
		}
	}

	// sort by key
	var keys []string
	for k := range totalKVMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// write to a file
	mergeFileName := mergeName(jobName, reduceTaskNumber)
	file, err := os.Create(mergeFileName)
	checkError(err)
	encoder := json.NewEncoder(file)

	for _, key := range keys {
		encoder.Encode(KeyValue{
			Key:   key,
			Value: reduceF(key, totalKVMap[key]),
		})
	}

	file.Close()
}

func combine(kvs []KeyValue) map[string][]string {
	data := make(map[string][]string)
	for _, kv := range kvs {
		data[kv.Key] = append(data[kv.Key], kv.Value)
	}
	return data
}
