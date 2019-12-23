package mapreduce

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//

	// init slices for saving results
	kvs := make([]KeyValue, 0)

	// read intermediate files
	for mapTask := 0; mapTask < nMap; mapTask++ {
		// open file
		fileName := reduceName(jobName, mapTask, reduceTask)
		mapOutput, err := ioutil.ReadFile(fileName)
		if err != nil {
			log.Fatal(err)
		}

		// define decode process
		currKVPairs := make([]KeyValue, 0)
		if err = json.Unmarshal(mapOutput, &currKVPairs); err != nil {
			log.Fatal(err)
		}

		// collect kv pairs
		for _, x := range currKVPairs {
			kvs = append(kvs, x)
		}

		// build kv map
		// no need to sort kv pairs
		kvMap := make(map[string][]string)
		for _, kv := range kvs {
			key, value := kv.Key, kv.Value
			kvMap[key] = append(kvMap[key], value)
		}

		// make output
		resFile, err := os.Create(outFile)
		if err != nil {
			log.Fatal(err)
		}
		encoder := json.NewEncoder(resFile)
		for key, value := range kvMap {
			if err := encoder.Encode(KeyValue{key, reduceF(key, value)}); err != nil {
				log.Fatal(err)
			}
		}

		// close file
		if err := resFile.Close(); err != nil {
			log.Fatal(err)
		}
	}

}
