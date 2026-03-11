package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"os"
	"sort"
)

func runMapTask(
	jobName string, // The name of the whole mapreduce job
	mapTaskIndex int, // The index of the map task
	inputFile string, // The path to the input file that was assigned to this task
	nReduce int, // The number of reduce tasks
	mapFn func(file string, contents string) []KeyValue, // The user-defined map function
) {
	// ToDo: Write this function. See the description in the assignment.
	contents, err := os.ReadFile(inputFile)
	if err != nil {
		panic(err) // exiting and displaying the error message
	}
	keyvalue_map := mapFn(inputFile, string(contents)) // returns each word as the key and the value is empty string in testfile only

	intermediateFiles := make([]*os.File, nReduce) // creating empty file pointers
	encoders := make([]*json.Encoder, nReduce)     // creating empty json encoder pointers

	for i := range nReduce {
		filename := getIntermediateName(jobName, mapTaskIndex, i)
		file, err := os.Create(filename) // creating the file pyhsically and if already existed then delete existing contents
		if err != nil {
			panic(err)
		}
		intermediateFiles[i] = file         // attaching the created file to the file pointer
		encoders[i] = json.NewEncoder(file) //creating encoder and attach.
	}

	for _, kv := range keyvalue_map {
		reduceIndex := int(hash32(kv.Key) % uint32(nReduce)) // Determine reduce file index from the key(word name)
		err := encoders[reduceIndex].Encode(&kv)             // Write to the specific file the key value pair. Intermediate files have duplicate keys scattered
		if err != nil {
			panic(err)
		}
	}
	//closing all files
	for _, file := range intermediateFiles {
		file.Close()
	}

}

func hash32(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

// it ouptuts the each key sortedly and their total values.
// it reads the intermediate files where each having the keys and values scatterred where it combines same keys on same line.
func runReduceTask(
	jobName string, // the name of the whole MapReduce job
	reduceTaskIndex int, // the index of the reduce task
	nMap int, // the number of map tasks (number of intermediate files)
	reduceFn func(key string, values []string) string,
) {
	// ToDo: Write this function. See the description in the assignment.

	kvmap := make(map[string][]string) // map for which key: {all values}

	for i := range nMap {
		filename := getIntermediateName(jobName, i, reduceTaskIndex) // to get same file created before
		file, err := os.Open(filename)                               // opening the file
		if err != nil {
			panic(err)
		}

		decoder := json.NewDecoder(file) //creating decoder and attach.
		for {
			var kv KeyValue
			err = decoder.Decode(&kv) // reading the key value pair from the file and add it to kv
			if err != nil {
				if err.Error() == "EOF" {
					break // if end of file
				} else {
					panic(err)
				}
			}
			kvmap[kv.Key] = append(kvmap[kv.Key], kv.Value) // collecting same keys together with a list of string values all "1". hi: {1, 1, 1}
		}

		file.Close()
	}

	sortedKeys := make([]string, 0, len(kvmap))

	for k := range kvmap {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys) // to add the key words in sorted order in the output file

	outfilename := getReduceOutName(jobName, reduceTaskIndex)
	out_file, err := os.Create(outfilename) // creating the file pyhsically and if already existed then delete existing contents
	if err != nil {
		panic(err)
	}

	encoder := json.NewEncoder(out_file)

	for _, key := range sortedKeys {
		value := reduceFn(key, kvmap[key]) // returns an array of keyValues having each key with its total values
		kv := KeyValue{key, value}

		err := encoder.Encode(&kv) // writing the key value pair to the output file
		if err != nil {
			panic(err)
		}
	}

	out_file.Close()

}
