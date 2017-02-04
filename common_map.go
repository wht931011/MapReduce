package mapreduce

import (
	"hash/fnv"
	"fmt"
	"io/ioutil"
	"os"
	"encoding/json"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	// TODO:
	// You will need to write this function.
	// You can find the filename for this map task's input to reduce task number
	// r using reduceName(jobName, mapTaskNumber, r). The ihash function (given
	// below doMap) should be used to decide which file a given key belongs into.
	//
	// The intermediate output of a map task is stored in the file
	// system as multiple files whose name indicates which map task produced
	// them, as well as which reduce task they are for. Coming up with a
	// scheme for how to store the key/value pairs on disk can be tricky,
	// especially when taking into account that both keys and values could
	// contain newlines, quotes, and any other character you can think of.
	//
	// One format often used for serializing data to a byte stream that the
	// other end can correctly reconstruct is JSON. You are not required to
	// use JSON, but as the output of the reduce tasks *must* be JSON,
	// familiarizing yourself with it here may prove useful. You can write
	// out a data structure as a JSON string to a file using the commented
	// code below. The corresponding decoding functions can be found in
	// common_reduce.go.
	//
	//   enc := json.NewEncoder(file)
	//   for _, kv := ... {
	//     err := enc.Encode(&kv)
	//
	// Remember to close the file after you have written all the values!
	dat, err := ioutil.ReadFile(inFile)
    checkError(err)

    // Create a KV_list which has word as key and "" as value
    // mapF in test_test.go
    KV_list := mapF(inFile,string(dat))

    // Create a map which has filename as Key and a list of KeyValue struct as value
    kv_file_map := make(map[string][]KeyValue)
    for _,element := range KV_list{
    	file_num := int(ihash(element.Key)) % nReduce
		filename := reduceName(jobName, mapTaskNumber, file_num)
		kv_file_map[filename] = append(kv_file_map[filename],element)
	}

	// According num of nReduce, create nReduce Files to store KeyValue pair
	for i :=0; i < nReduce;i++{
		// get file name
    	filename := reduceName(jobName, mapTaskNumber, i)
    	// create file
    	var file, err = os.Create(filename)
		checkError(err)
		// create json
		enc := json.NewEncoder(file)
	  	for _, kv := range kv_file_map[filename] {
	  		// write json to file
	    	err := enc.Encode(&kv)
	    	checkError(err)
    	}
    	// close file
		file.Close()
    }
}
    //***************************************************

//     // map: file name is key, opened file is value
//     files := make(map[string]*os.File)
//     // smap: file name is key, encoded jason is value
//     jsons := make(map[string]*json.Encoder)
//
//     // store opened file into files and encoded json to jsons
//     for i :=0; i < nReduce;i++ {
// 		filename := reduceName(jobName, mapTaskNumber, i)
// 		var file, err = os.Create(filename)
// 		checkError(err)
// 		enc := json.NewEncoder(file)
// 		files[filename] = file
// 		jsons[filename] = enc
// 	}
// 	// use ihash to decide which file the kv pair belongs to
// 	// write kv into file
// 	for _,kv := range KV_list{
// 		file_num := int(ihash(kv.Key)) % nReduce
// 		filename := reduceName(jobName, mapTaskNumber, file_num)
// 		err := jsons[filename].Encode(&kv)
// 		checkError(err)
// 	}
// 	// close all the files
// 	for _, file := range files{
// 		file.Close()
// 	}
// }

func checkError(err error) {
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(0)
	}
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
