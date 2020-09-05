package mapreduce

import (
	"hash/fnv"
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


	//example file content
	// hello my name is roberto hello
	//example key value pairs
	// [ hello , 1], [my, 1], [name , 1], [is, 1], [roberto, 1], [ hello , 1]
	//Divide the previous in nReduce workers
			//File 1 (reduceName) r = 1
				// [ hello , 1], [my, 1], [name , 1]
			//File 2 (reduceName) r = 2
				// [is, 1], [roberto, 1],[ hello , 1]



			//ihash
			//File 1 (reduceName) r = 1
				// [ hello , 1], [hello, 1], [name , 1]
			//File 2 (reduceName) r = 2
				// [is, 1], [roberto, 1],[ my , 1]


				/*File 1 JSON format [ 
					{ "key" : "hello", "value" : 1 }, 
					{ "key" : "hello", "value" : 1 }, 
					{ "key" : "my", "value" : 1 }
				]*/


//as9dua8sud98u4r98w9sed984u9orja90ud98ur98us98ua9s8dua9s8dua98us9a8ud9sa8us9dasd= base32

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
	// Use checkError to handle errors.


		/*robertojsh: So according to the description over the README File under 'getting familiar with the source' section point number 3 
			states the following --- Each call to doMap() (A) reads the appropriate file, (B) calls the map function on that file's contents, 
			and (C) produces nReduce files for each map file. ----
			Then we need to implement A B and C 
			*/
		


	//(A) reads the appropiate file
	appropiateFileContent, err := ioutil.ReadFile(inFile)
	checkError(err)

	//(B)calls the map function on that file's contents
	keyvs := mapF(inFile, string(appropiateFileContent))

    encoders := make([]*json.Encoder, nReduce)//Creates an array of enconders of nReduce size so I can store all the encoders for each file

	//(C) produces nReduce files for each map file
	//First we need to generate nReduce files and its encoders
	for i := 0; i < nReduce; i++{
		//I need to find the filename of the input reduce task with reduceName function
		filename := reduceName(jobName,mapTaskNumber, i)

		newFile, err := os.Create(filename)//creates the file
		checkError(err)

		defer newFile.Close()

		//Encode to JSON
		encoders[i] = json.NewEncoder(newFile)
	}
	
	//We need to store our key/value pairs in nReduce files using the encoders created above
	for _, kv := range keyvs {
		k := kv.Key // I need the key to use the ihash function to determine which file belongs to
		iHashedKey := ihash(k)
		
		//given that ihash returns a big number I need to reduce or group this number
		//if I use a mod operator with nReduce files the result always will be 0 because is posible to divide this big number in the 
		//nReduce number and if not I have an extra... that extra will never could be bigger than nReduce
		enc := encoders[int(iHashedKey) % nReduce]
		err := enc.Encode(&kv)
		checkError(err)

	}
	


}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
