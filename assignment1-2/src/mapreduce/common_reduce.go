package mapreduce

import (
	"os"
	"io"
	"encoding/json"

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

	// [hello, 1] [hello, 1] [my, 1]
	// [hello , [1,1]] [my, [1]]

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

//According to github assignment README point 4 says
//(A) collects corresponding files from each map result
//I need a mapper to 'collect' all keys in one file
	mapper := make(map[string]([]string))//slices


    for i := 0; i < nMap; i++{

		filename := reduceName(jobName,i,reduceTaskNumber)
		file,err := os.Open(filename)
		checkError(err)

		defer file.Close()

		enc := json.NewDecoder(file)

		//decode file
		var kv KeyValue
		for {
			
			err := enc.Decode(&kv);

			if err == io.EOF {
				break;
			}else if err != nil{
				checkError(err)
			}

			mapper[kv.Key] = append(mapper[kv.Key],kv.Value)//[1]  //[1,1]   //(hello)

			
			checkError(err)
		}
		
	}

	outputFileName := mergeName(jobName,reduceTaskNumber)

	file, err := os.Create(outputFileName)
	checkError(err)

	defer file.Close()

	enc := json.NewEncoder(file)

	//(B) and runs the reduce function on each collection
	for k,v := range mapper {
		enc.Encode(KeyValue{k, reduceF(k,v)})//{ "key" : "hello", "value" : "2"}
	}

	file.Close()

}
