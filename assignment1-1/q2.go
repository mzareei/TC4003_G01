package cos418_hw1_1

import (
	"bufio"
	"io"
	"strconv"
	//"sync"
	"os"
	//"fmt"
)

//WaitGroup to Sync channels
//var wg sync.WaitGroup

// Sum numbers from channel `nums` and output sum to `out`.
// You should only output to `out` once.
// Do NOT modify function signature.
func sumWorker(nums chan int, out chan int) {
	// TODO: implement me
	// HINT: use for loop over `nums`

	//We defef the waitgroup to be done
	//defer wg.Done()

	workerTotal := 0//This is a partial sum
	for n := range nums{
		workerTotal += n
	}

	//We put the partial result on the output channel
	out <- workerTotal
}

// Read integers from the file `fileName` and return sum of all values.
// This function must launch `num` go routines running
// `sumWorker` to find the sum of the values concurrently.
// You should use `checkError` to handle potential errors.
// Do NOT modify function signature.
func sum(num int, fileName string) int {
	// TODO: implement me
	// HINT: use `readInts` and `sumWorkers`
	// HINT: used buffered channels for splitting numbers between workers

	//Reading the file 
	content, e := os.Open(fileName)
	checkError(e)
	defer content.Close()

	numbers,e := readInts(content)
	checkError(e)
	
	
	//fmt.Println(numbers)

	//We need to create the input and output channels that will handle the buffer between jobs
	//We will create as many channels as workers requested by num parametes
	inputNums := make(chan int, len(numbers)/num)
	outputNums := make(chan int, num)//We just need one output channel per worker

	for i := 0; i < num; i++ {
		//wg.Add(1)//Add another worker to the WaitGroup
		go sumWorker(inputNums,outputNums)
	}

	for _, n := range numbers{
		inputNums <- n
	}


	//Concurrency must be over before closing the channels
	//So I need to wait for the channels before close them
	//wg.Wait()
	close(inputNums)



	total := 0
	for x :=0; x < num; x++ {
		total += <-outputNums
	}
	close(outputNums)
	
	return total
}

// Read a list of integers separated by whitespace from `r`.
// Return the integers successfully read with no error, or
// an empty slice of integers and the error that occurred.
// Do NOT modify this function.
func readInts(r io.Reader) ([]int, error) {
	scanner := bufio.NewScanner(r)
	scanner.Split(bufio.ScanWords)
	var elems []int
	for scanner.Scan() {
		val, err := strconv.Atoi(scanner.Text())
		if err != nil {
			return elems, err
		}
		elems = append(elems, val)
	}
	return elems, nil
}
