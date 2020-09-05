package mapreduce
// import "strconv"

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
    var ntasks int
    var nios int // number of inputs (for reduce) or outputs (for map)
    switch phase {
    case mapPhase:
        ntasks = len(mr.files)
        nios = mr.nReduce
    case reducePhase:
        ntasks = mr.nReduce
        nios = len(mr.files)
    }

    debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

    // All ntasks tasks have to be scheduled on workers, and only once all of
    // them have been completed successfully should the function return.
    // Remember that workers may fail, and that any given worker may finish
    // multiple tasks.
    //
    // TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
    //
    // for i := 0; i < ntasks; i++ {
    //     go RunWorker(mr.address, port("worker"+strconv.Itoa(i)),
    //         MapFunc, ReduceFunc, -1)
    // }
        
    // mr.Wait()
    // Call after the tasks have been completed
    // mr.Wait()
    debug("this is a test")

    debug("Schedule: %v phase done\n", phase)
}
