package mapreduce
import (
    "time"
    "fmt"
)

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

    // Time used to wait for registration of the workers to occur
    time.Sleep(50 * time.Millisecond)

    currentWorkers := len(mr.workers)
    for i := 0; i < ntasks; i++ {
        var taskArguments DoTaskArgs
        taskArguments.JobName = mr.jobName
        taskArguments.File = mr.files[i]
        taskArguments.Phase = phase
        taskArguments.TaskNumber = i
        taskArguments.NumOtherPhase = nios
        called := false
        called = call(mr.workers[i % currentWorkers], "Worker.DoTask", taskArguments, new(struct{}))

        if called == false {
            fmt.Printf("Error in sending the message to Worker: %v\n", mr.workers[i % currentWorkers])
        }
    }

    debug("Schedule: %v phase done\n", phase)
}
