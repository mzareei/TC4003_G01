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
    // for a, w := range mr.workers {
    //     debug("a:%v, worker:%v\n", a, w)
    // }

    currentWorkers := len(mr.workers)
    for i := 0; i < ntasks/(len(mr.workers)); i++ {
        var taskArguments DoTaskArgs
        taskArguments.JobName = fmt.Sprint(i)
        taskArguments.File = mr.files[i]
        taskArguments.Phase = phase
        taskArguments.TaskNumber = i
        taskArguments.NumOtherPhase = nios
        called := false
        for currWorker := 0; currWorker < currentWorkers; currWorker++ {
            called = call(mr.workers[(i + currWorker)%currentWorkers], "Worker.DoTask", taskArguments, new(struct{}))
            if called == false {
                fmt.Printf("Error in sending the message to Worker: %v\n", mr.workers[currWorker])
            }
        }

    }
        
    // mr.Wait()
    // Call after the tasks have been completed
    // mr.Wait()
    debug("RABC was here\n")

    debug("Schedule: %v phase done\n", phase)
}
