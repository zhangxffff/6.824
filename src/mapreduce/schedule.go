package mapreduce

import (
    "fmt"
    "sync"
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
    var wg sync.WaitGroup
    failChan := make(chan int, ntasks)
    done := make(chan int )
    for i := ntasks - 1; i >= 0; i-- {
        wg.Add(1)
        failChan <- i
    }
    go func() {
        wg.Wait()
        close(done)
    } ()

    loop:
    for {
        select {
        case <-done:
            break loop
        case i := <-failChan:
            var arg DoTaskArgs
            arg.JobName = jobName
            arg.Phase = phase
            arg.NumOtherPhase = n_other
            arg.TaskNumber = i
            if phase == mapPhase {
                arg.File = mapFiles[i]
            }
            go func(arg DoTaskArgs) {
                ch := <-registerChan
                ok := call(ch, "Worker.DoTask", arg, nil)
                if ok {
                    go func() {
                        registerChan <- ch
                    } ()
                    wg.Done()
                } else {
                    failChan <- arg.TaskNumber
                }
            } (arg)
        }
    }
	fmt.Printf("Schedule: %v phase done\n", phase)
}
