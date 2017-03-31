package mapreduce

import "fmt"

// import "net/rpc"
// import "log"
import "sync"

// import "time"

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to Call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var nTasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		nTasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		nTasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", nTasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	// var workers []*rpc.Client
	// label1:
	i := 0
	workerChan := make(chan string)
	var wg sync.WaitGroup
	wg.Add(nTasks)
loop1:
	for {
		var workerAddress string
		select {
		case workerAddress = <-registerChan:
			break
		case workerAddress = <-workerChan:
			break
		}
		go func(worker string, index int) {
			args := DoTaskArgs{jobName, mapFiles[index], phase, index, n_other}
			var reply *struct{}
			call(worker, "Worker.DoTask", &args, reply)
			wg.Done()
			workerChan <- worker
		}(workerAddress, i)
		i++
		if i == nTasks {
			break loop1
		}
	}
	wg.Wait()

	fmt.Printf("Schedule: %v phase done\n", phase)
}
