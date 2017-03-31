package mapreduce

import "fmt"
import "net/rpc"
import "log"
import "sync"

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
	var workers []*rpc.Client
	for {
		log.Println("get address")
		workerAddress, ok := <-registerChan
		if !ok {
			break
		}
		log.Println("Dial: ", workerAddress)
		c, errx := rpc.Dial("unix", workerAddress)
		if errx != nil {
			log.Fatal("Dail: ", errx)
		}
		log.Println("Dail Sucess: ", workerAddress)
		workers = append(workers, c)
	}

	log.Println("worker collection success: %d", len(workers))
	if phase == mapPhase {
		var wait_group sync.WaitGroup
		for idx, filename := range mapFiles {
			idx := idx
			filename := filename
			work_client := workers[idx%len(workers)]
			args := DoTaskArgs{jobName, filename, mapPhase, idx, len(mapFiles)}
			var reply *struct{}
			wait_group.Add(1)
			go func() {
				work_client.Call("Worker.DoTask", &args, reply)
				wait_group.Done()
			}()
		}
		wait_group.Wait()
	} else {
		var wait_group sync.WaitGroup

		for idx, filename := range mapFiles {
			idx := idx
			filename := filename
			work_client := workers[idx%len(workers)]
			args := DoTaskArgs{jobName, filename, reducePhase, idx, nReduce}
			var reply *struct{}
			wait_group.Add(1)
			go func() {
				work_client.Call("Worker.DoTask", &args, reply)
				wait_group.Done()
			}()
		}
		wait_group.Wait()
	}
	fmt.Printf("Schedule: %v phase done\n", phase)
}
