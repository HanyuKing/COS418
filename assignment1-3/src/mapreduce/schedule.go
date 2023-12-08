package mapreduce

import "sync"

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
	//
	//

	wg := &sync.WaitGroup{}

	for i := 0; i < ntasks; i++ {
		wg.Add(1)

		go func(i int) {
			for {
				worker := <-mr.registerChannel
				args := DoTaskArgs{
					JobName:       mr.jobName,
					File:          mr.files[i],
					Phase:         phase,
					TaskNumber:    i,
					NumOtherPhase: nios,
				}
				success := call(worker, "Worker.DoTask", args, &ShutdownReply{})
				if success {
					wg.Done()
					mr.registerChannel <- worker
					break
				}
			}
		}(i)
	}

	wg.Wait()

	debug("Schedule: %v phase done\n", phase)
}
