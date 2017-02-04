package mapreduce

import (
	"fmt"
	"sync"
	//"log"
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

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//

	//http://goinbigdata.com/golang-wait-for-all-goroutines-to-finish/
	//used to wait all goroutines to finish
	var wg sync.WaitGroup
	// for every task, create a goroutine.
	for i := 0; i < ntasks; i++ {
		// add 1 goroutine to wait group
		wg.Add(1)
		// create a goroutine
		// parse the i as a parameter to ensure the goroutine get the right i.
		go func(i int) {
			// call wg.Done when everything in goroutine is done
			defer wg.Done()

			/* To print debug message. It print things only when program stops
				so when the loop reaches the last loop, call log.Panic to force the program stop
			fmt.Println("here is %s",i)
			if i == ntasks-1 {
				log.Panic()
			}
			*/

			// put all job info to DoTaskArgs struct
			jobarg := DoTaskArgs{
				JobName:       mr.jobName,
				Phase:         phase,
				TaskNumber:    i,
				File:          mr.files[i],
				NumOtherPhase: nios,
			}

			// for loop used to Handling worker failures. if worker gives no response, keep calling the worker.
			for {
				// get the available worker from master registerchannel
				worker := <-mr.registerChannel
				// let worker do the job by using call()
				ok := call(worker, "Worker.DoTask", jobarg, new(struct{}))
				if ok == false {
					fmt.Printf("Can not send job to worker: %s\n", worker)
				} else {
					// if the job is finished, create a new go routine. let this goroutine keep trying (because there can be only one worker in regster channel)
					// to put worker back to the master register channel.
					go func() {
						mr.registerChannel <- worker
					}()
					// break the loop if work is done
					break
				}
			}
		}(i)
	}
	//wait until every goroutine is done.
	wg.Wait()

	fmt.Printf("Schedule: %v phase done\n", phase)
}
