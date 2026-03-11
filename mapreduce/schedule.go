package mapreduce

import (
	"fmt"
	"sync"
)

func (mr *Master) schedule(phase jobPhase) {
	fmt.Printf("Entered schedule with phase: %s\n", phase)

	var ntasks int
	var numOtherPhase int
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)     // number of map tasks
		numOtherPhase = mr.nReduce // number of reducers
	case reducePhase:
		ntasks = mr.nReduce           // number of reduce tasks
		numOtherPhase = len(mr.files) // number of map tasks
	}
	debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, numOtherPhase)

	// ToDo: Complete this function. See the description in the assignment.

	var waitgrp sync.WaitGroup
	waitgrp.Add(ntasks) // wait for all threads to finish where each thread is a task

	// create a struct to hold the reply
	type replystruct struct {
	}

	for task := 0; task < ntasks; task++ {

		fmt.Printf("enter the loop with task index:  %d\n", task)

		go func(task int) {
			for {
				worker_address := <-mr.registerChannel
				args := RunTaskArgs{
					JobName:       mr.jobName,
					TaskNumber:    task,
					Phase:         phase,
					NumOtherPhase: numOtherPhase,
				}
				if phase == mapPhase {
					args.File = mr.files[task] // assign the appropriate file to the mapper of this task
				}

				reply := replystruct{}
				fmt.Printf("Worker address: %s\n", worker_address)

				response := call(worker_address, "Worker.RunTask", &args, &reply)
				if response {
					waitgrp.Done() // decrement the waitgroup counter
					fmt.Printf("Task %d completed\n", task)
					mr.registerChannel <- worker_address // succefull worker is then added back to the channel
					break                                // used if the task wasnt completed then try same task on another worker
				}

			}

		}(task)
	}

	waitgrp.Wait() // wait for all tasks to finish

	debug("Schedule: %v phase finished\n", phase)
}
