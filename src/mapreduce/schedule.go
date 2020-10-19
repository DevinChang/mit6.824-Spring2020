package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
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

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	// 每个worker有n个task
	var wg sync.WaitGroup // waitGroup等待go routine的集合完成
	for i := 0; i < ntasks; i++ {
		// 递增waitgroup计数器
		wg.Add(1)
		// 启动go routine
		// 每个worker需要调用call rpc来完成
		dotaskarg := DoTaskArgs{
			JobName:       jobName,
			File:          mapFiles[i],
			Phase:         phase,
			TaskNumber:    i,
			NumOtherPhase: n_other}

		go func() {
			defer wg.Done()
			// 用死循环来表示当一个worker崩溃时，用另一个worker来工作
			for {
				worker := <-registerChan
				// 如果调用成功，就将worker返还
				if call(worker, "Worker.DoTask", &dotaskarg, nil) == true {
					// 注意！！！！ 要完成之后将worker返回
					go func() {
						registerChan <- worker
					}()
					// worker执行成功后跳出死循环
					break
				}
			}
		}()

	}
	// 等待所有worker
	wg.Wait()

	fmt.Printf("Schedule: %v done\n", phase)
}
