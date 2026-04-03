package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	mu          sync.Mutex
	MapTasks    []Task
	ReduceTasks []Task
	nReduce     int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RequestTask(args *RequestArgs, reply *RequestReply) error {
	// Iterate through map tasks first
	c.mu.Lock()
	defer c.mu.Unlock()

	for i := 0; i < len(c.MapTasks); i++ {
		if c.MapTasks[i].TaskStatus == TaskIdle {
			reply.Task = c.MapTasks[i]
			c.MapTasks[i].TaskStatus = TaskInProgress
			go c.revertFailedMapTask(i)
			return nil
		}
	}

	for i := 0; i < len(c.ReduceTasks); i++ {
		for c.ReduceTasks[i].TaskStatus == TaskIdle {
			reply.Task = c.ReduceTasks[i]
			c.ReduceTasks[i].TaskStatus = TaskInProgress
			go c.revertFailedReduceTask(i)
			return nil
		}
	}

	reply.Task = Task{
		TaskType: WAIT,
	}

	return nil
}

func (c *Coordinator) revertFailedMapTask(taskIndex int) {
	time.Sleep(20 * time.Second)
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.MapTasks[taskIndex].TaskStatus == TaskInProgress {
		c.MapTasks[taskIndex].TaskStatus = TaskIdle
	}
}

func (c *Coordinator) revertFailedReduceTask(taskIndex int) {
	time.Sleep(20 * time.Second)
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.ReduceTasks[taskIndex].TaskStatus == TaskInProgress {
		c.ReduceTasks[taskIndex].TaskStatus = TaskIdle
	}
}

func (c *Coordinator) CompleteMapTask(args *CompleteArgs, reply *CompleteReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := 0; i < len(c.MapTasks); i++ {
		if c.MapTasks[i].File == args.FileName {
			c.MapTasks[i].TaskStatus = TaskComplete
			break
		}
	}
	return nil
}

func (c *Coordinator) CompleteReduceTask(args *CompleteArgs, reply *CompleteReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ReduceTasks[args.ReduceNum].TaskStatus = TaskComplete
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := true

	// Your code here.
	for _, task := range c.MapTasks {
		if task.TaskStatus != TaskComplete {
			return false
		}
	}

	for _, task := range c.ReduceTasks {
		if task.TaskStatus != TaskComplete {
			return false
		}
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce: nReduce,
	}

	// Your code here.

	// Load map tasks
	/*
		We'll load them file-by-file for now, but what if one file is huge?
		We may need to consider splitting them by a set size
	*/
	c.MapTasks = make([]Task, len(files))
	for i, file := range files {
		c.MapTasks[i] = Task{
			TaskType:   1,
			TaskStatus: TaskIdle,
			File:       file,
			NReduce:    nReduce,
		}
	}
	c.ReduceTasks = make([]Task, nReduce)
	for i := 0; i < nReduce; i++ {
		c.ReduceTasks[i] = Task{
			TaskType:   2,
			TaskStatus: TaskIdle,
			JobNum:     i,
		}
	}

	// Remove prior output file
	_ = os.Remove("mr-out-0")

	c.server()
	return &c
}
