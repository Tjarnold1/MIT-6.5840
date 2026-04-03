package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type TaskStatus string

const (
	TaskIdle       TaskStatus = "idle"
	TaskInProgress TaskStatus = "in_progress"
	TaskComplete   TaskStatus = "complete"
)

type TaskType int

const (
	UNKNOWN = iota
	MAP
	REDUCE
	WAIT
)

type Task struct {
	TaskType
	TaskStatus
	File    string
	NReduce int
	JobNum  int
}

type RequestArgs struct {
}

type RequestReply struct {
	Task
}

type CompleteArgs struct {
	FileName  string
	ReduceNum int
}

type CompleteReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
