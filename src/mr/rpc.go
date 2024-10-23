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

// 定义任务结构体
type TaskRequest struct{}

type TaskType int

type Task struct {
	TaskType  TaskType
	TaskId    int
	Filename  string
	ReduceNum int
}

// 定义各种任务类型
const (
	MapTask TaskType = iota
	ReduceTask
	WaitingTask
	ExitTask
)

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
