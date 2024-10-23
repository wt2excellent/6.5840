package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	State      int // 如Map/Reduce, 当前0表示Map
	MapChan    chan *TaskResponse
	ReduceChan chan *TaskResponse
	ReduceNum  int
	Files      []string
}

// Your code here -- RPC handlers for the worker to call.

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
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		State:      0,
		MapChan:    make(chan *TaskResponse, len(files)),
		ReduceChan: make(chan *TaskResponse, nReduce),
		ReduceNum:  nReduce,
		Files:      files,
	}

	// Your code here.
	c.MakeMapTask(files)
	c.server()
	return &c
}

func (c *Coordinator) MakeMapTask(files []string) {
	for i, file := range files {
		task := TaskResponse{
			TaskType:  0,
			TaskId:    i,
			Filename:  file,
			ReduceNum: c.ReduceNum,
		}
		c.MapChan <- &task
	}
}

func (c *Coordinator) PullTask(taskReq *TaskRequest, taskResp *TaskResponse) error {
	*taskResp = *<-c.MapChan
	return nil
}
