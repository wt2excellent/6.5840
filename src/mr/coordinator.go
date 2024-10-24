package mr

import (
	"fmt"
	"log"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

// 定义当前总体进度，分为三个阶段：Map阶段、Reduce阶段、Done阶段
type Phase int

const (
	MapPhase Phase = iota
	ReducePhase
	AllDone
)

// 定义任务状态
type State int

// 保存任务的元数据
type TaskMetaInfo struct {
	State State
	// 传入任务的指针，为了任务从通道中取出来之后，能够通过地址标记这个任务已经完成
	TaskAdr *Task
}

// 任务状态类型
const (
	Working State = iota // 此阶段在工作
	Waiting              // 此阶段在等待执行, 当Map任务没有执行完成此时Reduce任务需要等待
	Done                 // 此阶段已经做完
)

// 任务元信息结构体，主要存储任务进行的状态以及任务对应的地址「以能够随时更改任务状态」
type TaskMetaHolder struct {
	TaskMeta map[int]*TaskMetaInfo
}

func (t *TaskMetaHolder) acceptTaskMetaInfo(taskMetaInfo *TaskMetaInfo) bool {
	taskId := taskMetaInfo.TaskAdr.TaskId
	meta, _ := t.TaskMeta[taskId]
	if meta != nil {
		fmt.Printf("[acceptTaskMetaInfo] contain task which taskId : %v\n", taskId)
		return false
	} else {
		//return false
		t.TaskMeta[taskId] = taskMetaInfo
	}
	return true
}

func (t *TaskMetaHolder) judgeTaskState(taskId int) bool {
	taskInfo, ok := t.TaskMeta[taskId]
	if !ok || taskInfo.State != Waiting {
		return false
	}
	taskInfo.State = Working
	return true
}

func (t *TaskMetaHolder) allTaskDone() bool {
	// 检查任务是否已经全部完成
	var (
		mapDoneNum      = 0
		mapUnDoneNum    = 0
		reduceDoneNum   = 0
		reduceUnDoneNum = 0
	)
	taskMeta := t.TaskMeta
	for _, taskMetaInfo := range taskMeta {
		if taskMetaInfo.TaskAdr.TaskType == MapTask {
			if taskMetaInfo.State == Done {
				mapDoneNum++
			} else {
				mapUnDoneNum++
			}
		} else if taskMetaInfo.TaskAdr.TaskType == ReduceTask {
			if taskMetaInfo.State == Done {
				reduceDoneNum++
			} else {
				reduceUnDoneNum++
			}
		}
	}
	if (mapDoneNum > 0 && mapUnDoneNum == 0) && (reduceDoneNum == 0 && reduceUnDoneNum == 0) {
		return true
	} else {
		if reduceDoneNum > 0 && reduceUnDoneNum == 0 {
			return true
		}
	}

	return false
}

type Coordinator struct {
	// Your definitions here.
	Phase          Phase
	MapChan        chan *Task
	ReduceChan     chan *Task
	ReduceNum      int
	Files          []string
	TaskId         int // 这个字段主要作用生成递增ID
	TaskMetaHolder TaskMetaHolder
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
	if c.Phase == AllDone {
		fmt.Println("All tasks have Done")
		ret = true
	} else {
		//fmt.Println("Not All tasks have Done")
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Phase:      MapPhase,
		MapChan:    make(chan *Task, len(files)),
		ReduceChan: make(chan *Task, nReduce),
		ReduceNum:  nReduce,
		Files:      files,
		TaskMetaHolder: TaskMetaHolder{
			TaskMeta: make(map[int]*TaskMetaInfo, nReduce+len(files)),
		},
		TaskId: 0,
	}

	// Your code here.
	c.MakeMapTask(files)
	c.server()
	return &c
}

func (c *Coordinator) MakeMapTask(files []string) {
	for _, file := range files {
		taskID := c.genTaskId()
		task := Task{
			TaskType:  MapTask,
			TaskId:    taskID,
			Filename:  file,
			ReduceNum: c.ReduceNum,
		}

		// 保存任务初始状态
		taskMetaInfo := TaskMetaInfo{State: Waiting, TaskAdr: &task}
		c.TaskMetaHolder.acceptTaskMetaInfo(&taskMetaInfo)
		c.MapChan <- &task
	}
}

/*
* 为什么需要一个全局唯一ID生成器，主要Map的worker个数为len(files), Reduce的worker个数为ReduceNum个，
* Coordinator中有一个属性TaskMetaHolder用于保存任务的元数据，更内层使用一个map表格存储各个任务的元信息，key为任务ID，同时任务总数为
* Map对应的worker+Reduce对应的worker，所以需要使用一个全局任务Id生成器，生成递增的任务ID
 */
func (c *Coordinator) genTaskId() int {
	res := c.TaskId
	c.TaskId++
	return res
}

func (c *Coordinator) PullTask(taskReq *TaskRequest, taskResp *Task) error {
	phase := c.Phase
	switch phase {
	case MapPhase:
		{
			if len(c.MapChan) > 0 {
				*taskResp = *<-c.MapChan
				if !c.TaskMetaHolder.judgeTaskState(taskResp.TaskId) {
					fmt.Println("[PullTask] task state is ", c.TaskMetaHolder.TaskMeta[taskResp.TaskId].State)
				}
			} else {
				// Map对应的任务被分发完了，但此时任务并没有全部完成，此时将任务状态设置为waiting状态
				taskResp.TaskType = WaitingTask
				// 检查Map任务是否都完成,完成后将流程进入Reduce阶段
				if c.TaskMetaHolder.allTaskDone() {
					c.toNextPhase()
				}
				return nil
			}
		}
	case ReducePhase:
		{

		}
	case AllDone:
		{

		}
	default:
		c.Phase = AllDone
	}
	return nil
}

func (c *Coordinator) MarkDone(task *Task, taskResp *Task) error {
	switch task.TaskType {
	case MapTask:
		{
			metaInfo, ok := c.TaskMetaHolder.TaskMeta[task.TaskId]
			if ok && metaInfo.State == Working {
				metaInfo.State = Done
				fmt.Println("[MarkDone] task is done, the taskId is ", task.TaskId)
			} else {
				fmt.Println("[MarkDone] error, the task not to be done ", task.TaskId)
			}
		}
	default:
		{

		}
	}
	return nil
}

func (c *Coordinator) toNextPhase() {
	switch c.Phase {
	case MapPhase:
		{
			//暂时将任务状态全部设置为已完成
			c.Phase = AllDone
		}
	case ReducePhase:
		{
			c.Phase = AllDone
		}
	}
}
