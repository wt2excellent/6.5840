package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
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
	// 添加任务开始执行时间
	StartTime time.Time
	State     State
	// 传入任务的指针，为了任务从通道中取出来之后，能够通过地址标记这个任务已经完成
	TaskAdr *Task
}

// 任务状态类型
// 任务对应的三种状态如何切换的：初始任务时将所有任务状态设置为Waiting，
// worker调用rpc执行某个任务时，任务状态由Waiting==>Working
// worker执行任务完成后调用rpc将任务状态有Working==>Done，至此任务完成，上面后两条暂时仅仅针对Map任务
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
	taskInfo.StartTime = time.Now()
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
	mu             sync.Mutex
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
	c.mu.Lock()
	defer c.mu.Unlock()
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
	// 开启一个探测器，监测任务执行时间是否过长
	go c.crashHandler()
	return &c
}

func (c *Coordinator) MakeMapTask(files []string) {
	for _, file := range files {
		taskID := c.genTaskId()
		task := Task{
			TaskType:  MapTask,
			TaskId:    taskID,
			FileSlice: []string{file},
			ReduceNum: c.ReduceNum,
		}

		// 保存任务初始状态
		taskMetaInfo := TaskMetaInfo{State: Waiting, TaskAdr: &task}
		c.TaskMetaHolder.acceptTaskMetaInfo(&taskMetaInfo)
		c.MapChan <- &task
	}
}

func (c *Coordinator) MakeReduceTask() {
	for reduceNum := 0; reduceNum < c.ReduceNum; reduceNum++ {
		taskID := c.genTaskId()
		task := Task{
			TaskType:  ReduceTask,
			TaskId:    taskID,
			FileSlice: c.selectReduceNum(reduceNum),
			ReduceNum: c.ReduceNum,
		}
		// 保存任务初始状态
		taskMetaInfo := TaskMetaInfo{State: Waiting, TaskAdr: &task}
		c.TaskMetaHolder.acceptTaskMetaInfo(&taskMetaInfo)
		c.ReduceChan <- &task
	}
}

func (c *Coordinator) selectReduceNum(reduceNum int) []string {
	var res []string
	path, _ := os.Getwd()
	files, err := ioutil.ReadDir(path)
	if err != nil {
		log.Fatal("[selectReduceNum] failure", err)
		return res
	}
	for _, file := range files {
		if strings.HasPrefix(file.Name(), "mr-") && strings.HasSuffix(file.Name(), strconv.Itoa(reduceNum)) {
			res = append(res, file.Name())
		}
	}
	return res
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

	c.mu.Lock()
	defer c.mu.Unlock()
	switch c.Phase {
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
			if len(c.ReduceChan) > 0 {
				*taskResp = *<-c.ReduceChan
				if !c.TaskMetaHolder.judgeTaskState(taskResp.TaskId) {
					fmt.Println("[PullTask] task state is ", c.TaskMetaHolder.allTaskDone())
				}
			} else {
				taskResp.TaskType = WaitingTask
				if c.TaskMetaHolder.allTaskDone() {
					c.toNextPhase()
				}
				return nil
			}
		}
	case AllDone:
		{
			taskResp.TaskType = ExitTask
		}
	default:
		panic("[PullTask] invalid Phase")
	}
	return nil
}

func (c *Coordinator) MarkDone(task *Task, taskResp *Task) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch task.TaskType {
	case MapTask:
		{
			metaInfo, ok := c.TaskMetaHolder.TaskMeta[task.TaskId]
			if ok && metaInfo.State == Working {
				metaInfo.State = Done
				fmt.Printf("[MarkDone] task is done, the taskId is: %v, the taskType is %v\n", task.TaskId, task.TaskType)
			} else {
				fmt.Printf("[MarkDone] error, the task not to be done, taskId id : %v, the tasktype is %v\n ", task.TaskId, task.TaskType)
			}
			break
		}
	case ReduceTask:
		{
			metaInfo, ok := c.TaskMetaHolder.TaskMeta[task.TaskId]
			if ok && metaInfo.State == Working {
				metaInfo.State = Done
				fmt.Printf("[MarkDone] task is done, the taskId is: %v, the taskType is %v\n", task.TaskId, task.TaskType)
			} else {
				fmt.Printf("[MarkDone] error, the task not to be done, taskId id : %v, the tasktype is %v\n ", task.TaskId, task.TaskType)
			}
			break
		}
	default:
		{
			panic("[MarkDone] invalid TaskType")
		}
	}
	return nil
}

func (c *Coordinator) toNextPhase() {
	switch c.Phase {
	case MapPhase:
		{
			//暂时将任务状态全部设置为已完成
			//c.Phase = AllDone
			c.MakeReduceTask()
			c.Phase = ReducePhase
		}
	case ReducePhase:
		{
			c.Phase = AllDone
		}
	default:
		panic("[toNextPhase] invalid phase")
	}
}

func (c *Coordinator) crashHandler() {
	for {
		// 关于这个休眠时间的思考：
		// 如果不设置这个休眠时间，可能导致探测器协程不断获取锁，释放锁，不断循环，从而导致分发任务的方法PullTask无法获取锁
		// 从而无法执行后续任务，这里类似时间片算法的使用了。
		time.Sleep(2 * time.Second)
		c.mu.Lock()
		if c.Phase == AllDone {
			c.mu.Unlock()
			break
		}
		for _, metaInfo := range c.TaskMetaHolder.TaskMeta {
			if metaInfo.State == Working && time.Since(metaInfo.StartTime) > 9*time.Second {
				if metaInfo.TaskAdr.TaskType == MapTask {
					c.MapChan <- metaInfo.TaskAdr
					metaInfo.State = Waiting
				} else if metaInfo.TaskAdr.TaskType == ReduceTask {
					c.ReduceChan <- metaInfo.TaskAdr
					metaInfo.State = Waiting
				}
			}
		}
		c.mu.Unlock()
	}
}
