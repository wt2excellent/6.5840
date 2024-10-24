package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type BySort []KeyValue

// for sorting by key.
func (a BySort) Len() int           { return len(a) }
func (a BySort) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a BySort) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()
	// ----------------------------------------第一次实现----------------------------------
	// GetTask
	//for i := 0; i < 2; i++ {
	//	task := GetTask()
	//	DoMapTask(&task, mapf)
	//}
	// ----------------------------------------第二次调整-----------------------------------
	// 对任务状态加入枚举
	flag := true
	for flag {
		task := GetTask()
		switch task.TaskType {
		case MapTask:
			{
				DoMapTask(&task, mapf)
				TaskDone(&task)
			}
		case ReduceTask:
			{
				DoReduceTask(&task, reducef)
				TaskDone(&task)
			}
		case WaitingTask:
			{
				fmt.Printf("the task is waiting, taskId : %v\n\n", task.TaskId)
				time.Sleep(1 * time.Second)
			}
		case ExitTask:
			{
				fmt.Println("Exit Task")
				flag = false
			}
		}
	}
}

// Get a Task
func GetTask() Task {
	taskReq := TaskRequest{}
	taskResp := Task{}
	ok := call("Coordinator.PullTask", &taskReq, &taskResp)
	if ok {
		fmt.Printf("success get task : %v\n", taskResp)
	} else {
		fmt.Printf("call failed!\n")
	}
	return taskResp
}

func DoMapTask(task *Task, mapf func(string, string) []KeyValue) {
	/***
	Map任务主要流程分为三部分：
	1. 根据RPC调用获取到文件名，调用已经写好的Map函数生成中间文件(intermediate)
	2. 根据RPC返回的任务参数中的ReduceNum的数值对中间文件(intermediate)进行分组,
		分组依据根据字母结构体的Key取Hash值随后对ReduceNum取余。
	3. 将分组后的中间文件保存到临时文件中
	---------------------个人理解-----------------------------------------
	生成的临时文件名：mr-X-Y(X是文件id，Y是哈希后对应ReduceNum)
	对于文件Id就是运行coordinator传入第二个参数files(文件集)中文件的顺序
	也就是说Map函数的主要功能是将传入的某个文件的词频统计出来(准确的说并没有统计词频，可以阅读wc.go源代码)仅仅将「单词-1」统计出来
		for _, w := range words {
		kv := mr.KeyValue{w, "1"}
		kva = append(kva, kv)
	随后将某个文件的所有词频根据Key也就是单词，根据单词的哈希值将单词分组，分组个数为ReduceNum
	*/
	var intermediate []KeyValue
	fmt.Printf("worker is map taskId : %v, fileName : %v\n", task.TaskId, task.FileSlice[0])
	file, err := os.Open(task.FileSlice[0])
	if err != nil {
		log.Fatalf("cannot open %v", task.FileSlice[0])
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.FileSlice[0])
	}
	file.Close()
	intermediate = mapf(task.FileSlice[0], string(content))
	reduceNum := task.ReduceNum
	hashKV := make([][]KeyValue, reduceNum)
	for _, value := range intermediate {
		index := ihash(value.Key) % reduceNum
		hashKV[index] = append(hashKV[index], value)
	}
	// 放入中间文件
	for i := 0; i < reduceNum; i++ {
		fileName := "mr-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i)
		tempFile, err := os.Create(fileName)
		if err != nil {
			log.Fatalf("create temp file: %v failed.", fileName)
		}
		enc := json.NewEncoder(tempFile)
		for _, kv := range hashKV[i] {
			err = enc.Encode(&kv)
			if err != nil {
				log.Fatalf("encode error: %v", err)
			}
		}
		tempFile.Close()
	}
}

func DoReduceTask(task *Task, reducef func(string, []string) string) {
	reduceFileNum := task.TaskId
	intermediate := shuffle(task.FileSlice)
	dir, _ := os.Getwd()
	//tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}
	i := 0
	// Debug
	//fmt.Printf("the intermediate length is %v\n", len(intermediate))
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tempFile.Close()
	fn := fmt.Sprintf("mr-out-%d", reduceFileNum)
	os.Rename(tempFile.Name(), fn)
}

func shuffle(files []string) []KeyValue {
	var kva []KeyValue
	for _, filepath := range files {
		file, _ := os.Open(filepath)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
		// 删除临时文件
		//os.Remove(filepath)
	}
	sort.Sort(BySort(kva))
	return kva
}

func TaskDone(task *Task) {
	taskReq := task
	taskResp := Task{}
	ok := call("Coordinator.MarkDone", &taskReq, &taskResp)
	if ok {
		fmt.Printf("success mark task : %v\n", taskReq)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
