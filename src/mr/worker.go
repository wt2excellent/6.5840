package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

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
	// GetTask
	for i := 0; i < 2; i++ {
		task := GetTask()
		DoMapTask(&task, mapf)
	}
}

// Get a Task
func GetTask() TaskResponse {
	taskReq := TaskRequest{}
	taskResp := TaskResponse{}
	ok := call("Coordinator.PullTask", &taskReq, &taskResp)
	if ok {
		fmt.Printf("success get task : %v\n", taskResp)
	} else {
		fmt.Printf("call failed!\n")
	}
	return taskResp
}

func DoMapTask(task *TaskResponse, mapf func(string, string) []KeyValue) {
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
	fmt.Printf("worker is map taskId : %v, fileName : %v\n", task.TaskId, task.Filename)
	file, err := os.Open(task.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", task.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.Filename)
	}
	file.Close()
	intermediate = mapf(task.Filename, string(content))
	reduceNum := task.ReduceNum
	hashKV := make([][]KeyValue, reduceNum)
	for _, value := range intermediate {
		index := ihash(value.Key) % reduceNum
		hashKV[index] = append(hashKV[index], value)
	}
	// 放入中间文件
	for i := 0; i < reduceNum; i++ {
		fileName := "../tempfiles/mr-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i)
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
