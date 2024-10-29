package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu          sync.Mutex
	kvMap       map[string]string
	identifySet map[string]string
	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.kvMap[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// Your code here.
	if args.OperateType == Delete {
		delete(kv.identifySet, args.Identity)
		return
	}
	if _, ok := kv.identifySet[args.Identity]; ok {
		return
	}
	kv.identifySet[args.Identity] = ""
	kv.kvMap[args.Key] = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.OperateType == Delete {
		delete(kv.identifySet, args.Identity)
		return
	}
	if _, ok := kv.identifySet[args.Identity]; ok {
		reply.Value = kv.identifySet[args.Identity]
		return
	}
	key := args.Key
	val := args.Value
	oldVal, _ := kv.kvMap[key]
	kv.kvMap[key] = oldVal + val
	reply.Value = oldVal
	kv.identifySet[args.Identity] = oldVal
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.kvMap = make(map[string]string)
	kv.identifySet = make(map[string]string)
	// You may need initialization code here.

	return kv
}
