package kvsrv

import (
	"6.5840/labrpc"
	"strconv"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	getArgs := GetArgs{
		Key: key,
	}
	getReply := GetReply{}
	for !ck.server.Call("KVServer.Get", &getArgs, &getReply) {
	}
	return getReply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// You will have to modify this function.
	identifyInt64 := nrand()
	putAppendArgs := PutAppendArgs{
		Key:         key,
		Value:       value,
		Identity:    strconv.Itoa(int(identifyInt64)),
		OperateType: Modify,
	}
	putAppendReply := PutAppendReply{}
	for !ck.server.Call("KVServer."+op, &putAppendArgs, &putAppendReply) {
		// 加一点睡眠的代码，等待一个间隔后再次发起RPC调用
	}
	deleteArgs := PutAppendArgs{
		Identity:    strconv.Itoa(int(identifyInt64)),
		OperateType: Delete,
	}
	for !ck.server.Call("KVServer."+op, &deleteArgs, &PutAppendReply{}) {
		// 加一点睡眠的代码，等待一个间隔后再次发起RPC调用
	}
	return putAppendReply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
