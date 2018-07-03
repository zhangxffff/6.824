package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader int
	npeer  int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leader = 0
	ck.npeer = len(servers)
	return ck
}

func (ck *Clerk) checkLeader() {
	var args GetArgs
	var reply GetReply
	for i := 0; i < ck.npeer; i++ {
		ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
		if ok && reply.Err == "" {
			ck.leader = i
			return
		}
	}
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	var args GetArgs
	var reply GetReply
	args.Key = key
	ok := ck.servers[ck.leader].Call("RaftKV.Get", &args, &reply)
	if !ok {
		return ""
	}
	for reply.Err != "" && reply.WrongLeader {
		ck.checkLeader()
		ok = ck.servers[ck.leader].Call("RaftKV.Get", &args, &reply)
		if !ok {
			return ""
		}
	}
	if reply.Err == "" {
		return reply.Value
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	var args PutAppendArgs
	var reply PutAppendReply
	args.Key = key
	args.Op = op
	args.Value = value
	ok := ck.servers[ck.leader].Call("RaftKV.PutAppend", &args, &reply)
	if !ok {
		return ""
	}
	for reply.Err != "" && reply.WrongLeader {
		ck.checkLeader()
		ok = ck.servers[ck.leader].Call("RaftKV.PutAppend", &args, &reply)
		if !ok {
			return ""
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
