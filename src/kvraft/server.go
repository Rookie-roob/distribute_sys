package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Rookie-roob/6.824/src/labgob"
	"github.com/Rookie-roob/6.824/src/labrpc"
	"github.com/Rookie-roob/6.824/src/raft"
)

const Debug = 1

const RequstTimeout = 600

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType    string
	Key       string
	Value     string
	ClientID  int64
	CommandID int64
}

type CommandReply struct {
	Value string
	Err   string
}

type CommandResult struct {
	LastCommandID int64
	CommandReply  CommandReply
}

type KVServer struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	commandMap map[int64]CommandResult
	notifyChan map[int]chan CommandReply //log index为key
	kvdata     map[string]string
}

func (kv *KVServer) getNotifyChan(logIndex int) chan CommandReply {
	_, ok := kv.notifyChan[logIndex]
	if !ok {
		kv.notifyChan[logIndex] = make(chan CommandReply)
	} // 万一这一步之后，返回之前，就直接被delete掉这个map条目，因此外面需要加锁
	return kv.notifyChan[logIndex]
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		OpType:    "Get",
		Key:       args.Key,
		Value:     "",
		ClientID:  args.ClientID,
		CommandID: args.CommandID,
	} // 注意这里一定要拷贝一波，不能直接传args的引用到Start中
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = GetErrWrongLeader
		return
	}
	kv.mu.Lock()
	ch := kv.getNotifyChan(index) // 相当于让get chan的这一步原子
	kv.mu.Unlock()
	select {
	case commandReply := <-ch:
		reply.Err = Err(commandReply.Err)
		reply.Value = commandReply.Value
		reply.LeaderID = kv.me
	case <-time.After(time.Duration(RequstTimeout) * time.Millisecond):
		reply.Err = GetErrTimeout
		reply.Value = ""
	}
	// 这里异步是完全没问题的，因为apply的logindex只会往前涨
	go func() {
		kv.mu.Lock()
		delete(kv.notifyChan, index)
		kv.mu.Unlock()
	}()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		OpType:    args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientID:  args.ClientID,
		CommandID: args.CommandID,
	} // 注意这里一定要拷贝一波，不能直接传args的引用到Start中
	kv.mu.RLock()
	if lastReply, ok := kv.commandMap[args.ClientID]; ok && lastReply.LastCommandID >= args.CommandID { //要是重复的话，连raft层都没必要传入，直接返回结果就可以
		reply.Err = Err(lastReply.CommandReply.Err)
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = PutAppendErrWrongLeader
		return
	}
	kv.mu.Lock()
	ch := kv.getNotifyChan(index)
	kv.mu.Unlock()
	select {
	case commandReply := <-ch:
		reply.Err = Err(commandReply.Err)
		reply.LeaderID = kv.me
	case <-time.After(time.Duration(RequstTimeout) * time.Millisecond):
		reply.Err = PutAppendErrTimeout
	}
	// 这里异步是完全没问题的，因为apply的logindex只会往前涨
	go func() {
		kv.mu.Lock()
		delete(kv.notifyChan, index)
		kv.mu.Unlock()
	}()
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) checkCommandID(curCommandID int64, clientID int64) bool { // return true means overlap request
	recordMaxCommand, ok := kv.commandMap[clientID]
	return ok && recordMaxCommand.LastCommandID >= curCommandID
}

func (kv *KVServer) applyToServerStateMachine(op Op) CommandReply {
	commandReply := CommandReply{}
	if op.OpType == "Get" {
		var ok bool
		commandReply.Value, ok = kv.kvdata[op.Key]
		if !ok {
			commandReply.Err = GetErrNoKey
		} else {
			commandReply.Err = GetOK
		}
	} else if op.OpType == "Put" {
		if kv.checkCommandID(op.CommandID, op.ClientID) {
			commandReply = kv.commandMap[op.ClientID].CommandReply
		} else {
			kv.kvdata[op.Key] = op.Value
			commandReply.Err = PutAppendOK
			commandResult := CommandResult{
				LastCommandID: op.CommandID,
				CommandReply:  commandReply,
			}
			kv.commandMap[op.ClientID] = commandResult
		}
	} else {
		if kv.checkCommandID(op.CommandID, op.ClientID) {
			commandReply = kv.commandMap[op.ClientID].CommandReply
		} else {
			_, ok := kv.kvdata[op.Key]
			if !ok {
				kv.kvdata[op.Key] = op.Value
			} else {
				kv.kvdata[op.Key] += op.Value
			}
			commandReply.Err = PutAppendOK
			commandResult := CommandResult{
				LastCommandID: op.CommandID,
				CommandReply:  commandReply,
			}
			kv.commandMap[op.ClientID] = commandResult
		}
	}
	return commandReply
}

func (kv *KVServer) startSnapshot(snapshotLastIndex int) {
	serverdata := kv.encodeSnapshot()
	go kv.rf.DoSnapshot(snapshotLastIndex, serverdata)
}

func (kv *KVServer) processApplyFromRaft() {
	for {
		if kv.killed() {
			break
		}
		select {
		case applyData := <-kv.applyCh:
			if applyData.CommandValid {
				kv.mu.Lock()
				op := applyData.Command.(Op)
				// apply statemachine这个操作是每个server都需要的！
				commandReply := kv.applyToServerStateMachine(op)
				var term int
				var isLeader bool
				term, isLeader = kv.rf.GetState()
				if isLeader && term == applyData.CommandTerm {
					logIndex := applyData.CommandIndex
					ch := kv.getNotifyChan(logIndex)
					ch <- commandReply
				}
				ifSnapshot := kv.checkSnapshot()
				if ifSnapshot {
					kv.startSnapshot(applyData.CommandIndex) //注意这个操作每个server都是有可能主动snapshot的！！！
				}
				kv.mu.Unlock()
			} else {
				kv.decodeKVSnapshot(applyData.Snapshot)
			}
		}
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.commandMap = make(map[int64]CommandResult)
	kv.notifyChan = make(map[int]chan CommandReply)
	kv.kvdata = make(map[string]string)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.decodeKVSnapshot(persister.ReadSnapshot()) // 刚boot或者reboot的时候也需要读取快照，如果之前有快照数据的话就要进行读入
	go kv.processApplyFromRaft()

	return kv
}

// snapshot相关
func (kv *KVServer) checkSnapshot() bool {
	if kv.maxraftstate == -1 {
		return false
	}
	return kv.rf.GetRaftStateSize() >= kv.maxraftstate
}

func (kv *KVServer) encodeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.commandMap)
	e.Encode(kv.kvdata)
	data := w.Bytes()
	return data
}

func (kv *KVServer) decodeKVSnapshot(snapshotKVData []byte) {
	if snapshotKVData == nil || len(snapshotKVData) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(snapshotKVData)
	d := labgob.NewDecoder(r)
	var commandMap map[int64]CommandResult
	var kvdata map[string]string
	if d.Decode(&commandMap) != nil ||
		d.Decode(&kvdata) != nil {
		fmt.Println("decode error!!!")
	} else {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		kv.commandMap = commandMap
		kv.kvdata = kvdata
	}
}
