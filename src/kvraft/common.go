package kvraft

const (
	GetOK             = "GetOK"
	GetErrNoKey       = "GetErrNoKey"
	GetErrWrongLeader = "GetErrWrongLeader"
	GetErrTimeout     = "GetErrTimeout"

	PutAppendOK             = "PutAppendOK"
	PutAppendErrTimeout     = "PutAppendErrTimeout"
	PutAppendErrWrongLeader = "PutAppendErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	CommandID int64
	ClientID  int64
}

type PutAppendReply struct {
	Err      Err
	LeaderID int
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	CommandID int64
	ClientID  int64
}

type GetReply struct {
	Err      Err
	Value    string
	LeaderID int
}
