package rpc

type Err string

const (
	// Err's returned by server and Clerk
	OK         = "OK"
	ErrNoKey   = "ErrNoKey"
	ErrVersion = "ErrVersion"

	// Err returned by Clerk only
	ErrMaybe = "ErrMaybe"

	// For future kvraft lab
	ErrWrongLeader = "ErrWrongLeader"
	ErrWrongGroup  = "ErrWrongGroup"
)

type Tversion uint64

type Identity struct {
	ClientId  int64
	RequestId int64
}

type Identifiable interface { //练一下接口语法
	IDs() (int64, int64)
}

func (id Identity) IDs() (int64, int64) { return id.ClientId, id.RequestId }

type PutArgs struct {
	Key     string
	Value   string
	Version Tversion
	Identity
}

type PutReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	Identity
}

type GetReply struct {
	Value   string
	Version Tversion
	Err     Err
}
