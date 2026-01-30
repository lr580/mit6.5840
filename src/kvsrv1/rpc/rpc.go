package rpc

type Err string

const (
	// Err's returned by server and Clerk
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrVersion     = "ErrVersion"
	ErrTxnDisabled = "ErrTxnDisabled"
	ErrTxnConflict = "ErrTxnConflict"

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

type RangeArgs struct {
	Low  string
	High string
	Identity
}

type KeyValue struct {
	Key     string
	Value   string
	Version Tversion
}

type RangeReply struct {
	Err Err
	KVs []KeyValue
}

type TxnOpType uint8

const (
	TxnOpGet TxnOpType = iota
	TxnOpPut
)

type TxnCompare struct {
	Key     string
	Version Tversion
}

type TxnOp struct {
	Type  TxnOpType
	Key   string
	Value string
}

type TxnArgs struct {
	Identity
	Compare []TxnCompare
	Success []TxnOp
	Failure []TxnOp
}

type TxnOpResult struct {
	Type TxnOpType
	Get  GetReply
	Put  PutReply
}

type TxnReply struct {
	Err       Err
	Succeeded bool
	Results   []TxnOpResult
}
