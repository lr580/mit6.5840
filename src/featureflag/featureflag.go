package featureflag

// lab5D 各种拓展功能的开关

// EnableControllerKVraft 控制 shardctrler 是否使用 kvraft 作为配置存储以及相关测试。
var EnableControllerKVraft = false

// EnableKVExactOnce 控制 kvsrv1/kvraft1 是否启用 exactly-once 语义以及相关测试。
var EnableKVExactOnce = false

// EnableKVRange 控制 kvsrv1 Range 功能是否可用。
var EnableKVRange = false

// EnableKVFastLeaseGet 控制 kvraft 只读 lease 优化及相关测试。
var EnableKVFastLeaseGet = true

// EnableKVTransactions 控制 kvraft 事务扩展和相关测试。
var EnableKVTransactions = false

// EnableShardTransactions 控制 shardkv 跨分片事务扩展和相关测试。
var EnableShardTransactions = false
