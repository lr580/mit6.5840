package featureflag

// lab5D 各种拓展功能的开关
const (
	// EnableKVExactOnce 控制 kvsrv1/kvraft1 是否启用 exactly-once 语义以及相关测试。
	EnableKVExactOnce = false

	// EnableControllerKVraft 控制 shardctrler 是否使用 kvraft 作为配置存储以及相关测试。
	EnableControllerKVraft = false
)
