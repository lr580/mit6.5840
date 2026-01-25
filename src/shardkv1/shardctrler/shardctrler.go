package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"time"

	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

const configKey = "config/current"

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// Your code here
	// _, ver, err := sck.Get(configKey)
	// if err == rpc.ErrNoKey {
	// 	ver = 0
	// }
	if err := sck.Put(configKey, cfg.String(), 0); err != rpc.OK {
		panic("sck: Init config err " + err)
	}
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	for {
		old := sck.Query()

		// 如果配置号已经超过了目标，说明已经被其他controller更新过了
		if old.Num >= new.Num {
			return
		}

		moves := 0
		// 逐个迁移shard
		allSuccess := true
		for shard := 0; shard < shardcfg.NShards; shard++ {
			oldGid := old.Shards[shard]
			newGid := new.Shards[shard]
			if oldGid == newGid {
				continue
			}
			moves++

			var shardState []byte
			var oldClerk *shardgrp.Clerk

			// Freeze
			if oldGid != 0 {
				oldServers, ok := old.Groups[oldGid]
				if !ok {
					allSuccess = false
					break
				}

				oldClerk = shardgrp.MakeClerk(sck.clnt, oldServers)
				var err rpc.Err
				shardState, err = oldClerk.FreezeShard(shardcfg.Tshid(shard), new.Num)
				if err != rpc.OK {
					allSuccess = false
					break
				}
			}

			// Install
			if newGid != 0 {
				newServers, ok := new.Groups[newGid]
				if !ok {
					allSuccess = false
					break
				}

				newClerk := shardgrp.MakeClerk(sck.clnt, newServers)
				err := newClerk.InstallShard(shardcfg.Tshid(shard), shardState, new.Num)
				if err != rpc.OK {
					allSuccess = false
					break
				}
			}

			// Delete
			if oldClerk != nil {
				if err := oldClerk.DeleteShard(shardcfg.Tshid(shard), new.Num); err != rpc.OK {
					allSuccess = false
					break
				}
			}
		}
		// 如果所有shard都迁移成功，尝试更新配置
		if allSuccess {
			err := sck.Put(configKey, new.String(), rpc.Tversion(old.Num))
			if err == rpc.OK {
				return // 成功更新配置
			}
			// ErrVersion说明配置已被其他controller更新，重试
		}

		// 失败或冲突，重试整个过程
		time.Sleep(100 * time.Millisecond)
	}
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	// Your code here.
	cfg, _, err := sck.Get(configKey)
	if err == rpc.OK {
		return shardcfg.FromString(cfg)
	} // 可能需要重试？ if not ok
	panic("sck: Query err " + err)
}
