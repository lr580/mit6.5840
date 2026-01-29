package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"time"

	"6.5840/featureflag"
	kvraft "6.5840/kvraft1"
	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

const (
	controllerReplicasKvraf = 3
	controllerReplicasKVsrv = 1
	configKey               = "config/current"
	nextConfigKey           = "config/next"
	retryDelay              = 100 * time.Millisecond
)

var ControllerUseKVraft = featureflag.EnableControllerKVraft

var ControllerReplicas = func() int {
	if ControllerUseKVraft {
		return controllerReplicasKvraf
	}
	return controllerReplicasKVsrv
}()

type recordResult int

const (
	recordRetry recordResult = iota
	recordWait
	recordProceed
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
}

// Make a ShardCltler, which stores its state in a kvraft-based kvsrv when
// ControllerUseKVraft is true, otherwise fall back to the lab2 kvsrv.
func MakeShardCtrler(clnt *tester.Clnt, servers []string) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	if ControllerUseKVraft {
		sck.IKVClerk = kvraft.MakeClerk(clnt, servers)
	} else {
		target := tester.ServerName(tester.GRP0, 0)
		if len(servers) > 0 {
			target = servers[0]
		}
		sck.IKVClerk = kvsrv.MakeClerk(clnt, target)
	}
	// Your code here.
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
	for {
		current := sck.Query()

		next, err := sck.loadNextConfig()
		if err == rpc.ErrNoKey {
			return
		}
		if err != rpc.OK {
			time.Sleep(retryDelay)
			continue
		}
		if next.Num <= current.Num {
			return
		}

		if sck.advanceConfig(current, next) {
			continue
		}
		time.Sleep(retryDelay)
	}
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
	cfgStr := cfg.String()
	for {
		switch err := sck.Put(configKey, cfgStr, 0); err {
		case rpc.OK:
			return
		case rpc.ErrMaybe, rpc.ErrVersion:
			if cur, _, gErr := sck.Get(configKey); gErr == rpc.OK && cur == cfgStr {
				return
			}
		default:
			time.Sleep(retryDelay)
		}
	}
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	for {
		current := sck.Query()

		// 如果配置号已经超过了目标，说明已经被其他controller更新过了
		if current.Num >= new.Num {
			return
		}

		switch sck.recordNextConfig(current, new) {
		case recordProceed:
			if sck.advanceConfig(current, new) {
				return
			}
		case recordRetry:
		case recordWait:
		}

		time.Sleep(retryDelay)
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

// advanceConfig 负责迁移分片，并尝试用原子方式将 target 写成新的 current。
func (sck *ShardCtrler) advanceConfig(current, target *shardcfg.ShardConfig) bool {
	if !sck.migrateShards(current, target) {
		return false
	}
	return sck.Put(configKey, target.String(), rpc.Tversion(current.Num)) == rpc.OK
}

// recordNextConfig 将目标配置写入 nextConfigKey，保证后续 controller 能接力迁移。
func (sck *ShardCtrler) recordNextConfig(current, target *shardcfg.ShardConfig) recordResult {
	targetStr := target.String()

	nextStr, ver, err := sck.Get(nextConfigKey)
	if err != rpc.OK && err != rpc.ErrNoKey {
		return recordRetry
	}

	if err == rpc.ErrNoKey {
		ver = 0
	} else {
		nextCfg := shardcfg.FromString(nextStr)
		switch {
		case nextCfg.Num > target.Num:
			return recordWait
		case nextCfg.Num == target.Num:
			if nextStr == targetStr {
				return recordProceed
			}
			return recordWait
		case nextCfg.Num > current.Num:
			return recordWait
		case nextCfg.Num <= current.Num:
			// 这是旧记录，可以被覆盖
		}
	}

	switch err := sck.Put(nextConfigKey, targetStr, ver); err {
	case rpc.OK:
		return recordProceed
	case rpc.ErrVersion, rpc.ErrNoKey:
		return recordWait
	case rpc.ErrMaybe:
		return recordRetry
	default:
		return recordRetry
	}
}

// loadNextConfig 读取并反序列化挂起的下一份配置。
func (sck *ShardCtrler) loadNextConfig() (*shardcfg.ShardConfig, rpc.Err) {
	cfgStr, _, err := sck.Get(nextConfigKey)
	if err != rpc.OK {
		return nil, err
	}
	return shardcfg.FromString(cfgStr), rpc.OK
}

// migrateShards 将分片从 current 迁移到 target，所有移动成功或可重试时返回 true。
func (sck *ShardCtrler) migrateShards(current, target *shardcfg.ShardConfig) bool {
	for shard := 0; shard < shardcfg.NShards; shard++ {
		oldGid := current.Shards[shard]
		newGid := target.Shards[shard]
		if oldGid == newGid {
			continue
		}

		var shardState []byte
		var oldClerk *shardgrp.Clerk
		shouldDelete := false

		if oldGid != 0 {
			if servers, ok := sck.lookupOldGroup(current, target, oldGid); ok {
				oldClerk = shardgrp.MakeClerk(sck.clnt, servers)
				var err rpc.Err
				shardState, err = oldClerk.FreezeShard(shardcfg.Tshid(shard), target.Num)
				if err == rpc.OK {
					shouldDelete = true
				} else {
					shardState = nil
				}
			}
		}

		if newGid != 0 {
			newServers, ok := target.Groups[newGid]
			if !ok {
				return false
			}

			newClerk := shardgrp.MakeClerk(sck.clnt, newServers)
			if err := newClerk.InstallShard(shardcfg.Tshid(shard), shardState, target.Num); err != rpc.OK && err != rpc.ErrWrongGroup {
				return false
			}
		}

		if shouldDelete && oldClerk != nil {
			if err := oldClerk.DeleteShard(shardcfg.Tshid(shard), target.Num); err != rpc.OK && err != rpc.ErrWrongGroup {
				// Delete failure doesn't block progress; shard already installed.
			}
		}
	}
	return true
}

func (sck *ShardCtrler) lookupOldGroup(current, target *shardcfg.ShardConfig, gid tester.Tgid) ([]string, bool) {
	if servers, ok := target.Groups[gid]; ok {
		return servers, true
	}
	if servers, ok := current.Groups[gid]; ok {
		return servers, true
	}
	return nil, false
}
