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

const (
	configKey     = "config/current"
	nextConfigKey = "config/next"
	retryDelay    = 100 * time.Millisecond
)

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
		current := sck.Query()

		// 如果配置号已经超过了目标，说明已经被其他controller更新过了
		if current.Num >= new.Num {
			return
		}

		if !sck.recordNextConfig(new) {
			time.Sleep(retryDelay)
			continue
		}

		if sck.advanceConfig(current, new) {
			return
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

// advanceConfig migrates shards and attempts to atomically install target as the
// next current configuration.
func (sck *ShardCtrler) advanceConfig(current, target *shardcfg.ShardConfig) bool {
	if !sck.migrateShards(current, target) {
		return false
	}
	return sck.Put(configKey, target.String(), rpc.Tversion(current.Num)) == rpc.OK
}

// recordNextConfig writes the provided configuration to nextConfigKey, ensuring
// future controllers can resume the migration.
func (sck *ShardCtrler) recordNextConfig(cfg *shardcfg.ShardConfig) bool {
	_, ver, err := sck.Get(nextConfigKey)
	switch err {
	case rpc.ErrNoKey:
		ver = 0
	case rpc.OK:
	default:
		return false
	}

	err = sck.Put(nextConfigKey, cfg.String(), ver)
	return err == rpc.OK || err == rpc.ErrNoKey
}

// loadNextConfig deserializes the pending next configuration, if any.
func (sck *ShardCtrler) loadNextConfig() (*shardcfg.ShardConfig, rpc.Err) {
	cfgStr, _, err := sck.Get(nextConfigKey)
	if err != rpc.OK {
		return nil, err
	}
	return shardcfg.FromString(cfgStr), rpc.OK
}

// migrateShards moves shards from current to target and returns true if all
// moves either succeeded or were skipped because RPCs can be retried later.
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
