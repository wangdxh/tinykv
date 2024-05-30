// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"github.com/pingcap-incubator/tinykv/mylog"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/filter"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"sort"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
	filters      []filter.Filter
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	s.filters = []filter.Filter{filter.StoreStateFilter{ActionScope: s.GetName(), MoveRegion: true}}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	// move region 避免某个store上的regions 太多了
	stores := cluster.GetStores()

	sources := filter.SelectSourceStores(stores, s.filters, cluster)
	targets := filter.SelectTargetStores(stores, s.filters, cluster)

	//cluster.GetPendingRegionsWithLock()

	// 降序排列 多的
	sort.Slice(sources, func(i, j int) bool {
		return sources[i].GetRegionCount() > sources[j].GetRegionCount()
	})
	// 升序排列 少的
	sort.Slice(targets, func(i, j int) bool {
		return targets[i].GetRegionCount() < targets[j].GetRegionCount()
	})

	if len(targets) == 0 {
		return nil
	}

	x := cluster.GetMaxReplicas()
	y := s.opController.OperatorCount(operator.OpBalance)
	mylog.Printf(mylog.LevelTest, "%v %v ", x, y)

	for _, source := range sources {
		// 根据region数 大到小，从store中，找到region， move出去
		var sourceregion *core.RegionInfo
		findfunc := func(container core.RegionsContainer) {
			sourceregion = container.RandomRegion(nil, nil)
		}
		cluster.GetPendingRegionsWithLock(source.GetID(), findfunc)
		if sourceregion == nil {
			cluster.GetFollowersWithLock(source.GetID(), findfunc)
			if sourceregion == nil {
				cluster.GetLeadersWithLock(source.GetID(), findfunc)
			}
		}
		if sourceregion != nil {
			regionstores := cluster.GetRegionStores(sourceregion)
			// 自己的个数都不够，还move啥啊，等着其他的store add
			if len(regionstores) < cluster.GetMaxReplicas() {
				continue
			}

			for _, target := range targets {
				has := false
				for _, regionstore := range regionstores {
					if target.GetID() == regionstore.GetID() {
						has = true
					}
				}
				if has == true {
					continue
				}

				if source.GetRegionSize()-target.GetRegionSize() > 2*sourceregion.GetApproximateSize() {
					newpeer, err := cluster.AllocPeer(target.GetID())
					if err != nil {
						continue
					}
					op, err := operator.CreateMovePeerOperator("balance-region", cluster, sourceregion, operator.OpRegion|operator.OpBalance,
						source.GetID(), target.GetID(), newpeer.Id)
					mylog.Printf(mylog.LevelTest, "region movepeer source %d  region %d  target %d newpper %d",
						source.GetID(), sourceregion.GetID(), target.GetID(), newpeer.Id)
					if err != nil {
						continue
					}
					return op
				}
			}

		}
	}

	//operator.CreateMovePeerOperator()
	return nil
}
