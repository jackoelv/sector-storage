package sectorstorage

import (
	"context"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/specs-actors/actors/abi"

	"github.com/filecoin-project/sector-storage/sealtasks"
	"github.com/filecoin-project/sector-storage/stores"
)

type allocSelector struct {
	index stores.SectorIndex
	alloc stores.SectorFileType
	ptype stores.PathType
}

func newAllocSelector(ctx context.Context, index stores.SectorIndex, alloc stores.SectorFileType, ptype stores.PathType) (*allocSelector, error) {
	return &allocSelector{
		index: index,
		alloc: alloc,
		ptype: ptype,
	}, nil
}

func (s *allocSelector) Ok(ctx context.Context, task sealtasks.TaskType, spt abi.RegisteredSealProof, whnd *workerHandle) (bool, error) {
	log.Warnf("jackoelv:selector_alloc:Ok")
	// xerrors.Errorf("jackoelv:selector_alloc:Ok")
	tasks, err := whnd.w.TaskTypes(ctx)
	log.Warnf("jackoelv:selector_alloc:Ok:task: %s", task)

	if err != nil {
		log.Warnf("jackoelv:selector_alloc:Ok:whnd.w.TaskTypes(ctx) error!!!")
		return false, xerrors.Errorf("getting supported worker task types: %w", err)
	}
	if _, supported := tasks[task]; !supported {
		log.Warnf("jackoelv:selector_alloc:Ok:task: %s not supported!!!", task)
		return false, nil
	}

	paths, err := whnd.w.Paths(ctx)
	if err != nil {
		log.Warnf("jackoelv:selector_alloc:Ok:whnd.w.Paths(ctx) error!!!")
		return false, xerrors.Errorf("getting worker paths: %w", err)
	}

	have := map[stores.ID]struct{}{}
	for _, path := range paths {
		have[path.ID] = struct{}{}
	}

	best, err := s.index.StorageBestAlloc(ctx, s.alloc, spt, s.ptype)
	if err != nil {
		log.Warnf("jackoelv:selector_alloc:Ok:s.index.StorageBestAlloc error!!!")
		return false, xerrors.Errorf("finding best alloc storage: %w", err)
	}
	log.Warnf("jackoelv:selector_alloc:Ok:s.index.StorageBestAlloc best: %s", best)
	for _, info := range best {
		if _, ok := have[info.ID]; ok {
			return true, nil
		}
	}
	log.Warnf("jackoelv:selector_alloc:Ok:for error so return false!!!")

	return false, nil
}

func (s *allocSelector) Cmp(ctx context.Context, task sealtasks.TaskType, a, b *workerHandle) (bool, error) {
	log.Warnf("jackoelv:selector_alloc:Cmp,a hostname is:%s; b hostname is %s.", a.info, b.info)
	log.Warnf("jackoelv:selector_alloc:Cmp,a.active.utilization(a.info.Resources):%d", a.active.utilization(a.info.Resources))
	log.Warnf("jackoelv:selector_alloc:Cmp,b.active.utilization(b.info.Resources):%d", b.active.utilization(b.info.Resources))
	return a.active.utilization(a.info.Resources) < b.active.utilization(b.info.Resources), nil
}

var _ WorkerSelector = &allocSelector{}
