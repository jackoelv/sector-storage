package sectorstorage

import (
	"container/heap"
	"context"
	"sort"
	"sync"
	"time"

	"github.com/hashicorp/go-multierror"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/specs-actors/actors/abi"

	"github.com/filecoin-project/sector-storage/sealtasks"
	"github.com/filecoin-project/sector-storage/storiface"
)

type schedPrioCtxKey int

var SchedPriorityKey schedPrioCtxKey
var DefaultSchedPriority = 0

func getPriority(ctx context.Context) int {
	sp := ctx.Value(SchedPriorityKey)
	if p, ok := sp.(int); ok {
		return p
	}

	return DefaultSchedPriority
}

func WithPriority(ctx context.Context, priority int) context.Context {
	return context.WithValue(ctx, SchedPriorityKey, priority)
}

const mib = 1 << 20

type WorkerAction func(ctx context.Context, w Worker) error

type WorkerSelector interface {
	Ok(ctx context.Context, task sealtasks.TaskType, spt abi.RegisteredSealProof, a *workerHandle) (bool, error) // true if worker is acceptable for performing a task

	Cmp(ctx context.Context, task sealtasks.TaskType, a, b *workerHandle) (bool, error) // true if a is preferred over b
}

<<<<<<< Updated upstream
type sectorsNum int
=======
//add by jackoelv
type workerState struct {
	sector   abi.SectorID
	taskType sealtasks.TaskType
}
>>>>>>> Stashed changes

type scheduler struct {
	spt abi.RegisteredSealProof

	workersLk  sync.Mutex
	nextWorker WorkerID
	workers    map[WorkerID]*workerHandle

<<<<<<< Updated upstream
	workerSectorNum  map[WorkerID]sectorsNum   //add by jackoelv
	workerBySectorID map[abi.SectorID]WorkerID //add by jackoelv
=======
	workerParallelSectors map[WorkerID]int           //add by jackoelv
	workerDoing           map[WorkerID][]workerState //add by jackoelv
>>>>>>> Stashed changes

	newWorkers chan *workerHandle

	watchClosing  chan WorkerID
	workerClosing chan WorkerID

	schedule   chan *workerRequest
	workerFree chan WorkerID
	closing    chan struct{}

	schedQueue *requestQueue
}

func newScheduler(spt abi.RegisteredSealProof) *scheduler {
	return &scheduler{
		spt: spt,

		nextWorker: 0,
		workers:    map[WorkerID]*workerHandle{},

<<<<<<< Updated upstream
		workerSectorNum:  map[WorkerID]sectorsNum{},   //add by jackoelv
		workerBySectorID: map[abi.SectorID]WorkerID{}, //add by jackoelv
=======
		workerParallelSectors: map[WorkerID]int{},
		workerDoing:           map[WorkerID][]workerState{},
>>>>>>> Stashed changes

		newWorkers: make(chan *workerHandle),

		watchClosing:  make(chan WorkerID),
		workerClosing: make(chan WorkerID),

		schedule:   make(chan *workerRequest),
		workerFree: make(chan WorkerID),
		closing:    make(chan struct{}),

		schedQueue: &requestQueue{},
	}
}

func (sh *scheduler) Schedule(ctx context.Context, sector abi.SectorID, taskType sealtasks.TaskType, sel WorkerSelector, prepare WorkerAction, work WorkerAction) error {
	ret := make(chan workerResponse)

	select {
	case sh.schedule <- &workerRequest{
		sector:   sector,
		taskType: taskType,
		priority: getPriority(ctx),
		sel:      sel,

		prepare: prepare,
		work:    work,

		ret: ret,
		ctx: ctx,
	}:
	case <-sh.closing:
		return xerrors.New("closing")
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case resp := <-ret:
		return resp.err
	case <-sh.closing:
		return xerrors.New("closing")
	case <-ctx.Done():
		return ctx.Err()
	}
}

type workerRequest struct {
	sector   abi.SectorID
	taskType sealtasks.TaskType
	priority int // larger values more important
	sel      WorkerSelector

	prepare WorkerAction
	work    WorkerAction

	index int // The index of the item in the heap.

	ret chan<- workerResponse
	ctx context.Context
}

type workerResponse struct {
	err error
}

func (r *workerRequest) respond(err error) {
	select {
	case r.ret <- workerResponse{err: err}:
	case <-r.ctx.Done():
		log.Warnf("request got cancelled before we could respond")
	}
}

type activeResources struct {
	memUsedMin uint64
	memUsedMax uint64
	gpuUsed    bool
	cpuUse     uint64

	cond *sync.Cond
}

type workerHandle struct {
	w Worker

	info storiface.WorkerInfo

	preparing *activeResources
	active    *activeResources
}

func (sh *scheduler) runSched() {
	go sh.runWorkerWatcher()

	for {
		select {
		case w := <-sh.newWorkers:
			// log.Warnf("jackoelv:sched:newWorkers")
			sh.schedNewWorker(w)
		case wid := <-sh.workerClosing:
			sh.schedDropWorker(wid)
		case req := <-sh.schedule:
			// log.Warnf("jackoelv:sched:schedule then maybeSchedRequest")
			scheduled, err := sh.maybeSchedRequest(req)
			if err != nil {
				req.respond(err)
				continue
			}
			if scheduled {
				continue
			}

			heap.Push(sh.schedQueue, req)
		case wid := <-sh.workerFree:
			sh.onWorkerFreed(wid)
		case <-sh.closing:
			sh.schedClose()
			return
		}
	}
}

func (sh *scheduler) onWorkerFreed(wid WorkerID) {
	sh.workersLk.Lock()
	w, ok := sh.workers[wid]
	sh.workersLk.Unlock()
	if !ok {
		log.Warnf("onWorkerFreed on invalid worker %d", wid)
		return
	}

	for i := 0; i < sh.schedQueue.Len(); i++ {
		req := (*sh.schedQueue)[i]

		ok, err := req.sel.Ok(req.ctx, req.taskType, sh.spt, w)
		if err != nil {
			log.Errorf("onWorkerFreed req.sel.Ok error: %+v", err)
			continue
		}

		if !ok {
			continue
		}

		scheduled, err := sh.maybeSchedRequest(req)
		if err != nil {
			req.respond(err)
			continue
		}

		if scheduled {
			heap.Remove(sh.schedQueue, i)
			i--
			continue
		}
	}
}

var selectorTimeout = 5 * time.Second

func (sh *scheduler) maybeSchedRequest(req *workerRequest) (bool, error) {
	// log.Warnf("jackoelv:sched:maybeSchedRequest")
	sh.workersLk.Lock()
	defer sh.workersLk.Unlock()

	tried := 0
	var acceptable []WorkerID

	needRes := ResourceTable[req.taskType][sh.spt]

	//add by jackoelv begin

	if req.taskType != sealtasks.TTAddPiece && req.taskType != sealtasks.TTFetch && req.taskType != sealtasks.TTUnseal && req.taskType != sealtasks.TTReadUnsealed {
		wid := sh.workerBySectorID[req.sector]
		if (wid > 0) && (sh.workerSectorNum[wid-1] <= 3) {
			log.Warnf("jackoelv:maybeSchedRequest:!!!NOT!!!:sectorNumber: %s,taskType: %s, wid: %d, totalSectorsNum:%s", req.sector.Number, req.taskType, wid-1, sh.workerSectorNum[wid-1])
			return true, sh.assignWorker(wid-1, sh.workers[wid-1], req)
		}
	}
	if req.taskType == sealtasks.TTAddPiece {
		log.Warnf("jackoelv:maybeSchedRequest:TTAddPiece:sectorNumber: %s,taskType: %s, wid: 0", req.sector.Number, req.taskType)
		return true, sh.assignWorker(0, sh.workers[0], req)
	}
	//add by jack end

	for wid, worker := range sh.workers {
		rpcCtx, cancel := context.WithTimeout(req.ctx, selectorTimeout)
		// log.Warnf("jackoelv:sched:maybeSchedRequest:req.taskType: %s, wid: %s, ok: %s",req.taskType,wid,ok)
		ok, err := req.sel.Ok(rpcCtx, req.taskType, sh.spt, worker)

		log.Warnf("jackoelv:sched:maybeSchedRequest:Orginal:req.taskType: %s, wid: %s, ok: %s", req.taskType, wid, ok)
=======
		cancel()

		if err != nil {
			return false, err
		}

		if !ok {
			continue
		}
		tried++

		if !canHandleRequest(needRes, sh.spt, wid, worker.info.Resources, worker.preparing) {
			log.Warnf("jackoelv:sched:maybeSchedRequest:Orginal:canHandleRequest,wid: %d", wid)
			continue
		}
		acceptable = append(acceptable, wid)
	}

	if len(acceptable) > 0 {
		{
			var serr error

			sort.SliceStable(acceptable, func(i, j int) bool {
				rpcCtx, cancel := context.WithTimeout(req.ctx, selectorTimeout)
				defer cancel()
				r, err := req.sel.Cmp(rpcCtx, req.taskType, sh.workers[acceptable[i]], sh.workers[acceptable[j]])
				// log.Warnf("jackoelv:sched:maybeSchedRequest:acceptable,winner r: %s",r)

				if err != nil {
					serr = multierror.Append(serr, err)
				}
				return r
			})

			if serr != nil {
				return false, xerrors.Errorf("error(s) selecting best worker: %w", serr)
			}
		}
		log.Warnf("jackoelv:sched:maybeSchedRequest:Orginal:return assignWorker true,acceptable[0]: %d; sector: %d", acceptable[0], req.sector)
		return true, sh.assignWorker(acceptable[0], sh.workers[acceptable[0]], req)
	}

	if tried == 0 {
		return false, xerrors.New("maybeSchedRequest didn't find any good workers")
	}

	return false, nil // put in waiting queue
}

func (sh *scheduler) assignWorker(wid WorkerID, w *workerHandle, req *workerRequest) error {
	log.Warnf("jackoelv:sched:assignWorker,WorkerID:%d ; taskType: %s; sector: %d", wid, req.taskType, req.sector)
	needRes := ResourceTable[req.taskType][sh.spt]

	w.preparing.add(w.info.Resources, needRes)

	go func() {
		err := req.prepare(req.ctx, w.w)
		sh.workersLk.Lock()

		if err != nil {
			w.preparing.free(w.info.Resources, needRes)
			sh.workersLk.Unlock()

			select {
			case sh.workerFree <- wid:
			case <-sh.closing:
				log.Warnf("scheduler closed while sending response (prepare error: %+v)", err)
			}

			select {
			case req.ret <- workerResponse{err: err}:
			case <-req.ctx.Done():
				log.Warnf("request got cancelled before we could respond (prepare error: %+v)", err)
			case <-sh.closing:
				log.Warnf("scheduler closed while sending response (prepare error: %+v)", err)
			}
			return
		}
		log.Warnf("jackoelv:sched:assignWorker:ask worker for withResources,ID: %d", wid)
		err = w.active.withResources(sh.spt, wid, w.info.Resources, needRes, &sh.workersLk, func() error {
			w.preparing.free(w.info.Resources, needRes)
			sh.workersLk.Unlock()
			defer sh.workersLk.Lock() // we MUST return locked from this function

			select {
			case sh.workerFree <- wid:
			case <-sh.closing:
			}

			err = req.work(req.ctx, w.w)
			// if req.taskType == sealtasks.TTPreCommit1 {
			// 	err = sh.changeWorkState(wid, req.sector, req.taskType)
			// }
			if req.taskType == sealtasks.TTFetch {
				err = sh.removeWorkState(wid, req.sector)
			} else {
				err = sh.changeWorkState(wid, req.sector, req.taskType)
			}

			//when first pre1 coming,then assign the sector to the worker

			//Add by jackoelv begin
			log.Warnf("jackoelv:sched:assignWorker:modify state for the last worker")

			switch req.taskType {
			case sealtasks.TTPreCommit1:
				log.Warnf("jackoelv:sched:assignWorker:initialize the last worker: wid: %d, sectorNumber: %s", wid, req.sector.Number)
				tmpWorkerID := sh.workerBySectorID[req.sector]
				tmpWorkerID = wid + 1
				sh.workerBySectorID[req.sector] = tmpWorkerID
				sh.workerSectorNum[wid]++
			case sealtasks.TTCommit2:
				sh.workerSectorNum[wid]--
			}
			log.Warnf("jackoelv:worker:%d in sector:%s,SectorNumTotal:%d", wid, req.sector.Number, sh.workerSectorNum[wid])

			//add by jackoelv end

			select {
			case req.ret <- workerResponse{err: err}:
			case <-req.ctx.Done():
				log.Warnf("request got cancelled before we could respond")
			case <-sh.closing:
				log.Warnf("scheduler closed while sending response")
			}

			return nil
		})

		sh.workersLk.Unlock()

		// This error should always be nil, since nothing is setting it, but just to be safe:
		if err != nil {
			log.Errorf("error executing worker (withResources): %+v", err)
		}
	}()

	return nil
}

<<<<<<< Updated upstream
func (a *activeResources) withResources(spt abi.RegisteredSealProof, id WorkerID, wr storiface.WorkerResources, r Resources, locker sync.Locker, cb func() error) error {
	// log.Warnf("jackoelv:sched:withResources")
	for !canHandleRequest(r, spt, id, wr, a) {
=======
func (sh *scheduler) changeWorkState(wid WorkerID, sector abi.SectorID, taskType sealtasks.TaskType) error {
	found := false
	wStates := sh.workerDoing[wid]
	log.Warnf("jackoelv:changeWorkState begin,wid:%s,wStates:%s", wid,wStates)
	for id, tmpWs := range wStates {
		if tmpWs.sector == sector {
			tmpWs.taskType = taskType
			wStates[id] = tmpWs
			sh.workerDoing[wid] = wStates
			found = true
			break
		}
	}
	if !found {
		var ws workerState
		ws.sector = sector
		ws.taskType = taskType
		wStates = append(wStates, ws)
		sh.workerDoing[wid] = wStates
	}
	log.Warnf("jackoelv:changeWorkState end,wid:%s,wStates:%s", wid,wStates)
	return nil
}

func (sh *scheduler) removeWorkState(wid WorkerID, sector abi.SectorID) error {
	wStates := sh.workerDoing[wid]
	log.Warnf("jackoelv:removeWorkState begin,wid:%s,wStates:%s", wid,wStates)
	for id, tmpWs := range wStates {
		if tmpWs.sector == sector {
			wStates = append(wStates[:id], wStates[id+1:]...)
			sh.workerDoing[wid] = wStates
			break
		}
	}
	log.Warnf("jackoelv:removeWorkState end:wid:%s,wStates:%s", wid,wStates)
	return nil
}

func (a *activeResources) withResources(id WorkerID, wr storiface.WorkerResources, r Resources, locker sync.Locker, cb func() error) error {
	for !canHandleRequest(r, id, wr, a) {
>>>>>>> Stashed changes
		if a.cond == nil {
			a.cond = sync.NewCond(locker)
		}
		a.cond.Wait()
	}

	a.add(wr, r)

	err := cb()

	a.free(wr, r)
	if a.cond != nil {
		a.cond.Broadcast()
	}

	return err
}

func (a *activeResources) add(wr storiface.WorkerResources, r Resources) {
	a.gpuUsed = r.CanGPU
	if r.MultiThread() {
		a.cpuUse += wr.CPUs
	} else {
		a.cpuUse += uint64(r.Threads)
	}

	a.memUsedMin += r.MinMemory
	a.memUsedMax += r.MaxMemory
}

func (a *activeResources) free(wr storiface.WorkerResources, r Resources) {
	if r.CanGPU {
		a.gpuUsed = false
	}
	if r.MultiThread() {
		a.cpuUse -= wr.CPUs
	} else {
		a.cpuUse -= uint64(r.Threads)
	}

	a.memUsedMin -= r.MinMemory
	a.memUsedMax -= r.MaxMemory
}

func canHandleRequest(needRes Resources, spt abi.RegisteredSealProof, wid WorkerID, res storiface.WorkerResources, active *activeResources) bool {
	// log.Warnf("jackoelv:sched:canHandleRequest")
	// TODO: dedupe needRes.BaseMinMemory per task type (don't add if that task is already running)
	minNeedMem := res.MemReserved + active.memUsedMin + needRes.MinMemory + needRes.BaseMinMemory
	// log.Warnf("jackoelv:sched:canHandleRequest:minNeedMem:%d", minNeedMem)
	// log.Warnf("jackoelv:sched:canHandleRequest:MemPhysical:%d", res.MemPhysical)
	if minNeedMem > res.MemPhysical {
		log.Debugf("sched: not scheduling on worker %d; not enough physical memory - need: %dM, have %dM", wid, minNeedMem/mib, res.MemPhysical/mib)
		return false
	}

	maxNeedMem := res.MemReserved + active.memUsedMax + needRes.MaxMemory + needRes.BaseMinMemory
	// log.Warnf("jackoelv:sched:canHandleRequest:maxNeedMem:%d", maxNeedMem)
	if spt == abi.RegisteredSealProof_StackedDrg32GiBV1 {
		maxNeedMem += MaxCachingOverhead
	}
	if spt == abi.RegisteredSealProof_StackedDrg64GiBV1 {
		maxNeedMem += MaxCachingOverhead * 2 // ewwrhmwh
	}
	// log.Warnf("jackoelv:sched:canHandleRequest:MaxCachingOverhead:%d", MaxCachingOverhead)
	// log.Warnf("jackoelv:sched:canHandleRequest:maxNeedMem:%d", maxNeedMem)
	// log.Warnf("jackoelv:sched:canHandleRequest:res.MemSwap+res.MemPhysical:%d", res.MemSwap+res.MemPhysical)
	if maxNeedMem > res.MemSwap+res.MemPhysical {
		log.Debugf("sched: not scheduling on worker %d; not enough virtual memory - need: %dM, have %dM", wid, maxNeedMem/mib, (res.MemSwap+res.MemPhysical)/mib)
		return false
	}
	// log.Warnf("jackoelv:sched:canHandleRequest: active.cpuUse :%d", active.cpuUse)
	// log.Warnf("jackoelv:sched:canHandleRequest: uint64(needRes.Threads) :%d", uint64(needRes.Threads))
	// log.Warnf("jackoelv:sched:canHandleRequest: res.CPUs :%d", res.CPUs)
	if needRes.MultiThread() {
		if active.cpuUse > 0 {
			log.Debugf("sched: not scheduling on worker %d; multicore process needs %d threads, %d in use, target %d", wid, res.CPUs, active.cpuUse, res.CPUs)
			return false
		}
	} else {
		if active.cpuUse+uint64(needRes.Threads) > res.CPUs {
			log.Debugf("sched: not scheduling on worker %d; not enough threads, need %d, %d in use, target %d", wid, needRes.Threads, active.cpuUse, res.CPUs)
			return false
		}
	}
	// log.Warnf("jackoelv:sched:canHandleRequest: len(res.GPUs)  :%d", len(res.GPUs))
	// log.Warnf("jackoelv:sched:canHandleRequest: active.gpuUsed  :%d", active.gpuUsed)
	if len(res.GPUs) > 0 && needRes.CanGPU {
		if active.gpuUsed {
			log.Debugf("sched: not scheduling on worker %d; GPU in use", wid)
			return false
		}
	}

	return true
}

func (a *activeResources) utilization(wr storiface.WorkerResources) float64 {
	var max float64

	cpu := float64(a.cpuUse) / float64(wr.CPUs)
	max = cpu

	memMin := float64(a.memUsedMin+wr.MemReserved) / float64(wr.MemPhysical)
	if memMin > max {
		max = memMin
	}

	memMax := float64(a.memUsedMax+wr.MemReserved) / float64(wr.MemPhysical+wr.MemSwap)
	if memMax > max {
		max = memMax
	}

	return max
}

func (sh *scheduler) schedNewWorker(w *workerHandle) {
	sh.workersLk.Lock()

	id := sh.nextWorker
	sh.workers[id] = w
	sh.nextWorker++

	sh.workersLk.Unlock()

	select {
	case sh.watchClosing <- id:
	case <-sh.closing:
		return
	}

	sh.onWorkerFreed(id)
}

func (sh *scheduler) schedDropWorker(wid WorkerID) {
	sh.workersLk.Lock()
	defer sh.workersLk.Unlock()

	w := sh.workers[wid]
	delete(sh.workers, wid)

	go func() {
		if err := w.w.Close(); err != nil {
			log.Warnf("closing worker %d: %+v", err)
		}
	}()
}

func (sh *scheduler) schedClose() {
	sh.workersLk.Lock()
	defer sh.workersLk.Unlock()

	for i, w := range sh.workers {
		if err := w.w.Close(); err != nil {
			log.Errorf("closing worker %d: %+v", i, err)
		}
	}
}

func (sh *scheduler) Close() error {
	close(sh.closing)
	return nil
}
