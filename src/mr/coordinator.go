package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	files   []string
	nMap    int
	nReduce int
	workers map[int]*Workerr
	// workersLock sync.Locker
	mapTasks    []*Task
	reduceTasks []*Task
	mapCh       chan *Task
	reduceCh    chan *Task
	mapWg       sync.WaitGroup
	reduceWg    sync.WaitGroup
	mapDone     bool
	reduceDone  bool
}

type WorkerStatus int

const (
	WorkerIdle WorkerStatus = iota
	WorkerWorking
	WorkerKilled
)

type Workerr struct {
	lastPing time.Time
	status   WorkerStatus
	pid      int
}

func (w *Workerr) isIdle() bool {
	return w.status == WorkerIdle
}
func (w *Workerr) isWorking() bool {
	return w.status == WorkerWorking
}
func (w *Workerr) isDead() bool {
	return w.status == WorkerKilled
}
func (w *Workerr) getSockName() string {
	return fmt.Sprintf("mr-worker-%d", w.pid)
}

type TaskStatus int

const (
	TaskIdle TaskStatus = iota
	TaskSended
	TaskDone
)

type Task struct {
	TaskId        int
	Type          TaskType
	InputFileName string
	// OutputFileName string
	Status TaskStatus
	// Worker *Workerr
}

type TaskType int

const (
	TaskTypeMap TaskType = iota
	TaskTypeReduce
	TaskTypeWait
	TaskTypeDone
)

func (t *Task) isDone() bool {
	return t.Status == TaskDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:       files,
		nMap:        len(files),
		nReduce:     nReduce,
		workers:     make(map[int]*Workerr, 0),
		mapTasks:    make([]*Task, len(files)),
		reduceTasks: make([]*Task, nReduce),
		mapCh:       make(chan *Task, len(files)),
		reduceCh:    make(chan *Task, nReduce),
		mapWg:       sync.WaitGroup{},
		reduceWg:    sync.WaitGroup{},
		mapDone:     false,
		reduceDone:  false,
	}

	// c.SyncWorkerState()
	for i, file := range files {
		c.mapTasks[i] = &Task{
			TaskId:        i,
			Type:          TaskTypeMap,
			InputFileName: file,
			Status:        TaskIdle,
		}
		fmt.Printf("%v\n", c.mapTasks[i])

	}
	// send map tasks
	go func() {
		// fmt.Printf("send map tasks, len %d", len(files))
		c.mapWg.Add(len(files))
		for i := 0; i < c.nMap; i++ {
			c.mapCh <- c.mapTasks[i]
		}
	}()

	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = &Task{
			TaskId: i,
			Type:   TaskTypeReduce,
			Status: TaskIdle,
		}
	}
	go func() {
		c.reduceWg.Add(c.nReduce)
		c.mapWg.Wait()
		for i := 0; i < c.nReduce; i++ {
			c.reduceCh <- c.reduceTasks[i]
		}
	}()

	c.server()
	return &c
}

type RequestTaskArgs struct {
	Pid      int
	TaskDone bool
	TaskType TaskType
	TaskId   int
}
type RequestTaskReply struct {
	Task
	NMap    int
	NReduce int
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	if args.TaskDone {
		switch args.TaskType {
		case TaskTypeMap:
			if c.mapTasks[args.TaskId].Status != TaskDone {
				c.mapWg.Done()
				c.mapTasks[args.TaskId].Status = TaskDone
			}
		case TaskTypeReduce:
			if c.reduceTasks[args.TaskId].Status != TaskDone {
				c.reduceWg.Done()
				c.reduceTasks[args.TaskId].Status = TaskDone
			}
		default:
			return nil
		}
		return nil
	}

	task, ok := <-c.mapCh
	if ok {
		reply.Task = *task
		reply.NMap = c.nMap
		reply.NReduce = c.nReduce
		task.Status = TaskSended
		go func() {
			time.Sleep(10 * time.Second)
			if c.mapTasks[task.TaskId].Status != TaskDone {
				c.mapCh <- task
			}
		}()
		return nil
	}

	task, ok = <-c.reduceCh
	if ok {
		reply.Task = *task
		reply.NMap = c.nMap
		reply.NReduce = c.nReduce
		task.Status = TaskSended
		go func() {
			time.Sleep(10 * time.Second)
			if c.reduceTasks[task.TaskId].Status != TaskDone {
				c.reduceCh <- task
			}
		}()
		return nil
	}

	if c.reduceDone {
		reply.Type = TaskTypeDone
	} else {
		reply.Type = TaskTypeWait
		task.Status = TaskSended
	}
	return nil
}

func (c *Coordinator) SyncWorkerState() {
	go func() {
		for {
			// check if worker is dead
			if len(c.workers) == 0 {
				// fmt.Println("no worker")
				time.Sleep(1 * time.Second)
				continue
			}
			for pid, worker := range c.workers {
				if worker.isDead() {
					delete(c.workers, pid)
					continue
				}
				// if lastPing > 5min, kill worker
				if time.Now().Sub(worker.lastPing) > 1*time.Minute {
					// worker.status = WorkerKilled
					delete(c.workers, pid)
					continue
				}
				// cooridinator 主动 ping worker
				// err := PingWorker(worker)
				// if err != nil {
				// worker.status = WorkerKilled
				// }
			}
			// for _, worker := range c.workers {
			// 	fmt.Printf("	worker %d status: %d\n", worker.pid, worker.status)
			// }
			time.Sleep(1 * time.Second)
		}
	}()
}

func (w *Workerr) call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := w.getSockName()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	c.workers[args.Pid] = &Workerr{
		lastPing: time.Now(),
		status:   WorkerIdle,
		pid:      args.Pid,
	}
	// fmt.Printf("pid: %d registerd, woeker len: %d\n", args.Pid, len(c.workers))
	reply.Pid = args.Pid
	return nil
}

type UpdateLastPingArgs struct {
	Pid int
}
type UpdateLastPingReply struct{}

func (c *Coordinator) UpdateLastPing(args *UpdateLastPingArgs, reply *UpdateLastPingReply) error {
	worker, ok := c.workers[args.Pid]
	if ok {
		worker.lastPing = time.Now()
	}
	return nil
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {

	c.mapWg.Wait()
	c.mapDone = true
	close(c.mapCh)

	c.reduceWg.Wait()
	c.reduceDone = true
	close(c.reduceCh)

	ret := true

	// Your code here.
	return ret
}
