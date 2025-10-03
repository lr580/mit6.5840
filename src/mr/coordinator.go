package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	IDLE uint8 = iota
	RUNNING
	COMPLETED
) // task 状态

const WAIT_TIME = 10 * time.Second

type task struct {
	status    uint8
	startTime time.Time
}

/*type mapTask struct {
	filename string
	// 不记录文件内容，worker按需读取，否则不符合分布式的设计 (一是 coordinator 内存装不下这么多 content；二是网络 I/O 流量带宽浪费)
	// content  string
	// 同理，结果应该存储到磁盘文件 mr-X-Y 里
	// results []mr.KeyValue
	t task
}*/

const (
	MAPPING uint8 = iota
	REDUCING
	FINISHED
	WAITING // 给worker返回值用，也可以复用 finished 但语义不清晰
) // coordinator 状态

type Coordinator struct {
	// Your definitions here.
	filenames   []string
	nReduce     int
	mapTasks    []task
	reduceTasks []task
	mutexTask   sync.Mutex
	status      uint8 //多个状态方便扩展
	// isReducing  bool // 两个状态不足，reduce 执行完后可以发送退出指令给 worker，两个状态的话还要再遍历比较麻烦
}

func (c *Coordinator) currentTasks() (tasks *[]task) {
	if c.status == MAPPING {
		tasks = &c.mapTasks
	} else { // else if?
		tasks = &c.reduceTasks
	}
	return
}

func isAllCompleted(tasks []task) bool {
	for i := range tasks {
		if tasks[i].status != COMPLETED {
			return false
		}
	}
	return true
}

func (c *Coordinator) deadWorkerChecker() {
	ticker := time.NewTicker(time.Second) //WAIT_TIME / 2) // 可自行修改周期
	defer ticker.Stop()
	for range ticker.C {
		c.mutexTask.Lock()
		tasks := c.currentTasks()
		now := time.Now()
		for i := range *tasks {
			// mutex 粒度是否可以降低到 if 成立里？
			if (*tasks)[i].status == RUNNING && now.Sub((*tasks)[i].startTime) > WAIT_TIME {
				// 直接设置状态即可。如果后面发现 worker 没挂，让该任务被执行多次，因幂等不影响结果。可以给读写同一文件的过程加锁，避免 reducer 执行读到一半被覆盖
				(*tasks)[i].status = IDLE
			}
		}
		c.mutexTask.Unlock()
	}
}

type AllocatedTask struct {
	// isMapTask bool // 方便应对其他状况，换用其他：暂时没有任务(mapping, 但未 reducing；或都在reducing但还没做完，但可能还需要worker)
	TaskType uint8
	TaskId   int    // 0-indexed
	Filepath string // optional for map task
	NReduce  int    //map需要
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Allocate(_ *struct{}, reply *AllocatedTask) error {
	c.mutexTask.Lock()
	defer c.mutexTask.Unlock()
	if c.status == FINISHED { // 区分需要等待(如reduce还没开始都在map)
		// return errors.New("任务已完成，请退出")
		reply.TaskType = FINISHED
		return nil
	}
	tasks := c.currentTasks()
	// 认为 tasks 少(<1e4)，简单起见直接遍历；如果任务多，可以额外维护 bitset 或 map (未完成 tasks 集) 等。
	for i := range *tasks {
		if (*tasks)[i].status == IDLE {
			(*tasks)[i].status = RUNNING
			(*tasks)[i].startTime = time.Now()
			reply.TaskId = i
			reply.TaskType = c.status
			if c.status == MAPPING {
				reply.Filepath = c.filenames[i]
				reply.NReduce = c.nReduce
			}
			return nil
		}
	}
	reply.TaskType = WAITING
	return nil
}

type ReportedTask struct {
	// isMapTask bool (同理，方便扩展报告其他状态，如出错?)
	TaskType uint8
	TaskId   int  //0-indexed
	IsDone   bool //可能出错，不出错就执行完毕，否则失败，为了鲁棒性
}

func (c *Coordinator) Report(args *ReportedTask, _ *struct{}) error {
	c.mutexTask.Lock()
	defer c.mutexTask.Unlock()
	if args.TaskType != c.status {
		return errors.New("当前任务已完成：当前阶段与报告阶段不一致")
	}
	// 不考虑下面的情况：当前 map-reduce 任务已经完成
	// 因为超时缘故，上一个任务被 worker 做了多次，并错误地向下一个 coordinator 发送完成报告
	// 这是因为 coordinator 无法阻止 worker，它写的 mr-X-Y 必然是覆盖了的
	// 实际上，在 test-mr-many.sh 是串行执行的，并且在执行下一个 map-reduce 任务之前，测试过程保证会关闭所有的 worker
	// 如果要考虑这种情况，应当仿照 HTTP 防攻击，给每个 map-reduce 任务加 id 或其他东西，只有 id 对才相应。
	// 与此同时，id 应该增加到所有文件名字里，如 mr-id-X-Y。但是 Lab1 并未做此要求

	tasks := c.currentTasks()
	if (*tasks)[args.TaskId].status != RUNNING {
		return errors.New("当前任务不在运行中")
	}
	if args.IsDone {
		(*tasks)[args.TaskId].status = COMPLETED
	} else { // 简单起见，再分配给任意 worker 做
		(*tasks)[args.TaskId].status = IDLE
	}

	// 同理，认为 tasks 少(<1e4)，简单起见直接遍历
	if isAllCompleted(*tasks) {
		switch c.status {
		case MAPPING:
			c.status = REDUCING
		case REDUCING:
			c.status = FINISHED
		}
	}
	// 当所有 reduce task 执行完毕，什么都不需要做。Done 会收拾结果 (感觉主动通知会更好？)
	// 无需合并多个 reduce 任务结果，test-mr.sh 会排序合并多个 mr-out-X 与顺序执行对比
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if isAllCompleted(c.reduceTasks) {
		ret = true
	}

	// 认为 Lab1 出于简单和职责分离目的，没有用回调主动通知 mrcoordinator.go ，而是让它来轮询
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	nMap := len(files) // 方便起见，目前设定一个文件一个map，可以更改
	c := Coordinator{filenames: files, nReduce: nReduce, mapTasks: make([]task, nMap), reduceTasks: make([]task, nReduce)}

	// Your code here.
	/*for _, filename := range files {
		// mapTask := mapTask{filename, string(content), task{IDLE}}
		c.mapTasks = append(c.mapTasks, mapTask)
	}*/

	c.server()
	go c.deadWorkerChecker()
	return &c
}
