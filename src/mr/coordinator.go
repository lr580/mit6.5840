package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

const (
	IDLE uint8 = iota
	RUNNING
	COMPLETED
)

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

type Coordinator struct {
	// Your definitions here.
	filenames   []string
	nReduce     int
	mapTasks    []task
	reduceTasks []task
}

// Your code here -- RPC handlers for the worker to call.

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

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{filenames: files, nReduce: nReduce}

	// Your code here.
	/*for _, filename := range files {
		// mapTask := mapTask{filename, string(content), task{IDLE}}
		c.mapTasks = append(c.mapTasks, mapTask)
	}*/

	c.server()
	return &c
}
