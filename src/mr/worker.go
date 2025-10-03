package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func mapping(mapf func(string, string) []KeyValue, taskId int, filename string, nReduce int) bool {
	content := ReadFile(filename)
	kva := mapf(filename, content)
	kv_bin := make([][]KeyValue, nReduce)
	for i := range kva {
		h := ihash(kva[i].Key) % nReduce
		kv_bin[h] = append(kv_bin[h], kva[i])
	}
	for y := range kv_bin {
		filename := fmt.Sprintf("mr-%v-%v", taskId, y)
		if err := WriteKVtoFile(filename, kv_bin[y]); err != nil {
			return false
		}
	}
	return true
}

func reducing(reducef func(string, []string) string, taskId int) bool {
	intermediate := []KeyValue{}
	pattern := fmt.Sprintf("mr-*-%v", taskId)
	filenames, _ := filepath.Glob(pattern)
	for _, filename := range filenames {
		kva, err := ReadKVFile(filename)
		if err != nil {
			return false
		}
		intermediate = append(intermediate, kva...)
	}
	sort.Sort(ByKey(intermediate))
	i := 0
	outputs := []KeyValue{}
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		result := reducef(intermediate[i].Key, values)
		outputs = append(outputs, KeyValue{intermediate[i].Key, result})
		i = j
	}
	filename := fmt.Sprintf("mr-out-%v", taskId)
	if err := WriteKVtoFile(filename, outputs); err != nil {
		return false
	}
	return true
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		allocatedTask := AllocatedTask{}
		ok := call("Coordinator.Allocate", &Empty{}, &allocatedTask)
		if !ok { // 简单起见，coordinator 报告执行完毕，则直接退出
			break
		}
		success := true
		switch allocatedTask.TaskType {
		case WAITING:
			// 可以考虑换成指数退避(类比计网)；这里只考虑简单情况，随机 slepp 20-200ms
			min, max := 20, 200
			sleepTime := time.Duration(rand.Intn(max-min)+min) * time.Millisecond
			time.Sleep(sleepTime)
		case MAPPING:
			success = mapping(mapf, allocatedTask.TaskId, allocatedTask.Filepath, allocatedTask.NReduce)
		case REDUCING:
			success = reducing(reducef, allocatedTask.TaskId)
		} // default break

		reportedTask := ReportedTask{TaskType: allocatedTask.TaskType, TaskId: allocatedTask.TaskId, IsDone: success}
		// 简单起见，不管这里是否失败；失败了让 coordinator 再分配给 worker 重做
		call("Coordinator.Report", &reportedTask, &Empty{})
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {
	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	if err.Error() != "当前任务已完成：当前阶段与报告阶段不一致" { // SPJ
		if err.Error() != "任务已完成，请退出" {
			fmt.Println(err)
		}
	}
	return false
}

// 下面是文件I/O部分
var ioMutex sync.Mutex

func ReadFile(filename string) string {
	ioMutex.Lock() // 简单起见，全局锁，防止被覆盖(down的任务又up，导致同一个文件多个读写)
	defer ioMutex.Unlock()
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return string(content)
}

// 为了方便复用，把写格式+数据和写操作分离，可能以后用得上；所以分离了三个函数
func WriteFile(filename string, writeFunc func(*os.File) error) error {
	dir := filepath.Dir(filename)
	file, err := os.CreateTemp(dir, filepath.Base(filename)+"-")
	if err != nil {
		return err
	}
	tempFile := file.Name()
	defer func() {
		file.Close()
		if err != nil {
			os.Remove(tempFile)
		}
	}()
	if err := writeFunc(file); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	ioMutex.Lock() // 临时文件是唯一的，不需要锁
	defer ioMutex.Unlock()
	return os.Rename(tempFile, filename)
}

// 必须保证：key, value 都不含空格
func MapReduceWriter(file *os.File, kva []KeyValue) error {
	for _, kv := range kva {
		if _, err := fmt.Fprintf(file, "%v %v\n", kv.Key, kv.Value); err != nil {
			return err
		}
	}
	return nil
}

func WriteKVtoFile(filename string, kva []KeyValue) error {
	return WriteFile(filename, func(file *os.File) error {
		return MapReduceWriter(file, kva)
	})
}

func ReadKVFile(filename string) ([]KeyValue, error) {
	ioMutex.Lock()
	defer ioMutex.Unlock()

	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("cannot open %v: %w", filename, err)
	}
	defer file.Close()

	var kvs []KeyValue
	scanner := bufio.NewScanner(file)
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := scanner.Text()
		if len(line) == 0 { // 跳过空行
			continue
		}

		// 查找第一个空格位置（键值分隔符）
		idx := strings.Index(line, " ")
		if idx < 0 {
			return nil, fmt.Errorf("invalid format at line %d: no space found", lineNum)
		}

		kvs = append(kvs, KeyValue{
			Key:   line[:idx],
			Value: line[idx+1:], // 包含值中可能存在的空格
		})
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("cannot read %v: %w", filename, err)
	}
	return kvs, nil
}
