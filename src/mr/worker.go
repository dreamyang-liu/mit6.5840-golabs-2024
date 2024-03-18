package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// set log level to error and above
	log.SetOutput(os.Stderr)
	workerId := RegisterWorker()
	// Start a go routine to keep sending heart bea
	go func() {
		for {
			time.Sleep(5 * time.Second)
			SendHeartBeat(workerId)
		}
	}()
	for {
		reply := CallGetTask(workerId)
		if reply.Task.TaskType == None {
			log.Println("No task available, hibernating for 2 seconds")
			time.Sleep(2 * time.Second)
			continue
		}
		if reply.Task.TaskType == Map {
			executeMapTask(mapf, reply)
			// time.Sleep(5 * time.Second)
		} else if reply.Task.TaskType == Reduce {
			executeReduceTask(reducef, reply)
		}
	}
}

func SendHeartBeat(workerId int) {
	args := HeartBeatArgs{}
	args.WorkerId = workerId
	reply := HeartBeatReply{}
	call("Coordinator.HeartBeat", &args, &reply)
}

func executeMapTask(mapf func(string, string) []KeyValue, reply GetTaskReply) {
	task := reply.Task
	kvs := mapf(task.KeyValue.Key, task.KeyValue.Value)
	buckets := make(map[int][]KeyValue)
	for _, kv := range kvs {
		reduceTaskId := ihash(kv.Key) % reply.NReduce
		buckets[reduceTaskId] = append(buckets[reduceTaskId], kv)
	}
	for reduceTaskId, kvs := range buckets {
		writeIntermediateFile(task.TaskId, reduceTaskId, kvs)
	}
	callUpdateTaskStatus(task.TaskId, task.TaskType, Finished)
}

func executeReduceTask(reducef func(string, []string) string, reply GetTaskReply) {
	task := reply.Task
	intermediate_files := make([]string, 0)
	for i := 0; i < reply.NMap; i++ {
		intermediate_files = append(intermediate_files, fmt.Sprintf("mr-%d-%d", i, task.TaskId))
	}
	kvs := []KeyValue{}
	for _, filename := range intermediate_files {
		tmp_kvs, err := ReadIntermediateFile(filename)
		if err != nil {
			continue
		}
		kvs = append(kvs, tmp_kvs...)
	}
	output := fmt.Sprintf("mr-out-%d", task.TaskId)
	file, err := os.Create(output)
	if err != nil {
		log.Fatalf("cannot create %v", output)
	}
	defer file.Close()
	w := bufio.NewWriter(file)
	sortedKvs := make(map[string][]string)
	for _, kv := range kvs {
		sortedKvs[kv.Key] = append(sortedKvs[kv.Key], kv.Value)
	}
	for key, values := range sortedKvs {
		output := reducef(key, values)
		w.Write([]byte(fmt.Sprintf("%v %v\n", key, output)))
	}
	w.Flush()
	callUpdateTaskStatus(task.TaskId, task.TaskType, Finished)
}

func ReadIntermediateFile(filename string) ([]KeyValue, error) {
	file, err := os.Open(filename)
	if err != nil {
		// log.Printf("cannot open %v", filename)
		return nil, err
	}
	content, err := io.ReadAll(file)
	if err != nil {
		// log.Printf("cannot read %v", filename)
		return nil, err
	}
	file.Close()
	separatedLines := strings.Split(string(content), "\n")
	var kvs []KeyValue
	for _, line := range separatedLines {
		if line != "" {
			separatedKV := strings.Split(line, " ")
			kvs = append(kvs, KeyValue{separatedKV[0], separatedKV[1]})
		}
	}
	return kvs, nil
}

func writeIntermediateFile(taskId int, reduceTaskId int, kvs []KeyValue) {
	fileName := fmt.Sprintf("mr-%d-%d", taskId, reduceTaskId)
	file, err := os.Create(fileName)
	if err != nil {
		log.Fatalf("cannot create %v", fileName)
	}
	defer file.Close()
	w := bufio.NewWriter(file)
	for _, kv := range kvs {
		fmt.Fprintf(w, "%v %v\n", kv.Key, kv.Value)
	}
	w.Flush()
}

func RegisterWorker() int {
	args := RegisterWorkerArgs{}
	reply := RegisterWorkerReply{}
	call("Coordinator.RegisterWorker", &args, &reply)
	log.Println("Successfully registered with WorkerId: ", reply.WorkerId)
	return reply.WorkerId
}

func callUpdateTaskStatus(taskId int, taskType int, status int) {
	args := UpdateTaskStatusArgs{}
	args.TaskId = taskId
	args.TaskType = taskType
	args.TaskStatus = status
	reply := UpdateTaskStatusReply{}
	call("Coordinator.UpdateTaskStatus", &args, &reply)
}

func CallGetTask(workerId int) GetTaskReply {
	args := GetTaskArgs{}
	args.WorkerId = workerId
	reply := GetTaskReply{}
	call("Coordinator.GetTask", &args, &reply)
	return reply
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

	fmt.Println(err)
	return false
}
