package mr

import (
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	Unassigned = 0
	Assigned   = 1
	Finished   = 2
)

const (
	Running = 0
	Lost    = 1
)

type WorkerStatus struct {
	WorkerId        int
	Status          int
	LastUpdateInSec int64
}

type Task struct {
	TaskId     int
	TaskType   int
	TaskStatus int
	WorkerId   int
	KeyValue   KeyValue
}

type Coordinator struct {
	// Your definitions here.
	WorkerStatus map[int]WorkerStatus

	finishedMapTasks    int
	finishedReduceTasks int

	mapTasks    []Task
	reduceTasks []Task

	nReduce int
	nMap    int
	mutex   sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func ConvertFileToKV(filename string) (KeyValue, error) {
	file, err := os.Open(filename)
	if err != nil {
		return KeyValue{}, err
	}
	content, err := io.ReadAll(file)
	if err != nil {
		return KeyValue{}, err
	}
	file.Close()
	return KeyValue{filename, string(content)}, nil
}

func (c *Coordinator) LoadMapTasksFromInput(files []string) {
	// Your code here.
	var tasks []Task
	for idx, file := range files {
		kv, err := ConvertFileToKV(file)
		if err != nil {
			log.Fatalf("cannot convert %v to KeyValue", file)
		}
		tasks = append(tasks, Task{TaskId: idx, TaskType: Map, TaskStatus: Unassigned, KeyValue: kv})
	}
	c.mapTasks = tasks
}

func (c *Coordinator) LoadReduceTasks() {
	// Your code here.
	log.Println("Loading Reduce Tasks...")
	var tasks []Task
	for idx := 0; idx < c.nReduce; idx++ {
		tasks = append(tasks, Task{TaskId: idx, TaskType: Reduce, TaskStatus: Unassigned})
	}
	c.reduceTasks = tasks
}

func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	// Your code here.
	c.mutex.Lock()
	log.Println("Register Worker")
	reply.WorkerId = len(c.WorkerStatus)
	currentTimeStampInSec := time.Now().UnixMilli() / 1000
	c.WorkerStatus[reply.WorkerId] = WorkerStatus{WorkerId: reply.WorkerId, Status: Running, LastUpdateInSec: currentTimeStampInSec}
	c.mutex.Unlock()
	return nil
}

func (c *Coordinator) HeartBeat(args *HeartBeatArgs, reply *HeartBeatReply) error {
	// Your code here.
	c.mutex.Lock()
	c.WorkerStatus[args.WorkerId] = WorkerStatus{WorkerId: args.WorkerId, Status: Running, LastUpdateInSec: time.Now().Unix()}
	c.mutex.Unlock()
	return nil
}

func (c *Coordinator) UpdateTaskStatus(args *UpdateTaskStatusArgs, reply *UpdateTaskStatusReply) error {
	// Your code here.
	c.mutex.Lock()
	if args.TaskType == Map {
		if c.mapTasks[args.TaskId].TaskStatus != Finished && args.TaskStatus == Finished {
			c.finishedMapTasks++
		}
		c.mapTasks[args.TaskId].TaskStatus = args.TaskStatus
	} else if args.TaskType == Reduce {
		if c.reduceTasks[args.TaskId].TaskStatus != Finished && args.TaskStatus == Finished {
			c.finishedReduceTasks++
		}
		c.reduceTasks[args.TaskId].TaskStatus = args.TaskStatus
	}
	if c.finishedMapTasks == c.nMap && len(c.reduceTasks) == 0 {
		log.Println("Map Tasks finished, populating Reduce Tasks...")
		c.LoadReduceTasks()
	}
	c.mutex.Unlock()
	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	// Your code here.
	c.mutex.Lock()
	if c.finishedMapTasks < c.nMap {
		for idx, task := range c.mapTasks {
			if task.TaskStatus == Unassigned {
				c.mapTasks[idx].TaskStatus = Assigned
				c.mapTasks[idx].WorkerId = args.WorkerId
				c.mutex.Unlock()
				reply.Task = task
				reply.NReduce = c.nReduce
				reply.NMap = c.nMap
				return nil
			}
		}
	} else if c.finishedReduceTasks < c.nReduce {
		for idx, task := range c.reduceTasks {
			if task.TaskStatus == Unassigned {
				c.reduceTasks[idx].TaskStatus = Assigned
				c.reduceTasks[idx].WorkerId = args.WorkerId
				c.mutex.Unlock()
				reply.Task = task
				reply.NReduce = c.nReduce
				reply.NMap = c.nMap
				return nil
			}
		}
	}
	c.mutex.Unlock()
	reply.Task = Task{TaskType: None}
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
	// Your code here.
	return c.finishedMapTasks == c.nMap && c.finishedReduceTasks == c.nReduce
}

// HeartBeatMonitor monitors the worker status and mark them as lost if they are not sending heartbeats
func (c *Coordinator) HeartBeatMonitor() {
	go func() {
		for {
			time.Sleep(10 * time.Second)
			c.mutex.Lock()
			for workerId, workerStatus := range c.WorkerStatus {
				if time.Now().Unix()-workerStatus.LastUpdateInSec > 10 {
					c.WorkerStatus[workerId] = WorkerStatus{WorkerId: workerId, Status: Lost, LastUpdateInSec: time.Now().Unix()}
					for idx, task := range c.mapTasks {
						if task.WorkerId == workerId && task.TaskStatus != Finished {
							c.mapTasks[idx].TaskStatus = Unassigned
						}
					}
					for idx, task := range c.reduceTasks {
						if task.WorkerId == workerId && task.TaskStatus != Finished {
							c.reduceTasks[idx].TaskStatus = Unassigned
						}
					}
				}
			}
			c.mutex.Unlock()
		}
	}()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.WorkerStatus = make(map[int]WorkerStatus)
	c.nReduce = nReduce
	c.nMap = len(files)
	// Your code here.
	c.LoadMapTasksFromInput(files)
	c.HeartBeatMonitor()
	c.server()
	return &c
}
