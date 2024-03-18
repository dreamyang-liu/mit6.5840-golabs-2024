package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// TaskStatus update RPC
type UpdateTaskStatusArgs struct {
	TaskId     int
	TaskType   int
	TaskStatus int
}

type UpdateTaskStatusReply struct {
}

// Register Worker to Coordinator RPC

type RegisterWorkerArgs struct {
}

type RegisterWorkerReply struct {
	WorkerId int
}

// Get Task RPC
const (
	Map    = 0
	Reduce = 1
	None   = 2
)

type GetTaskArgs struct {
	WorkerId int
}

type GetTaskReply struct {
	Task    Task
	NReduce int
	NMap    int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
