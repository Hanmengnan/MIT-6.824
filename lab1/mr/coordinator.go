package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	MapJobs            chan string     // map任务管道
	MapJobID           chan int        // map任务id
	MapJobCount        int             // map任务数
	MapJobState        map[string]bool // map任务状态`map`
	MapJobStateLock    sync.Mutex      // map任务状态锁
	ReduceJobs         chan int        // reduce任务管道
	ReduceJobState     map[int]bool    // reduce任务状态map
	ReduceJobStateLock sync.Mutex      // reduce任务状态锁
	Over               bool            // mr任务结束标志
	OverLock           sync.Mutex      // mr任务状态锁
}

func (c *Coordinator) MapJobNum(args *MapNumArgs, reply *MapNumReply) error {
	reply.Num = c.MapJobCount
	return nil
}

func (c *Coordinator) MapJob(args *MapJobArgs, reply *MapJobReply) error {
	select {
	case reply.File = <-c.MapJobs:
		reply.MapJobID = <-c.MapJobID
		go func(jobFile string, jobID int) {
			//开启一个协程等待10s判断任务是否完成
			time.Sleep(time.Second * 10)
			c.MapJobStateLock.Lock()
			if !c.MapJobState[jobFile] {
				c.MapJobs <- jobFile
				c.MapJobID <- jobID
			}
			c.MapJobStateLock.Unlock()
		}(reply.File, reply.MapJobID)
	default:
		reply.MapJobID = 0
		reply.File = ""
	}
	return nil
}

func (c *Coordinator) MapJobDone(args *MapJobDoneArgs, reply *MapJobDoneReply) error {
	if args.Content == "single" {
		// 通知单个任务结束
		c.MapJobStateLock.Lock()
		c.MapJobState[args.FileName] = true
		c.MapJobStateLock.Unlock()
	} else if args.Content == "all" {
		// 查询全部任务是否结束
		reply.Done = true
		c.MapJobStateLock.Lock()
		for _, value := range c.MapJobState {
			// 存在未完成map任务
			if !value {
				reply.Done = false
				break
			}
		}
		c.MapJobStateLock.Unlock()
	}
	return nil
}

func (c *Coordinator) ReduceJob(args *ReduceArgs, reply *ReduceReply) error {
	select {
	case reply.ReduceJobID = <-c.ReduceJobs:
		go func(jobID int) { 
			//同map
			time.Sleep(time.Second * 10)
			c.ReduceJobStateLock.Lock()
			if !c.ReduceJobState[jobID] {
				c.ReduceJobs <- jobID
			}
			c.ReduceJobStateLock.Unlock()
		}(reply.ReduceJobID)
	default:
		reply.ReduceJobID = -1
	}
	return nil
}

func (c *Coordinator) ReduceJobDone(args *ReduceJobDoneArgs, reply *ReduceJobDoneReply) error {
	c.ReduceJobStateLock.Lock()
	c.ReduceJobState[args.JobID] = true
	c.ReduceJobStateLock.Unlock()
	return nil
}

func (c *Coordinator) MROver(args *MROverArgs, reply *MROverReply) error {
	c.OverLock.Lock()
	reply.Done = c.Over
	c.OverLock.Unlock()
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	flag := true
	c.ReduceJobStateLock.Lock()
	for _, value := range c.ReduceJobState {
		if !value {
			flag = false
		}
	}
	c.ReduceJobStateLock.Unlock()
	c.OverLock.Lock()
	c.Over = flag
	c.OverLock.Unlock()
	return c.Over

}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.MapJobs = make(chan string, len(files))
	c.MapJobID = make(chan int, len(files))
	c.MapJobCount = len(files)
	c.MapJobState = make(map[string]bool)
	for index, file := range files {
		c.MapJobs <- file
		c.MapJobID <- index
		c.MapJobState[file] = false
	}
	c.ReduceJobs = make(chan int, nReduce)
	c.ReduceJobState = make(map[int]bool)
	for i := 0; i < nReduce; i++ {
		c.ReduceJobs <- i
		c.ReduceJobState[i] = false
	}
	// 初始化 coordinator

	c.server()
	return &c
}
