package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// map 阶段
	mapJobNum := getMapNum() // map任务数，同时也是文件数
	for {
		mapFile, mapJobID := getMapJob() // map任务所对应的文件名以及任务ID
		if mapFile != "" {               // 代表还有任务待分配
			file, err := os.Open(mapFile)
			if err != nil {
				log.Fatalf("cannot open %v", mapFile)
			}
			content, err := ioutil.ReadAll(file) // 读取文件内容
			if err != nil {
				log.Fatalf("cannot read %v", mapFile)
			}
			file.Close()
			kva := mapf(mapFile, string(content)) // map操作
			shuffle(mapJobID, kva)                // 将map任务的结果写入临时文件
			getMapJobDone("single", mapFile)      // 通知调度器一个map任务已完成

		} else {
			if getMapJobDone("all", "") { //判断是否全部map任务已结束
				break
			}
			time.Sleep(time.Second)
		}
	}

	// reduce 阶段
	for {
		reduceJobID := getReduceJob() // 获取reduce任务ID
		if reduceJobID != -1 {
			kva := make([]KeyValue, 0)
			for j := 0; j < mapJobNum; j++ {
				fileName := fmt.Sprintf("mr-%d-%d", j, reduceJobID)
				file, err := os.Open(fileName)
				if err != nil {
					log.Fatalf("cannot open %v", fileName)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
			}
			storeReduceRes(reduceJobID, kva, reducef) // reduce 操作
			getReduceJobDone(reduceJobID)             // 通知调度器一个reduce任务已完成
		} else {
			if getMROver() { // 判断是否全部任务已完成
				break
			}
		}
	}
}

func getMapNum() int {
	args := MapNumArgs{}
	reply := MapNumReply{}
	call("Coordinator.MapJobNum", &args, &reply)
	return reply.Num

}

func getMapJob() (string, int) {
	args := MapJobArgs{}
	reply := MapJobReply{}
	call("Coordinator.MapJob", &args, &reply)
	return reply.File, reply.MapJobID
}

func getMapJobDone(content, fileName string) bool {
	args := MapJobDoneArgs{Content: content, FileName: fileName}
	reply := MapJobDoneReply{}
	call("Coordinator.MapJobDone", &args, &reply)
	return reply.Done
}

func shuffle(mapJobID int, intermediate []KeyValue) {
	interFiles := make([]*os.File, 10)
	for i := 0; i < 10; i++ {
		interFileName := fmt.Sprintf("mr-%d-%d", mapJobID, i)
		interFiles[i], _ = os.Create(interFileName)
		defer interFiles[i].Close()
	}
	for _, kv := range intermediate {
		reduceJobID := ihash(kv.Key) % 10
		enc := json.NewEncoder(interFiles[reduceJobID])
		enc.Encode(&kv)
	}
}

func getReduceJob() int {
	args := ReduceArgs{}
	reply := ReduceReply{}
	call("Coordinator.ReduceJob", &args, &reply)
	return reply.ReduceJobID
}

func storeReduceRes(reduceJobID int, intermediate []KeyValue, reducef func(string, []string) string) {
	fileName := fmt.Sprintf("mr-out-%d", reduceJobID)
	ofile, _ := os.Create(fileName)
	defer ofile.Close()
	sort.Sort(ByKey(intermediate))
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
}

func getReduceJobDone(jobID int) {
	args := ReduceJobDoneArgs{JobID: jobID}
	reply := ReduceJobDoneReply{}
	call("Coordinator.ReduceJobDone", &args, &reply)
}

func getMROver() bool {
	args := MROverArgs{}
	reply := MROverReply{}
	call("Coordinator.MROver", &args, &reply)
	return reply.Done
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)

	return err == nil
}
