package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
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
func Worker(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	// Your worker implementation here.

	// get current pid, and send to coordinator
	pid := os.Getpid()

	args := RegisterWorkerArgs{
		Pid: pid,
	}
	reply := RegisterWorkerReply{}
	ok := call("Coordinator.RegisterWorker", &args, &reply)
	if !ok {
		panic("register worker failed")
	}

	for {
		args := RequestTaskArgs{
			Pid:      pid,
			TaskDone: false,
		}
		taskReply := RequestTaskReply{}
		ok = call("Coordinator.RequestTask", &args, &taskReply)
		if !ok {
			return
		}
		// fmt.Printf("ReqxuestTask done %v-%v nMap %v nReduce %v\n", taskReply.Type, taskReply.TaskId, taskReply.NMap, taskReply.NReduce)
		fmt.Printf("Pid: [%v] ReqxuestTask done %v-%v nMap %v nReduce %v\n", pid, taskReply.Type, taskReply.TaskId, taskReply.NMap, taskReply.NReduce)
		switch taskReply.Type {
		case TaskTypeMap:
			kvPairs := make([]KeyValue, 0)
			// read map task input file
			content, _ := os.ReadFile(taskReply.InputFileName)
			kvPairs = append(kvPairs, mapf(taskReply.InputFileName, string(content))...)

			kvPairsGroup := make([][]KeyValue, taskReply.NReduce)
			for _, pair := range kvPairs {
				reduceId := ihash(pair.Key) % taskReply.NReduce
				kvPairsGroup[reduceId] = append(kvPairsGroup[reduceId], pair)
				// fmt.Printf("Pid: [%v] Read file %v done, content length: %v\n", pid, reduceId, kvPairsGroup[reduceId])
			}
			for reduceId, pairs := range kvPairsGroup {
				mapOutputFileName := fmt.Sprintf("mr-output-%v-%v", taskReply.TaskId, reduceId)
				// fmt.Printf("Pid: [%v] mapOutputFileName %v done, %v\n", pid, mapOutputFileName, pairs)

				// 创建 map 任务输出文件
				file, _ := os.Create(mapOutputFileName)
				defer file.Close()
				jsonStr, _ := json.Marshal(pairs)
				// 将 map 任务输出写入文件
				fmt.Fprintf(file, string(jsonStr))
			}
			args := RequestTaskArgs{
				Pid:      pid,
				TaskDone: true,
				TaskType: TaskTypeMap,
				TaskId:   taskReply.TaskId,
			}
			reply := RequestTaskReply{}
			ok := call("Coordinator.RequestTask", &args, &reply)
			if !ok {
				// coordinator may be dead, exit
				return
			}
		case TaskTypeReduce:
			intermediate := make([]KeyValue, 0)
			// read map task output file
			for mapId := 0; mapId < taskReply.NMap; mapId++ {
				mapOutputFileName := fmt.Sprintf("mr-output-%v-%v", mapId, taskReply.TaskId)
				content, _ := os.ReadFile(mapOutputFileName)
				// fmt.Printf("read mapresult file: %v %v\n", mapOutputFileName, content)
				kvPairs := make([]KeyValue, 0)
				json.Unmarshal([]byte(content), &kvPairs)

				intermediate = append(intermediate, kvPairs...)
			}

			// sort intermediate by key
			sort.Slice(intermediate, func(i, j int) bool {
				return intermediate[i].Key < intermediate[j].Key
			})

			results := make([]KeyValue, 0)
			startIndex := 0
			// reduce by key
			for i := 0; i < len(intermediate); i++ {
				if i < len(intermediate)-1 && intermediate[i].Key == intermediate[i+1].Key {
					continue
				}
				values := make([]string, 0)
				for j := startIndex; j <= i; j++ {
					values = append(values, intermediate[j].Value)
				}
				res := reducef(intermediate[startIndex].Key, values)
				if intermediate[startIndex].Key == "sufferings" {
					fmt.Printf("sufferings %v\n", res)
				}
				results = append(results, KeyValue{intermediate[startIndex].Key, res})
				startIndex = i + 1
			}

			reduceOutputFileName := fmt.Sprintf("mr-out-%v", taskReply.TaskId)

			// 创建 reduce 任务输出文件
			file, _ := os.Create(reduceOutputFileName)
			defer file.Close()
			for _, result := range results {
				s := fmt.Sprintf("%v %v\n", result.Key, result.Value)
				i, err := fmt.Fprintf(file, s)
				if err != nil {
					log.Fatalf("cannot write %v %v", i, err)
				}
			}

			// delete map output files
			for i := 0; i < taskReply.NMap; i++ {
				iname := fmt.Sprintf("mr-output-%v-%v", i, taskReply.TaskId)
				err := os.Remove(iname)
				if err != nil {
					log.Fatalf("cannot open delete" + iname)
				}
			}

			args := RequestTaskArgs{
				Pid:      pid,
				TaskDone: true,
				TaskType: TaskTypeReduce,
				TaskId:   taskReply.TaskId,
			}
			reply := RequestTaskReply{}
			// send finish reduce task message to coordinator
			ok := call("Coordinator.RequestTask", &args, &reply)
			if !ok {
				// coordinator may be dead, exit
				return
			}
		case TaskTypeWait:
			time.Sleep(1 * time.Second)
			continue
		case TaskTypeDone:
			return
		default:
			return
		}
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

	fmt.Println(err)
	return false
}
