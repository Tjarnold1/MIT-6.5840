package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	fmt.Println("Starting worker...")

	// Your worker implementation here.
	seepCount := 0
	for {
		if seepCount == 3 {
			fmt.Println("3 seeps")
			break
		}
		task := RequestTask()
		switch task.TaskType {
		case MAP:
			// Load the file
			contents, err := os.ReadFile(task.File)
			if err != nil {
				return
			}
			contentsString := string(contents)
			values := mapf(task.File, contentsString)
			outputMapFile(values, task.NReduce, task.JobNum)
			CompleteMapTask(task)
		case REDUCE:
			// Collect the files corresponding to this reduce job
			pattern := "./*-" + strconv.Itoa(task.JobNum)
			fileNames, err := filepath.Glob(pattern)
			if err != nil {
				panic(err)
			}
			files := make([]*os.File, len(fileNames))
			for i, fileName := range fileNames {
				file, err := os.Open(fileName)
				if err != nil {
					panic(err)
				}
				files[i] = file
			}
			sortedKvs := parseIntermediateFiles(files)

			outFile, err := os.OpenFile(
				"mr-out-0",
				os.O_WRONLY|os.O_CREATE|os.O_APPEND,
				0666,
			)
			if err != nil {
				panic(err)
			}

			i := 0
			for i < len(sortedKvs) {
				j := i + 1
				for j < len(sortedKvs) && sortedKvs[j].Key == sortedKvs[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, sortedKvs[k].Value)
				}
				output := reducef(sortedKvs[i].Key, values)

				_, err = fmt.Fprintf(outFile, "%v %v\n", sortedKvs[i].Key, output)
				if err != nil {
					panic(err)
				}
				i = j
			}
			for _, file := range files {
				if err := os.Remove(file.Name()); err != nil {
					panic(err)
				}
			}
			CompleteReduceTask(task)
		case WAIT:
			fmt.Println("seeping")
			time.Sleep(5 * time.Second)
			seepCount++
		default:
			panic("Unknown task type")
		}
	}
}

func outputMapFile(values []KeyValue, nReduce int, jobNum int) {

	// Create temp files for outputs
	files := make([]*os.File, nReduce)
	encoders := make([]*json.Encoder, nReduce)
	for i := 0; i < nReduce; i++ {
		fileName := fmt.Sprintf("map-out-%d-%s-tmp", jobNum, strconv.Itoa(i))
		_ = os.Remove(fileName)

		file, err := os.Create(fileName)
		if err != nil {
			panic(err)
		}
		files[i] = file
		encoders[i] = json.NewEncoder(file)
	}

	// Write the values to our temp files
	for _, value := range values {
		key := ihash(value.Key) % nReduce
		if err := encoders[key].Encode(&value); err != nil {
			panic(err)
		}
	}

	for _, file := range files {
		os.Rename(file.Name(), file.Name()[0:len(file.Name())-4])
		if err := file.Close(); err != nil {
			panic(err)
		}
	}
}

func parseIntermediateFiles(files []*os.File) []KeyValue {
	kvs := make([]KeyValue, 0)
	for _, file := range files {
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				if err == io.EOF {
					break
				}
				panic(err)
			}
			kvs = append(kvs, kv)
		}
	}
	sort.Sort(ByKey(kvs))
	return kvs
}

func RequestTask() Task {
	resp := &RequestReply{}
	ok := call("Coordinator.RequestTask", &RequestArgs{}, &resp)
	if !ok {
		fmt.Printf("call failed\n")
	}
	return resp.Task
}

func CompleteMapTask(task Task) {
	req := CompleteReply{}
	ok := call("Coordinator.CompleteMapTask", &CompleteArgs{FileName: task.File}, &req)
	if !ok {
		fmt.Printf("call failed\n")
	}
}

func CompleteReduceTask(task Task) {
	req := CompleteReply{}
	ok := call("Coordinator.CompleteReduceTask", &CompleteArgs{ReduceNum: task.JobNum}, &req)
	if !ok {
		fmt.Printf("call failed\n")
	}
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

	fmt.Println(err)
	return false
}
