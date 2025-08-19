package core

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

var workerId = os.Getpid()

var enableDebug = false

type ByKey []*KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type KeyValue struct {
	Key   string
	Value string
}

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		task := getFileToWork()

		if task == nil || task.JobType == "noWork" {
			logger(fmt.Sprintf("[Worker: %v]: no task to work. Exit", workerId), "INFO")
			break
		}

		logger(fmt.Sprintf("[Worker: %v]: get task %v", workerId, task), "INFO")

		if task.JobType == "map" {

			_, _ = mapTask(mapf, task)

		} else if task.JobType == "reduce" {

			_, _ = reduceTask(reducef, task)

		} else if task.JobType == "pleaseWait" {
			logger(fmt.Sprintf("[Worker: %v]: Coordinator tells me please wait.. I will sleep for 1 sec", workerId), "INFO")
			time.Sleep(time.Second)
		} else {
			log.Fatalf("[Workeer: %v]: unknown job type %v", workerId, task.JobType)
		}

	}

}

func logger(message string, logType string) {
	if logType == "DEBUG" && !enableDebug {
		return
	}

	log.Printf("%v", message)

}

func getContentFileKV(reduceTaskNumber int) ([]*KeyValue, error) {

	files, err := filepath.Glob(fmt.Sprintf("mr-*-%v", reduceTaskNumber))

	if err != nil {
		logger(fmt.Sprintf("[Worker: %v]: cannot get file %v", workerId, files), "ERROR")
		return nil, err
	}

	response := make([]*KeyValue, 0)

	for _, fileName := range files {
		logger(fmt.Sprintf("[Worker: %v]: get file %v", workerId, fileName), "DEBUG")

		file, errorOpenFileName := os.Open(fileName)
		if errorOpenFileName != nil {
			logger(fmt.Sprintf("[Worker: %v]: cannot open file %v", workerId, fileName), "ERROR")
			continue
		}

		decoder := json.NewDecoder(file)

		for {
			kv := &KeyValue{}

			errDecode := decoder.Decode(kv)
			if errDecode != nil {
				logger(fmt.Sprintf("Worker: %v: cannot decode file %v because: %v", workerId, fileName, errDecode), "DEBUG")
				break
			}
			response = append(response, kv)
		}

		errorOpenFileName = file.Close()

		if errorOpenFileName != nil {
			logger(fmt.Sprintf("[Worker: %v]: cannot close file %v", workerId, fileName), "ERROR")
		}
	}

	return response, nil

}

func getContentFile(fileName string) ([]byte, error) {
	file, errorOpenFileName := os.Open(fileName)
	if errorOpenFileName != nil {
		logger(fmt.Sprintf("Worker: %v: cannot open file %v", workerId, fileName), "ERROR")
		return nil, errorOpenFileName
	}

	content, errorReadFileName := io.ReadAll(file)
	if errorReadFileName != nil {
		logger(fmt.Sprintf("Worker: %v: cannot read file %v", workerId, fileName), "ERROR")
		return nil, errorReadFileName
	}

	errorCloseFileName := file.Close()
	if errorCloseFileName != nil {
		logger(fmt.Sprintf("Worker: %v: cannot close file %v", workerId, fileName), "ERROR")
		return nil, errorCloseFileName
	}

	return content, nil
}

func reduceTask(task func(string, []string) string, bluePrint *ResponseTask) (bool, error) {
	preReadyData, errGetContent := getContentFileKV(bluePrint.TaskNumber)
	if errGetContent != nil {
		logger(fmt.Sprintf("Worker: %v: cannot get content of reduceJobNumber: %v", workerId, bluePrint.TaskNumber), "ERROR")
		return false, errGetContent
	}

	logger(fmt.Sprintf("Worker: %v: get content for intermediate file number: %v", workerId, bluePrint.TaskNumber), "DEBUG")

	sort.Sort(ByKey(preReadyData))

	results := getReduceTaskResult(preReadyData, task)

	ok, writeError := startToWriteReduceResult(results, bluePrint.TaskNumber)

	if !ok || writeError != nil {
		return false, writeError
	}

	cleanUpOk := cleanUp(bluePrint.TaskNumber)

	if !cleanUpOk {
		return false, fmt.Errorf("[Worker: %v]: cannot clean up intermediate file of ReduceTaskNumber: %v", workerId, bluePrint.TaskNumber)
	}

	return true, nil

}

func cleanUp(reduceTaskNumber int) bool {

	files, errGlob := filepath.Glob(fmt.Sprintf("mr-[0-9]*-%v", reduceTaskNumber))

	if errGlob != nil {
		log.Fatalf("[Worker: %v]: cannot get file %v", workerId, files)
	}

	for _, fileName := range files {
		logger(fmt.Sprintf("[Worker: %v]: clean up file %v", workerId, fileName), "DEBUG")

		err := os.Remove(fileName)

		if err != nil {
			logger(fmt.Sprintf("[Worker: %v]: cannot remove file %v", workerId, fileName), "ERROR")
			continue
		}
	}

	return true

}

func writeReduceResultToFileTemp(file *os.File, content map[string]string) bool {

	for key, value := range content {

		logger(fmt.Sprintf("[Worker: %v]: write reduce result: %v %v", workerId, key, value), "DEBUG")

		_, errWrite := fmt.Fprintf(file, "%v %v\n", key, value)
		if errWrite != nil {

			return false
		}

	}

	return true
}

func startToWriteReduceResult(results map[string]string, reduceTaskNumber int) (bool, error) {
	resultFileName := fmt.Sprintf("mr-out-%v", reduceTaskNumber)

	ok := pingReduceTask(reduceTaskNumber)

	if !ok {
		return false, fmt.Errorf("[Worker: %v]: cannot ping reduceTaskNumber %v]", workerId, reduceTaskNumber)
	}

	file, errOpenFile := os.OpenFile(resultFileName, os.O_WRONLY|os.O_CREATE, 0600)

	if errOpenFile != nil {
		return false, errOpenFile
	}

	okWriteTempFile := writeReduceResultToFileTemp(file, results)

	if !okWriteTempFile {
		_, errRollback := rollback(file.Name())
		if errRollback != nil {
			logger(fmt.Sprintf("Worker: %v: cannot rollback result file %v. Rollback failed", workerId, file.Name()), "ERROR")
		}

		return false, errRollback
	}

	okRequest := signalCommitingReduceTask(reduceTaskNumber)
	if !okRequest {
		logger(fmt.Sprintf("Worker: %v: cannot signal commiting ReduceTaskNumber: %v. Rollback failed", workerId, reduceTaskNumber), "ERROR")
		_, _ = rollback(file.Name())

		return false, fmt.Errorf("[Worker: %v]: cannot signal commiting ReduceTaskNumber %v. Rollback", workerId, reduceTaskNumber)
	}

	errCloseTransaction := file.Close()
	if errCloseTransaction != nil {
		logger(fmt.Sprintf("Worker: %v: cannot close transaction file %v. Rollback failed", workerId, file.Name()), "ERROR")
		_, _ = rollback(file.Name())

		return false, errCloseTransaction
	}

	ack := sendCompleteMessageForReduceTaskNumber(reduceTaskNumber)
	if !ack {
		logger(fmt.Sprintf("Worker: %v: cannot send CompleteMessageForReduceTaskNumber %v. Rollback failed", workerId, reduceTaskNumber), "ERROR")
		_, _ = rollback(file.Name())

		return false, fmt.Errorf("[Worker: %v]: cannot send result file %v. Rollback", workerId, file.Name())
	}

	return true, nil

}

func getReduceTaskResult(data []*KeyValue, task func(string, []string) string) map[string]string {
	results := make(map[string]string)

	startIndex := 0
	endIndex := 0

	for {

		key := data[startIndex].Key

		endIndex = findTheDistinctName(key, startIndex, data)

		result := task(key, getValues(data, startIndex, endIndex))

		results[key] = result

		startIndex = endIndex

		if startIndex >= len(data) {
			break
		}
	}

	return results
}

func getValues(data []*KeyValue, start int, end int) []string {
	result := make([]string, 0)

	for start != end {
		result = append(result, data[start].Value)
		start++

	}

	return result
}

func findTheDistinctName(name string, prefix int, data []*KeyValue) int {

	index := 0

	if prefix+index >= len(data) {
		return -1
	}

	for prefix+index < len(data) && data[prefix+index].Key == name {
		index++
	}

	return prefix + index

}

func mapTask(task func(string, string) []KeyValue, bluePrint *ResponseTask) (bool, error) {
	preReadyData, errGetContent := getContentFile(bluePrint.Filename)
	if errGetContent != nil {
		logger(fmt.Sprintf("Worker: %v: cannot get content of %v", workerId, bluePrint.Filename), "ERROR")
		return false, errGetContent
	}

	processedData := task(bluePrint.Filename, string(preReadyData))

	ok, errWriteContent := writeMapResult(processedData, bluePrint.Filename, bluePrint.NReduce, bluePrint.TaskNumber)

	if !ok || errWriteContent != nil {
		return false, errWriteContent
	}

	return true, nil

}

func writeMapResult(processedData []KeyValue, originalFileName string, nReduce int, taskNumber int) (bool, error) {
	writeFileGroup := make(map[int][]KeyValue)

	for _, kv := range processedData {
		reduceId := ihash(kv.Key) % nReduce
		if _, found := writeFileGroup[reduceId]; !found {
			writeFileGroup[reduceId] = make([]KeyValue, 0)
		}
		writeFileGroup[reduceId] = append(writeFileGroup[reduceId], kv)
	}

	ok := pingMapTask(originalFileName)
	if !ok {
		return false, fmt.Errorf("[Worker: %v]: cannot ping task %v]", workerId, originalFileName)
	}

	intermediateFiles := make([]string, 0)

	for reduceId, data := range writeFileGroup {
		intermediateFileName := fmt.Sprintf("mr-%v-%v", taskNumber, reduceId)

		okWriteIntermediateFile, err := writeIntermediateFile(intermediateFileName, data, originalFileName)

		if !okWriteIntermediateFile {
			return false, err
		}

		intermediateFiles = append(intermediateFiles, intermediateFileName)

	}

	okCompleting := sendIntermediateFiles(intermediateFiles, originalFileName)

	if !okCompleting {
		return false, fmt.Errorf("[Worker: %v]: cannot send completing file %v", workerId, intermediateFiles)
	}

	return true, nil

}

func sendCompleteMessageForReduceTaskNumber(reduceTaskNumber int) bool {
	request := &SendCompleteReduceTaskRequest{
		ReduceNumber: reduceTaskNumber,
		WorkerId:     workerId,
	}

	response := &SendCompleteReduceTaskResponse{}

	call("Coordinator.SendCompleteReduceTask", request, response)

	return response.Acknowledged
}

func sendIntermediateFiles(intermediateFiles []string, originalFileName string) bool {
	request := &SendCompleteMapTaskRequest{
		DestinationFileName: intermediateFiles,
		OriginalFileName:    originalFileName,
		WorkerId:            workerId,
	}

	response := &SendCompleteMapTaskResponse{}

	call("Coordinator.SendCompleteMapTask", request, response)

	return response.Acknowledged
}

func writeIntermediateFile(intermediateFileName string, content []KeyValue, originalFileName string) (bool, error) {
	file, errOpenFile := os.OpenFile(intermediateFileName, os.O_WRONLY|os.O_CREATE, 0600)

	if errOpenFile != nil {
		return false, errOpenFile
	}

	encoder := json.NewEncoder(file)

	for _, c := range content {
		errEncode := encoder.Encode(c)
		if errEncode != nil {
			logger(fmt.Sprintf("Worker: %v: cannot encode intermediate file %v. Rollback....", workerId, file.Name()), "ERROR")
			ok, _ := rollback(intermediateFileName)

			if !ok {
				logger(fmt.Sprintf("Worker: %v: cannot rollback intermediate file %v. Rollback failed", workerId, file.Name()), "ERROR")
			}

			return false, errEncode
		}
	}

	signalCommitOk := signalCommitingMapTask(originalFileName)
	if !signalCommitOk {
		logger(fmt.Sprintf("[Worker: %v]: cannot signal commiting file %v. Rollback....", workerId, originalFileName), "ERROR")
		ok, _ := rollback(intermediateFileName)

		if !ok {
			logger(fmt.Sprintf("[Worker: %v]: cannot rollback intermediate file %v. Rollback failed", workerId, intermediateFileName), "ERROR")
		}

		return false, fmt.Errorf("[Worker: %v]: cannot signal commiting file %v. Rollback", workerId, originalFileName)
	}

	errCloseTempFile := file.Close()
	if errCloseTempFile != nil {
		logger(fmt.Sprintf("[Worker: %v]: cannot close tempfile %v. Rollback....", workerId, intermediateFileName), "ERROR")
		ok, _ := rollback(intermediateFileName)

		if !ok {
			logger(fmt.Sprintf("[Worker: %v]: cannot rollback intermediate file %v. Rollback failed", workerId, intermediateFileName), "ERROR")
		}

		return false, errCloseTempFile
	}

	return true, nil

}

func rollback(fileName string) (bool, error) {

	err := os.Remove(fileName)
	if err != nil {
		return false, err
	}

	return true, nil

}

func signalCommitingMapTask(fileName string) bool {
	request := &SendRequestToCompleteMapTaskRequest{
		FileName: fileName,
		WorkerId: workerId,
	}

	response := &SendRequestToCompleteMapTaskResponse{}

	call("Coordinator.NotifyCommitingFileForMapTask", request, response)

	return response.Ok
}

func signalCommitingReduceTask(reduceTaskNumber int) bool {
	request := &SendRequestToCompleteReduceTaskRequest{
		ReduceTaskNumber: reduceTaskNumber,
		WorkerId:         workerId,
	}

	response := &SendRequestToCompleteReduceTaskResponse{}

	call("Coordinator.NotifyCommitingFileForReduceTask", request, response)

	return response.Ok
}

func pingMapTask(fileName string) bool {
	request := &PingOnGoingMapTaskRequest{
		FileName: fileName,
		WorkerId: workerId,
	}

	response := &PingOnGoingMapTaskResponse{}

	call("Coordinator.PingOnGoingMapTask", request, response)

	return response.Ok
}

func pingReduceTask(reduceFileNumber int) bool {
	request := &PingOnGoingReduceTaskRequest{
		ReduceNumber: reduceFileNumber,
		WorkerId:     workerId,
	}

	response := &PingOnGoingReduceTaskResponse{}

	call("Coordinator.PingOnGoingReduceTask", request, response)

	return response.Ok
}

func getFileToWork() *ResponseTask {
	response := &ResponseTask{}

	ok := call("Coordinator.ServeTask", &RequestTask{WorkerId: workerId}, response)

	if !ok {
		logger(fmt.Sprintf("[Worker: %v]: cannot get task from coordinator", workerId), "ERROR")
		return nil
	}

	return response
}

func call(rpcname string, args interface{}, reply interface{}) bool {
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
