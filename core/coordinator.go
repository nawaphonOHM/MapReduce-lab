package core

import (
	"fmt"
	"log"
	"regexp"
	"slices"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	mutexLock   sync.Mutex
	mapFiles    []*MapReadyJob
	reduceFiles []*ReduceReadyJob
	nReduce     int

	allDone bool

	onGoingMapTasks    []*OnGoingMapTask
	onGoingReduceTasks []*OnGoingReduceTask

	hasAppearingReduceTaskNumber map[int]bool
}

type MapReadyJob struct {
	fileName  string
	jobNumber int
}

type ReduceReadyJob struct {
	reduceNumber int
}

type OnGoingReduceTask struct {
	jobNumber int

	timeToLive int

	state string
}

type OnGoingMapTask struct {
	fileName string

	timeToLive int

	state string

	jobNumber int
}

func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()

	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) PingOnGoingReduceTask(request *PingOnGoingReduceTaskRequest, response *PingOnGoingReduceTaskResponse) error {

	log.Printf("WorkerId: %v sends ping for reduce task task number that is operating %v", request.WorkerId, request.ReduceNumber)

	c.mutexLock.Lock()
	defer c.mutexLock.Unlock()

	location := slices.IndexFunc(c.onGoingReduceTasks, func(predicate *OnGoingReduceTask) bool {
		return predicate.jobNumber == request.ReduceNumber
	})

	if location == -1 {
		response.Ok = false
		log.Printf("It's too late for reset ttl for ongoing reduce number: %v", request.ReduceNumber)
		return nil
	}

	c.onGoingReduceTasks[location].timeToLive = 10

	response.Ok = true

	log.Printf("reset ttl for reduceTaskNumber %v is done", request.ReduceNumber)

	return nil

}

func (c *Coordinator) PingOnGoingMapTask(request *PingOnGoingMapTaskRequest, response *PingOnGoingMapTaskResponse) error {

	log.Printf("WorkerId: %v sends ping for map task file that is operating %v", request.WorkerId, request.FileName)

	c.mutexLock.Lock()
	defer c.mutexLock.Unlock()

	location := slices.IndexFunc(c.onGoingMapTasks, func(predicate *OnGoingMapTask) bool {
		return predicate.fileName == request.FileName
	})

	if location == -1 {
		response.Ok = false
		log.Printf("It's too late for reset ttl for ongoing map task file: %v", request.FileName)
		return nil
	}

	c.onGoingMapTasks[location].timeToLive = 10

	response.Ok = true

	log.Printf("reset ttl for file %v is done", request.FileName)

	return nil

}

func (c *Coordinator) SendCompleteMapTask(request *SendCompleteMapTaskRequest, response *SendCompleteMapTaskResponse) error {

	log.Printf("WorkerId: %v sends completing mapTask for file that is operating %v", request.WorkerId, request.OriginalFileName)

	c.mutexLock.Lock()
	defer c.mutexLock.Unlock()

	location := slices.IndexFunc(c.onGoingMapTasks, func(predicate *OnGoingMapTask) bool {
		return request.OriginalFileName == predicate.fileName
	})

	if location == -1 {
		response.Acknowledged = false
		log.Printf("It's too late for completing file: %v as ttl has been expired", request.OriginalFileName)
		return nil
	}

	newOnGoingTask := make([]*OnGoingMapTask, 0)

	for index, task := range c.onGoingMapTasks {
		if index != location {
			newOnGoingTask = append(newOnGoingTask, task)
		}
	}

	c.onGoingMapTasks = newOnGoingTask

	compile := regexp.MustCompile("mr-([0-9]+)-([0-9]+)")

	for _, destinationFile := range request.DestinationFileName {
		submatch := compile.FindStringSubmatch(destinationFile)

		if len(submatch) != 3 {
			log.Fatalf("cannot parse file name %v; the size of submatch is %v", destinationFile, len(submatch))
		}

		reduceNumberAsStr := submatch[2]

		reduceNumber, err := strconv.Atoi(reduceNumberAsStr)

		if err != nil {
			log.Fatalf("cannot parse number %v", reduceNumberAsStr)
		}

		if _, found := c.hasAppearingReduceTaskNumber[reduceNumber]; found {
			continue
		}

		c.hasAppearingReduceTaskNumber[reduceNumber] = true

		found := slices.IndexFunc(c.reduceFiles, func(predicate *ReduceReadyJob) bool {
			return predicate.reduceNumber == reduceNumber
		}) >= 0

		if !found {
			c.reduceFiles = append(c.reduceFiles, &ReduceReadyJob{reduceNumber})
		}
	}

	{
		reduceFileString := make([]string, 0)

		for _, reduceFile := range c.reduceFiles {
			reduceFileString = append(reduceFileString, fmt.Sprintf("%v", reduceFile.reduceNumber))
		}

		log.Printf("There is now reduceTaskNumber %v", reduceFileString)
	}

	response.Acknowledged = true

	return nil

}

func (c *Coordinator) SendCompleteReduceTask(request *SendCompleteReduceTaskRequest, response *SendCompleteReduceTaskResponse) error {

	log.Printf("WorkerId: %v sends completing reduceTask for reduceTaskNumber that is operating %v", request.WorkerId, request.ReduceNumber)

	c.mutexLock.Lock()
	defer c.mutexLock.Unlock()

	location := slices.IndexFunc(c.onGoingReduceTasks, func(predicate *OnGoingReduceTask) bool {
		return request.ReduceNumber == predicate.jobNumber
	})

	if location == -1 {
		response.Acknowledged = false
		log.Printf("It's too late for completing reduceTaskNumber: %v as ttl has been expired", request.ReduceNumber)
		return nil
	}

	newOnGoingTask := make([]*OnGoingReduceTask, 0)

	for index, task := range c.onGoingReduceTasks {
		if index != location {
			newOnGoingTask = append(newOnGoingTask, task)
		}
	}

	c.onGoingReduceTasks = newOnGoingTask

	response.Acknowledged = true

	return nil

}

func (c *Coordinator) NotifyCommitingFileForReduceTask(request *SendRequestToCompleteReduceTaskRequest, response *SendRequestToCompleteReduceTaskResponse) error {
	log.Printf("WorkerId: %v notifies commiting reduceTaskNumber %v ", request.WorkerId, request.ReduceTaskNumber)

	c.mutexLock.Lock()
	defer c.mutexLock.Unlock()

	location := slices.IndexFunc(c.onGoingReduceTasks, func(predicate *OnGoingReduceTask) bool {
		return predicate.jobNumber == request.ReduceTaskNumber
	})

	if location == -1 {
		response.Ok = false
		log.Printf("It's too late for comming reduceTaskNumber: %v as ttl has been expired", request.ReduceTaskNumber)
		return nil
	}

	c.onGoingReduceTasks[location].state = "commiting"
	c.onGoingReduceTasks[location].timeToLive = 10

	response.Ok = true

	log.Printf("commiting reduceTaskNumber %v is done", request.ReduceTaskNumber)

	return nil
}

func (c *Coordinator) NotifyCommitingFileForMapTask(request *SendRequestToCompleteMapTaskRequest, response *SendRequestToCompleteMapTaskResponse) error {
	log.Printf("WorkerId: %v notifies commiting file %v ", request.WorkerId, request.FileName)

	c.mutexLock.Lock()
	defer c.mutexLock.Unlock()

	location := slices.IndexFunc(c.onGoingMapTasks, func(predicate *OnGoingMapTask) bool {
		return predicate.fileName == request.FileName
	})

	if location == -1 {
		response.Ok = false
		log.Printf("It's too late for comming file: %v as ttl has been expired", request.FileName)
		return nil
	}

	c.onGoingMapTasks[location].state = "commiting"
	c.onGoingMapTasks[location].timeToLive = 10

	response.Ok = true

	log.Printf("commiting file %v is done", request.FileName)

	return nil
}

func (c *Coordinator) ServeTask(request *RequestTask, response *ResponseTask) error {

	log.Printf("WorkerId: %v requests task to work", request.WorkerId)

	c.mutexLock.Lock()
	defer c.mutexLock.Unlock()

	if len(c.mapFiles) > 0 {

		log.Printf("[For workerId: %v]: There are %v map files left", request.WorkerId, len(c.mapFiles))

		file := c.mapFiles[0]

		response.TaskNumber = file.jobNumber
		response.JobType = "map"
		response.Filename = file.fileName
		response.NReduce = c.nReduce

		log.Printf("[For workerId: %v]: response task: %v", request.WorkerId, response)

		c.onGoingMapTasks = append(c.onGoingMapTasks, &OnGoingMapTask{
			fileName:   file.fileName,
			timeToLive: 10,
			state:      "mapping",
			jobNumber:  file.jobNumber,
		})

		c.mapFiles = c.mapFiles[1:]

		return nil
	}

	if len(c.onGoingMapTasks) > 0 {
		response.Filename = ""
		response.TaskNumber = -1
		response.JobType = "pleaseWait"
		response.NReduce = c.nReduce

		return nil
	}

	if len(c.reduceFiles) > 0 {

		log.Printf("[For workerId: %v]: There are %v reduce files left", request.WorkerId, len(c.reduceFiles))

		file := c.reduceFiles[0]

		response.TaskNumber = file.reduceNumber
		response.JobType = "reduce"
		response.Filename = fmt.Sprintf("mr-*-%v", file.reduceNumber)
		response.NReduce = c.nReduce

		log.Printf("[For workerId: %v]: response task: %v", request.WorkerId, response)

		c.onGoingReduceTasks = append(c.onGoingReduceTasks, &OnGoingReduceTask{
			jobNumber:  file.reduceNumber,
			timeToLive: 10,
			state:      "reducing",
		})

		c.reduceFiles = c.reduceFiles[1:]

		return nil
	}

	if len(c.onGoingReduceTasks) > 0 {
		response.Filename = ""
		response.TaskNumber = -1
		response.JobType = "pleaseWait"
		response.NReduce = c.nReduce

		return nil
	}

	log.Printf("[For workerId: %v]: There are no files left", request.WorkerId)

	response.TaskNumber = -1
	response.JobType = "noWork"
	response.Filename = ""
	response.NReduce = c.nReduce

	log.Printf("[For workerId: %v]: response task: %v", request.WorkerId, response)

	return nil

}

func (c *Coordinator) validateLoopEntryPoint() {

	for !c.allDone {
		time.Sleep(time.Second)

		c.mutexLock.Lock()

		c.reduceTimeToLiveByOne()

		c.validateOnGoingMapTask()
		c.validateOnGoingReduceTask()

		done := c.validateLoop()

		if done {
			c.allDone = true
		}

		c.mutexLock.Unlock()
	}
}

func (c *Coordinator) validateOnGoingMapTask() bool {

	if len(c.onGoingMapTasks) == 0 {
		return true
	}

	passResult := make([]*OnGoingMapTask, 0)

	failedTask := make([]*OnGoingMapTask, 0)

	for _, task := range c.onGoingMapTasks {
		if task.timeToLive == 0 {
			failedTask = append(failedTask, task)
		} else {
			passResult = append(passResult, task)
		}
	}

	for _, task := range failedTask {
		log.Printf("task %v failed", task.fileName)
		c.mapFiles = append(c.mapFiles, &MapReadyJob{task.fileName, task.jobNumber})
	}

	c.onGoingMapTasks = passResult

	return true
}

func (c *Coordinator) validateOnGoingReduceTask() bool {

	if len(c.onGoingReduceTasks) == 0 {
		return true
	}

	passResult := make([]*OnGoingReduceTask, 0)

	failedTask := make([]*OnGoingReduceTask, 0)

	for _, task := range c.onGoingReduceTasks {
		if task.timeToLive == 0 {
			failedTask = append(failedTask, task)
		} else {
			passResult = append(passResult, task)
		}
	}

	for _, task := range failedTask {
		log.Printf("reduceTaskNumber %v failed", task.jobNumber)
		c.reduceFiles = append(c.reduceFiles, &ReduceReadyJob{task.jobNumber})
	}

	c.onGoingReduceTasks = passResult

	return true
}

func (c *Coordinator) reduceTimeToLiveByOne() {

	for _, task := range c.onGoingMapTasks {
		task.timeToLive--
	}

	for _, task := range c.onGoingReduceTasks {
		task.timeToLive--
	}

}

func (c *Coordinator) validateLoop() bool {

	if len(c.mapFiles) > 0 {
		return false
	}

	if len(c.reduceFiles) > 0 {
		return false
	}

	if len(c.onGoingMapTasks) > 0 {
		return false
	}

	if len(c.onGoingReduceTasks) > 0 {
		return false
	}

	return true
}

func (c *Coordinator) Done() bool {

	c.mutexLock.Lock()
	done := c.allDone
	c.mutexLock.Unlock()

	return done
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.mapFiles = make([]*MapReadyJob, 0)

	for index, file := range files {
		c.mapFiles = append(c.mapFiles, &MapReadyJob{file, index})
	}

	c.nReduce = nReduce

	c.onGoingMapTasks = make([]*OnGoingMapTask, 0)
	c.onGoingReduceTasks = make([]*OnGoingReduceTask, 0)

	c.reduceFiles = make([]*ReduceReadyJob, 0)

	c.allDone = false

	c.hasAppearingReduceTaskNumber = make(map[int]bool)

	go c.validateLoopEntryPoint()

	c.server()
	return &c
}
