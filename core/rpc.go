package core

import "os"
import "strconv"

type PingOnGoingMapTaskRequest struct {
	FileName string

	WorkerId int
}

type PingOnGoingMapTaskResponse struct {
	Ok bool
}

type PingOnGoingReduceTaskRequest struct {
	ReduceNumber int

	WorkerId int
}

type PingOnGoingReduceTaskResponse struct {
	Ok bool
}

type RequestTask struct {
	WorkerId int
}

type ResponseTask struct {
	Filename   string
	JobType    string
	TaskNumber int

	NReduce int
}

type SendRequestToCompleteReduceTaskRequest struct {
	ReduceTaskNumber int

	WorkerId int
}

type SendRequestToCompleteReduceTaskResponse struct {
	Ok bool
}

type SendRequestToCompleteMapTaskRequest struct {
	FileName string

	WorkerId int
}

type SendRequestToCompleteMapTaskResponse struct {
	Ok bool
}

type SendCompleteMapTaskRequest struct {
	DestinationFileName []string
	OriginalFileName    string

	WorkerId int
}

type SendCompleteMapTaskResponse struct {
	Acknowledged bool
}

type SendCompleteReduceTaskRequest struct {
	ReduceNumber int

	WorkerId int
}

type SendCompleteReduceTaskResponse struct {
	Acknowledged bool
}

type TaskResponse struct {
	Filename string
	JobType  string

	NReduce int
}

type TaskRequest struct {
}

type ReduceResultRequest struct {
	ReduceFileName string
}

type ReduceResultResponse struct {
}

type MapResultRequest struct {
	FilenameHasBeenPerformed string
	IntermediateResultFiles  []string
}

func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
