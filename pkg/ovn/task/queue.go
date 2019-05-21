package task

import "k8s.io/client-go/util/workqueue"

type Queue struct {

	// queue is the work queue the worker polls
	queue workqueue.RateLimitingInterface
	// sync is called for each item in the queue
	sync func(interface{}) error
	// workerDone is closed when the worker exits
	workerDone chan bool
	// fn makes a key for an API object
	fn func(obj interface{}) (interface{}, error)
	// lastSync is the Unix epoch time of the last execution of 'sync'
	lastSync int64
}
