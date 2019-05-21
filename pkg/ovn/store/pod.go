package store

import "k8s.io/client-go/tools/cache"

// PodLister makes a Store that lists Pods.
type PodLister struct {
	cache.Store
}
