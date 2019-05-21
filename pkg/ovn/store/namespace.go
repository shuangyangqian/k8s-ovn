package store

import (
	"k8s.io/client-go/tools/cache"
)

type NamespaceLister struct {
	cache.Store
}
