package store

import (
	"fmt"
	"github.com/eapache/channels"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	clientcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"time"
)

// Storer is the interface that wraps the required methods to gather information
// about ingresses, services, secrets and ingress annotations.
type Storer interface {
	// GetConfigMap returns the ConfigMap matching key.
	//GetConfigMap(key string) (*corev1.ConfigMap, error)
	//
	//// GetSecret returns the Secret matching key.
	//GetSecret(key string) (*corev1.Secret, error)
	//
	//// GetService returns the Service matching key.
	//GetService(key string) (*corev1.Service, error)
	//
	//// GetServiceEndpoints returns the Endpoints of a Service matching key.
	//GetServiceEndpoints(key string) (*corev1.Endpoints, error)
	//
	//// ListIngresses returns a list of all Ingresses in the store.
	//ListIngresses() []*ingress.Ingress
	//
	//// GetRunningControllerPodsCount returns the number of Running ingress-nginx controller Pods.
	//GetRunningControllerPodsCount() int
	//
	//// GetLocalSSLCert returns the local copy of a SSLCert
	//GetLocalSSLCert(name string) (*ingress.SSLCert, error)
	//
	//// ListLocalSSLCerts returns the list of local SSLCerts
	//ListLocalSSLCerts() []*ingress.SSLCert
	//
	//// GetAuthCertificate resolves a given secret name into an SSL certificate.
	//// The secret must contain 3 keys named:
	////   ca.crt: contains the certificate chain used for authentication
	//GetAuthCertificate(string) (*resolver.AuthSSLCert, error)
	//
	//// GetDefaultBackend returns the default backend configuration
	//GetDefaultBackend() defaults.Backend

	// Run initiates the synchronization of the controllers
	Run(stopCh chan struct{})
}

type EventType string

const (
	CreateEvent EventType = "CREATE"
	UpdateEvent EventType = "UPDATE"
	DeleteEvent EventType = "DELETE"
)

type Event struct {
	Type EventType
	Obj  interface{}
}

type Informer struct {
	Namespace cache.SharedIndexInformer
	//Endpoint      cache.SharedIndexInformer
	//Service       cache.SharedIndexInformer
	Pod cache.SharedIndexInformer
	//Deployment    cache.SharedIndexInformer
	//NetworkPolicy cache.SharedIndexInformer
}

type Lister struct {
	Pod       PodLister
	Namespace NamespaceLister
}

// NotExistsError is returned when an object does not exist in a local store.
type NotExistsError string

// Error implements the error interface.
func (e NotExistsError) Error() string {
	return fmt.Sprintf("no object matching key %q in local store", string(e))
}

func (i *Informer) Run(stopCh chan struct{}) {
	go i.Namespace.Run(stopCh)

	if !cache.WaitForCacheSync(stopCh,
		i.Namespace.HasSynced) {
		runtime.HandleError(fmt.Errorf("Timed out waiting for caches to sync"))
	}

	// functions have returned 'true'
	// in big clusters, deltas can keep arriving even after HasSynced

	time.Sleep(1 * time.Second)

	go i.Pod.Run(stopCh)

	if !cache.WaitForCacheSync(stopCh,
		i.Pod.HasSynced) {
		runtime.HandleError(fmt.Errorf("Timed out waiting for caches to sync"))
	}

}

// k8sStore internal Storer implementation using informers and thread safe stores
type k8sStore struct {

	// informer contains the cache Informers
	informers *Informer

	// listers contains the cache.Store interfaces used in the ingress controller
	listers *Lister

	// updateCh
	updateCh *channels.RingChannel
}

func New(namespace string, reSyncPeriod time.Duration,
	client clientset.Interface, updateCh *channels.RingChannel) Storer {

	store := &k8sStore{
		informers: &Informer{},
		listers:   &Lister{},
		updateCh:  updateCh,
	}

	eventBroadcaster := record.NewBroadcaster()

	eventBroadcaster.StartRecordingToSink(&clientcorev1.EventSinkImpl{
		Interface: client.CoreV1().Events(namespace),
	})

	//recorder := eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{
	//	Component: "ovn kubernetes controller",
	//})

	infFactory := informers.NewSharedInformerFactoryWithOptions(client, reSyncPeriod,
		informers.WithNamespace(namespace),
		informers.WithTweakListOptions(func(*metav1.ListOptions) {}))

	store.informers.Namespace = infFactory.Core().V1().Namespaces().Informer()
	store.informers.Pod = infFactory.Core().V1().Pods().Informer()

	store.listers.Namespace.Store = store.informers.Namespace.GetStore()
	store.listers.Pod.Store = store.informers.Pod.GetStore()

	namespaceEventHandler := cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {

		},
		DeleteFunc: func(obj interface{}) {

		},
		UpdateFunc: func(oldObj, newObj interface{}) {

		},
	}

	podEventHandler := cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {

		},
		DeleteFunc: func(obj interface{}) {

		},
		UpdateFunc: func(oldObj, newObj interface{}) {

		},
	}

	store.informers.Pod.AddEventHandler(podEventHandler)
	store.informers.Namespace.AddEventHandler(namespaceEventHandler)

	return store
}

// Run initiates the synchronization of the informers and the initial
// synchronization of the secrets.
func (s *k8sStore) Run(stopCh chan struct{}) {
	// start informers
	s.informers.Run(stopCh)

}
