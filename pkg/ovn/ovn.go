package ovn

import (
	"fmt"
	"github.com/shuangyangqian/k8s-ovn/pkg/kube"
	"github.com/shuangyangqian/k8s-ovn/pkg/values"
	"github.com/sirupsen/logrus"
	kapi "k8s.io/api/core/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	listerV1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog"
	"sync"
	"time"
)

// Controller structure is the object which holds the controls for starting
// and reacting upon the watched resources (e.g. pods, endpoints)
type Controller struct {
	kube            kube.Interface
	nodePortEnable  bool
	informerFactory informers.SharedInformerFactory

	// For each logical port, the number of network policies that want
	// to add a ingress deny rule.
	lspIngressDenyCache map[string]int

	// For each logical port, the number of network policies that want
	// to add a egress deny rule.
	lspEgressDenyCache map[string]int

	// A mutex for lspIngressDenyCache and lspEgressDenyCache
	lspMutex *sync.Mutex

	// A mutex for gatewayCache and logicalSwitchCache which holds
	// logicalSwitch information
	lsMutex *sync.Mutex

	// supports port_group?
	portGroupSupport bool

	// =============
	recorder record.EventRecorder

	podsLister     listerV1.PodLister
	podsSynced     cache.InformerSynced
	addPodQueue    workqueue.RateLimitingInterface
	deletePodQueue workqueue.RateLimitingInterface
	updatePodQueue workqueue.RateLimitingInterface

	elector *leaderelection.LeaderElector
}

const (
	// TCP is the constant string for the string "TCP"
	TCP = "TCP"

	// UDP is the constant string for the string "UDP"
	UDP = "UDP"

	controllerAgentName = "ovn-controller"
)

// NewOvnController creates a new OVN controller for creating logical network
// infrastructure and policy
func NewOvnController(kubeClient kubernetes.Interface, nodePortEnable bool) *Controller {
	utilruntime.Must(scheme.AddToScheme(scheme.Scheme))
	klog.V(4).Info("Creating event broadcaster")
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(klog.Infof)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: kubeClient.CoreV1().Events("")})
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, kapi.EventSource{Component: controllerAgentName})

	informerFactory := informers.NewSharedInformerFactory(kubeClient, time.Second*30)

	podInformer := informerFactory.Core().V1().Pods()

	controller := &Controller{
		kube:           &kube.Kube{KClient: kubeClient},
		nodePortEnable: nodePortEnable,

		podsLister:     podInformer.Lister(),
		podsSynced:     podInformer.Informer().HasSynced,
		addPodQueue:    workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "AddPod"),
		deletePodQueue: workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "DeletePod"),
		updatePodQueue: workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "UpdatePod"),

		informerFactory: informerFactory,

		recorder: recorder,
	}

	podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    controller.enqueueAddPod,
		DeleteFunc: controller.enqueueDeletePod,
		UpdateFunc: controller.enqueueUpdatePod,
	})

	return controller
}

// Run starts the actual watching. Also initializes any local structures needed.
func (oc *Controller) Run(stopChan <-chan struct{}) error {

	defer utilruntime.HandleCrash()

	defer oc.addPodQueue.ShutDown()
	defer oc.deletePodQueue.ShutDown()
	defer oc.updatePodQueue.ShutDown()

	logrus.Debugf("Starting ovn controller")

	oc.informerFactory.Start(stopChan)

	// Wait for the caches to be synced before starting workers
	logrus.Info("Waiting for informer caches to sync")
	if ok := cache.WaitForCacheSync(stopChan, oc.podsSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	klog.Info("Starting workers")
	for i := 0; i < 10; i++ {
		go wait.Until(oc.runAddPodWorker, time.Second, stopChan)
		go wait.Until(oc.runDeletePodWorker, time.Second, stopChan)
		go wait.Until(oc.runUpdatePodWorker, time.Second, stopChan)

	}

	klog.Info("Started workers")
	<-stopChan
	klog.Info("Shutting down workers")

	return nil
}

func (c *Controller) enqueueAddPod(obj interface{}) {
	pod := obj.(*kapi.Pod)
	if pod.Annotations[values.IPAddressStatic] != "" {
		return
	}
	if pod.Spec.HostNetwork {
		return
	}

	var key string
	var err error
	if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
		utilruntime.HandleError(err)
		return
	}

	klog.V(3).Infof("enqueue add pod %s", key)
	c.addPodQueue.AddRateLimited(key)
}

func (c *Controller) enqueueDeletePod(obj interface{}) {
	pod := obj.(*kapi.Pod)
	if pod.Spec.HostNetwork {
		return
	}
	var key string
	var err error
	if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
		utilruntime.HandleError(err)
		return
	}
	klog.V(3).Infof("enqueue delete pod %s", key)
	c.deletePodQueue.AddRateLimited(key)
}

func (c *Controller) enqueueUpdatePod(oldObj, newObj interface{}) {
	podOld := oldObj.(*kapi.Pod)
	podNew := newObj.(*kapi.Pod)
	if podOld.Spec.HostNetwork || podNew.Spec.HostNetwork {
		return
	}
	if podNew.Annotations[values.IPAddressStatic] != "" {
		return
	}
	var key string
	var err error
	if key, err = cache.MetaNamespaceKeyFunc(newObj); err != nil {
		utilruntime.HandleError(err)
		return
	}
	klog.V(3).Infof("enqueue delete pod %s", key)
	c.updatePodQueue.AddRateLimited(key)
}

func (c *Controller) runAddPodWorker() {
	for c.processNextAddPodWorkItem() {
	}
}

// processNextWorkItem will read a single work item off the workqueue and
// attempt to process it, by calling the syncHandler.
func (c *Controller) processNextAddPodWorkItem() bool {
	obj, shutdown := c.addPodQueue.Get()

	if shutdown {
		return false
	}

	// We wrap this block in a func so we can defer c.workqueue.Done.
	err := func(obj interface{}) error {
		// We call Done here so the workqueue knows we have finished
		// processing this item. We also must remember to call Forget if we
		// do not want this work item being re-queued. For example, we do
		// not call Forget if a transient error occurs, instead the item is
		// put back on the workqueue and attempted again after a back-off
		// period.
		defer c.addPodQueue.Done(obj)
		var key string
		var ok bool
		// We expect strings to come off the workqueue. These are of the
		// form namespace/name. We do this as the delayed nature of the
		// workqueue means the items in the informer cache may actually be
		// more up to date that when the item was initially put onto the
		// workqueue.
		if key, ok = obj.(string); !ok {
			// As the item in the workqueue is actually invalid, we call
			// Forget here else we'd go into a loop of attempting to
			// process a work item that is invalid.
			c.addPodQueue.Forget(obj)
			utilruntime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil
		}
		// Run the syncHandler, passing it the namespace/name string of the
		// Foo resource to be synced.
		if err := c.addPod(key); err != nil {
			// Put the item back on the workqueue to handle any transient errors.
			c.addPodQueue.AddRateLimited(key)
			return fmt.Errorf("error syncing '%s': %s, requeuing", key, err.Error())
		}
		// Finally, if no error occurs we Forget this item so it does not
		// get queued again until another change happens.
		c.addPodQueue.Forget(obj)
		return nil
	}(obj)

	if err != nil {
		utilruntime.HandleError(err)
		return true
	}

	return true
}

func (c *Controller) runDeletePodWorker() {
	for c.processNextDeletePodWorkItem() {
	}
}

// attempt to process it, by calling the syncHandler.
func (c *Controller) processNextDeletePodWorkItem() bool {
	obj, shutdown := c.deletePodQueue.Get()

	if shutdown {
		return false
	}

	// We wrap this block in a func so we can defer c.workqueue.Done.
	err := func(obj interface{}) error {
		// We call Done here so the workqueue knows we have finished
		// processing this item. We also must remember to call Forget if we
		// do not want this work item being re-queued. For example, we do
		// not call Forget if a transient error occurs, instead the item is
		// put back on the workqueue and attempted again after a back-off
		// period.
		defer c.deletePodQueue.Done(obj)
		var key string
		var ok bool
		// We expect strings to come off the workqueue. These are of the
		// form namespace/name. We do this as the delayed nature of the
		// workqueue means the items in the informer cache may actually be
		// more up to date that when the item was initially put onto the
		// workqueue.
		if key, ok = obj.(string); !ok {
			// As the item in the workqueue is actually invalid, we call
			// Forget here else we'd go into a loop of attempting to
			// process a work item that is invalid.
			c.deletePodQueue.Forget(obj)
			utilruntime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil
		}
		// Run the syncHandler, passing it the namespace/name string of the
		// Foo resource to be synced.
		if err := c.deletePod(key); err != nil {
			// Put the item back on the workqueue to handle any transient errors.
			c.deletePodQueue.AddRateLimited(key)
			return fmt.Errorf("error syncing '%s': %s, requeuing", key, err.Error())
		}
		// Finally, if no error occurs we Forget this item so it does not
		// get queued again until another change happens.
		c.deletePodQueue.Forget(obj)
		return nil
	}(obj)

	if err != nil {
		utilruntime.HandleError(err)
		return true
	}

	return true
}

func (c *Controller) runUpdatePodWorker() {
	for c.processNextUpdatePodWorkItem() {
	}
}

func (c *Controller) processNextUpdatePodWorkItem() bool {
	obj, shutdown := c.updatePodQueue.Get()

	if shutdown {
		return false
	}

	// We wrap this block in a func so we can defer c.workqueue.Done.
	err := func(obj interface{}) error {
		// We call Done here so the workqueue knows we have finished
		// processing this item. We also must remember to call Forget if we
		// do not want this work item being re-queued. For example, we do
		// not call Forget if a transient error occurs, instead the item is
		// put back on the workqueue and attempted again after a back-off
		// period.
		defer c.updatePodQueue.Done(obj)
		var key string
		var ok bool
		// We expect strings to come off the workqueue. These are of the
		// form namespace/name. We do this as the delayed nature of the
		// workqueue means the items in the informer cache may actually be
		// more up to date that when the item was initially put onto the
		// workqueue.
		if key, ok = obj.(string); !ok {
			// As the item in the workqueue is actually invalid, we call
			// Forget here else we'd go into a loop of attempting to
			// process a work item that is invalid.
			c.updatePodQueue.Forget(obj)
			utilruntime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil
		}
		// Run the syncHandler, passing it the namespace/name string of the
		// Foo resource to be synced.
		if err := c.updatePod(key); err != nil {
			// Put the item back on the workqueue to handle any transient errors.
			c.updatePodQueue.AddRateLimited(key)
			return fmt.Errorf("error syncing '%s': %s, requeuing", key, err.Error())
		}
		// Finally, if no error occurs we Forget this item so it does not
		// get queued again until another change happens.
		c.updatePodQueue.Forget(obj)
		return nil
	}(obj)

	if err != nil {
		utilruntime.HandleError(err)
		return true
	}

	return true
}
