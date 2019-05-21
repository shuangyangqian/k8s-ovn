package ovn

import (
	"errors"
	"fmt"
	"github.com/shuangyangqian/k8s-ovn/pkg/values"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/client-go/tools/cache"
	"strings"
	"time"

	"github.com/shuangyangqian/k8s-ovn/pkg/util"
	"github.com/sirupsen/logrus"
	kapi "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/wait"
)

var CurrentSubnetNotUniqueError = fmt.Errorf("found more than one subnet in db")

func (oc *Controller) syncPods(pods []interface{}) {
	// get the list of logical switch ports (equivalent to pods)
	expectedLogicalPorts := make(map[string]bool)
	for _, podInterface := range pods {
		pod, ok := podInterface.(*kapi.Pod)
		if !ok {
			logrus.Errorf("Spurious object in syncPods: %v", podInterface)
			continue
		}
		logicalPort := fmt.Sprintf("%s_%s", pod.Namespace, pod.Name)
		expectedLogicalPorts[logicalPort] = true
	}

	// get the list of logical ports from OVN
	output, stderr, err := util.RunOVNNbctl("--data=bare", "--no-heading",
		"--columns=name", "find", "logical_switch_port", "external_ids:pod=true")
	if err != nil {
		logrus.Errorf("Error in obtaining list of logical ports, "+
			"stderr: %q, err: %v",
			stderr, err)
		return
	}
	existingLogicalPorts := strings.Fields(output)
	for _, existingPort := range existingLogicalPorts {
		if _, ok := expectedLogicalPorts[existingPort]; !ok {
			// not found, delete this logical port
			logrus.Infof("Stale logical port found: %s. This logical port will be deleted.", existingPort)
			out, stderr, err := util.RunOVNNbctl("--if-exists", "lsp-del",
				existingPort)
			if err != nil {
				logrus.Errorf("Error in deleting pod's logical port "+
					"stdout: %q, stderr: %q err: %v",
					out, stderr, err)
			}
			if !oc.portGroupSupport {
				oc.deletePodAcls(existingPort)
			}
		}
	}
}

func (oc *Controller) addPod(key string) error {

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	pod, err := oc.podsLister.Pods(namespace).Get(name)
	if err != nil {
		return err
	}
	//check whether open ip static switch, if not, add lsp directly and return
	labels := pod.Labels
	// todo check this pod's controller
	// now just assume that it's controlled by deployment which versions is apps/v1
	deployment, err := oc.kube.GetDeployment(namespace, labels["app"])
	if err != nil {
		logrus.Error(fmt.Errorf("err:%s when get deployment with name:%s-%s", err, namespace, labels["app"]))
		return err
	}
	_, isStaticIP := deployment.Annotations[values.DeploymentStaticIPSwitch]
	if !isStaticIP {
		_, err := oc.addLogicalPort(pod)
		if err != nil {
			logrus.Errorf("err:%s when create lsp for pod:%s_%s", err, namespace, name)
		}
		return err
	}

	ipPool, isStaticIPool := deployment.Annotations[values.IPPoolStatic]
	if isStaticIPool {
		// try to find a ip in ipPool
		ipAndMacs := strings.Split(ipPool, ",")
		logrus.Debugf("convert ipPool:%s to ipAndMac:%s successfully", ipPool, ipAndMacs)
		if len(ipAndMacs) == int(*deployment.Spec.Replicas) {
			for i := 0; i <= 50; i++ {
				var isBreak bool
				pods, err := oc.kube.GetPods(namespace)
				if err != nil {
					logrus.Error(fmt.Errorf("err:%s when get pod list from namespace:%s", err, namespace))
				}
				logrus.Debugf("get %d pod in namespace %s", len(pods.Items), namespace)
				var result string
				for _, ipAndMac := range ipAndMacs {
					used := false
					for _, existPod := range pods.Items {
						existIPAndMask := fmt.Sprintf("%s %s", existPod.Annotations[values.IPAddressStatic],
							existPod.Annotations[values.MacAddressStatic])
						if ipAndMac == existIPAndMask {
							used = true
							break
						}
					}
					if !used {
						result = ipAndMac
						break
					}

				}
				// if result is not empty, means that the ipAndMac in ipPool of deployment is not
				// allocated to pod, this situation the ipAndMac have a fake port in ls
				if result != "" {
					result := strings.Split(result, " ")
					ip := strings.Split(result[0], "/")[0]
					mac := result[1]
					// if is thisdeleteLsp case, on ls may be a fake switch with the ip and mac
					err = oc.deleteLsp(pod.Namespace, ip)
					if err != nil {
						logrus.Error(fmt.Errorf("err:%s when delete lsp with ip:%s", err,
							pod.Annotations[values.DeploymentStaticIPSwitch]))
						return err
					}
					err := oc.AddLogicalPortWithIPAndMac(pod, "", ip, mac)
					if err != nil {
						logrus.Error(fmt.Errorf("err:%s when create lsp for pod:%s", err, name))
						return err
					}
					err = oc.patchPod(pod, ip, mac)
					if err != nil {
						logrus.Error(fmt.Errorf("err:%s when patch pod :%s", err, name))
						return err
					}
					isBreak = true
					return nil
				}
				if isBreak {
					break
				}
			}

		}

	}
	newPod, err := oc.addLogicalPort(pod)
	if err != nil {
		logrus.Errorf("err:%s when create lsp for pod:%s_%s", err, namespace, name)
		return err
	}
	newIPAndMac := fmt.Sprintf("%s %s", newPod.Annotations[values.IPAddressStatic],
		newPod.Annotations[values.MacAddressStatic])
	var newAnnotations string
	if ipPool == "" {
		newAnnotations = newIPAndMac
	} else {
		newAnnotations = ipPool + "," + newIPAndMac
	}

	// patch this ip and address to deployment
	err = oc.kube.SetAnnotationOnDeployment(deployment, values.IPPoolStatic, newAnnotations)
	if err != nil {
		logrus.Errorf("err %s when set annnotations:%s on deployment:%s", err, newAnnotations,
			deployment.Name)
		return err
	}
	return err
}

func (oc *Controller) updatePod(key string) error {

	namespace, name, _ := cache.SplitMetaNamespaceKey(key)
	pod, err := oc.podsLister.Pods(namespace).Get(name)
	if err != nil {
		return err
	}

	if pod.Annotations[values.IPAddressStatic] == "" {
		return oc.addPod(key)
	}

	return nil
}

func (oc *Controller) deletePod(key string) error {

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	pod, err := oc.podsLister.Pods(namespace).Get(name)

	logrus.Debug("deleting pod:%s", pod.Name)
	if pod.Spec.HostNetwork {
		return nil
	}

	// get the pod's deloyment to check whether the deployment is update or scroll down
	deployment, err := oc.kube.GetDeployment(namespace, pod.Labels["app"])
	if err != nil {
		if k8serrors.IsNotFound(err) {
			// if we get isNotFoundErr, means that the deployment that control this pod may be deleted
			// so delete lsp directly
			logrus.Errorf("cannot found deployment:%s with err:%s", pod.Labels["app"], err)
			logrus.Debugf("the deployment may be deleted, so delete lsp directly")
			err := oc.deleteLogicalPort(pod)
			if err != nil {
				logrus.Debugf("err:%s when delete lsp:%s_%s", err, pod.Namespace, pod.Name)
			}

		}
		return err
	}
	ipSwitch, isStaticIP := deployment.Annotations[values.DeploymentStaticIPSwitch]
	if !isStaticIP {
		// if the deployment doesn't open ip static switch
		// delete lsp directly
		err := oc.deleteLogicalPort(pod)
		if err != nil {
			logrus.Errorf("err:%s when delete lsp for pod:%s_%s", err, namespace, name)
		}
		return err
	}
	logrus.Debugf("deployment:%s open ip static switch with %s", deployment.Name, ipSwitch)

	// if run to this line, means the deployment which control this pod open ip static
	// so we not only delete lsp for pod, also need to create a fake port with the pod'ip and mac address
	err = oc.deleteLogicalPort(pod)
	if err != nil {
		logrus.Errorf("err:%s when create lsp for pod:%s_%s", err, namespace, name)
		return err
	}

	number := int(*deployment.Spec.Replicas)
	if number == 0 {
		logrus.Debugf("get replicas is zero with deployment:%s", pod.Labels["app"])
		// we should delete pn frpm deployment's annotations ipPool
		// todo delete pn from deployment annotations
		return nil
	}
	ipPool, ipStaticPool := deployment.Annotations[values.IPPoolStatic]
	if !ipStaticPool {
		logrus.Errorf("cannot get ipPool info for deployment:%s", deployment)
		return nil
	}
	pns, err := IpPoolToPodSubnet(ipPool)
	if err != nil {
		logrus.Errorf("cannot convert ipPool:%s to pns", ipPool)
		return err
	}
	logrus.Debugf("convert ipPool to pns successfully:%s ---> %d pns", ipPool, len(pns))
	logrus.Debugf("now we should check len(pns):%s and number(replicas):%d", len(pns), number)
	if number == 0 {
		// delete pn from pns and update deployment annotations
		var newPns []podNetwork
		for _, pn := range pns {
			if pn.IP.String()+"/"+pn.Mask != pod.Annotations[values.IPAddressStatic] {
				newPns = append(newPns, pn)
			}
		}
		err := oc.kube.SetAnnotationOnDeployment(deployment, values.IPPoolStatic, podNetworkString(newPns))
		if err != nil {
			logrus.Errorf("err:%s when set annotations for deployment:%s", podNetworkString(newPns), deployment.Name)
			return err
		}
		return nil
	}

	// means deployment update
	if len(pns) == number {
		fakePort := fmt.Sprintf("%s_fakeport_%s", pod.Namespace, rand.String(6))
		err = oc.AddLogicalPortWithIPAndMac(pod, fakePort, pod.Annotations[values.IPAddressStatic],
			pod.Annotations[values.MacAddressStatic])
		if err != nil {
			logrus.Errorf("err:%s when create lsp for pod:%s", err, namespace, fakePort)
			return err
		}
		return nil
	}

	// means scroll down
	if len(pns) > number {
		// delete pn from pns and update deployment annotations
		var newPns []podNetwork
		for _, pn := range pns {
			if pn.IP.String() != pod.Annotations[values.IPAddressStatic] {
				newPns = append(newPns, pn)
			}
		}
		err := oc.kube.SetAnnotationOnDeployment(deployment, values.IPPoolStatic, podNetworkString(newPns))
		if err != nil {
			logrus.Errorf("err:%s when set annotations for deployment:%s", podNetworkString(newPns), deployment.Name)
			return err
		}
	}

	return nil
}

func (oc *Controller) deletePodAcls(logicalPort string) {
	// delete the ACL rules on OVN that corresponding pod has been deleted
	uuids, stderr, err := util.RunOVNNbctl("--data=bare", "--no-heading",
		"--columns=_uuid", "find", "ACL",
		fmt.Sprintf("external_ids:logical_port=%s", logicalPort))
	if err != nil {
		logrus.Errorf("Error in getting list of acls "+
			"stdout: %q, stderr: %q, error: %v", uuids, stderr, err)
		return
	}

	if uuids == "" {
		logrus.Debugf("deletePodAcls: returning because find " +
			"returned no ACLs")
		return
	}

	uuidSlice := strings.Fields(uuids)
	for _, uuid := range uuidSlice {
		// Get logical switch
		out, stderr, err := util.RunOVNNbctl("--data=bare",
			"--no-heading", "--columns=_uuid", "find", "logical_switch",
			fmt.Sprintf("acls{>=}%s", uuid))
		if err != nil {
			logrus.Errorf("find failed to get the logical_switch of acl "+
				"uuid=%s, stderr: %q, (%v)", uuid, stderr, err)
			continue
		}

		if out == "" {
			continue
		}
		logicalSwitch := out

		_, stderr, err = util.RunOVNNbctl("--if-exists", "remove",
			"logical_switch", logicalSwitch, "acls", uuid)
		if err != nil {
			logrus.Errorf("failed to delete the allow-from rule %s for"+
				" logical_switch=%s, logical_port=%s, stderr: %q, (%v)",
				uuid, logicalSwitch, logicalPort, stderr, err)
			continue
		}
	}
}

func (oc *Controller) getLogicalPortUUID(logicalPort string) string {

	out, stderr, err := util.RunOVNNbctl("--if-exists", "get",
		"logical_switch_port", logicalPort, "_uuid")
	if err != nil {
		logrus.Errorf("Error while getting uuid for logical_switch_port "+
			"%s, stderr: %q, err: %v", logicalPort, stderr, err)
		return ""
	}

	return out
}

func (oc *Controller) getGatewayFromSwitch(logicalSwitch string) (string, string, error) {
	var gatewayIPMaskStr, stderr string
	var err error

	oc.lsMutex.Lock()
	defer oc.lsMutex.Unlock()
	gatewayIPMaskStr, stderr, err = util.RunOVNNbctl("--if-exists",
		"get", "logical_switch", logicalSwitch,
		"external_ids:gateway_ip")
	if err != nil {
		logrus.Errorf("Failed to get gateway IP:  %s, stderr: %q, %v",
			gatewayIPMaskStr, stderr, err)
		return "", "", err
	}
	if gatewayIPMaskStr == "" {
		return "", "", fmt.Errorf("Empty gateway IP in logical switch %s",
			logicalSwitch)
	}
	gatewayIPMask := strings.Split(gatewayIPMaskStr, "/")
	gatewayIP := gatewayIPMask[0]
	mask := gatewayIPMask[1]
	logrus.Debugf("Gateway IP: %s, Mask: %s", gatewayIP, mask)
	return gatewayIP, mask, nil
}

func (oc *Controller) deleteLogicalPort(pod *kapi.Pod) error {
	logicalPort := fmt.Sprintf("%s_%s", pod.Namespace, pod.Name)

	// delete lsp
	out, stderr, err := util.RunOVNNbctl("--if-exists", "lsp-del",
		logicalPort)
	if err != nil {
		logrus.Errorf("Error in deleting pod logical port "+
			"stdout: %q, stderr: %q, (%v)",
			out, stderr, err)
	}
	//ipAddress := oc.getIPFromOvnAnnotation(pod.Annotations["ovn"])

	oc.lspMutex.Lock()
	delete(oc.lspIngressDenyCache, logicalPort)
	delete(oc.lspEgressDenyCache, logicalPort)
	oc.lspMutex.Unlock()

	// just comments this, if we need it later, to search it more
	//if !oc.portGroupSupport {
	//	oc.deleteACLDenyOld(pod.Namespace, pod.Spec.NodeName, logicalPort,
	//		"Ingress")
	//	oc.deleteACLDenyOld(pod.Namespace, pod.Spec.NodeName, logicalPort,
	//		"Egress")
	//}
	//oc.deletePodFromNamespaceAddressSet(pod.Namespace, ipAddress)
	return nil
}

func (oc *Controller) waitForNamespaceLogicalSwitch(namespaceName string) error {

	// Otherwise wait for the node logical switch to be created by the ClusterController.
	// The node switch will be created very soon after startup so we should
	// only be waiting here once per node at most.
	if err := wait.PollImmediate(500*time.Millisecond, 30*time.Second, func() (bool, error) {
		if _, _, err := util.RunOVNNbctl("get", "logical_switch", namespaceName, "other-config"); err != nil {
			return false, nil
		}
		return true, nil
	}); err != nil {
		logrus.Errorf("timed out waiting for namespace %q logical switch: %v", namespaceName, err)
		return err
	}

	return nil
}

func (oc *Controller) addLogicalPort(pod *kapi.Pod) (*kapi.Pod, error) {

	portName := fmt.Sprintf("%s_%s", pod.Namespace, pod.Name)
	out, stderr, err := util.RunOVNNbctl("--wait=sb", "--",
		"--may-exist", "lsp-add", pod.Namespace, portName,
		"--", "lsp-set-addresses",
		portName, "dynamic", "--", "set",
		"logical_switch_port", portName,
		"external-ids:namespace="+pod.Namespace,
		"external-ids:logical_switch="+pod.Namespace,
		"external-ids:deployment="+pod.Labels["app"],
		"external-ids:pod=true")
	if err != nil {
		logrus.Errorf("Error while creating logical port %s "+
			"stdout: %q, stderr: %q (%v)",
			portName, out, stderr, err)
		return nil, err
	}

	gatewayIP, gatewayMask, err := oc.getGatewayFromSwitch(pod.Namespace)
	if err != nil {
		logrus.Errorf("Error obtaining gateway address for switch %s", pod.Namespace)
		return nil, err
	}

	out, stderr, err = util.RunOVNNbctl("get",
		"logical_switch_port", portName, "dynamic_addresses")
	if err != nil {
		logrus.Errorf("Error while obtaining addresses for %s - %v", portName,
			err)
		return nil, err
	}
	// static addresses have format ["0a:00:00:00:00:01 192.168.1.3"], while
	// dynamic addresses have format "0a:00:00:00:00:01 192.168.1.3".
	outStr := strings.TrimLeft(out, `[`)
	outStr = strings.TrimRight(outStr, `]`)
	outStr = strings.Trim(outStr, `"`)
	addresses := strings.Split(outStr, " ")
	if len(addresses) != 2 {
		logrus.Errorf("Error while obtaining addresses for %s", portName)
		return nil, fmt.Errorf("error while obtaining addresses for port:%s", portName)
	}

	ipAddress := fmt.Sprintf("%s/%s", addresses[1], gatewayMask)
	pod, err = oc.kube.SetAnnotationOnPod(pod, values.IPAddressStatic, ipAddress)
	if err != nil {
		return nil, err
	}
	pod, err = oc.kube.SetAnnotationOnPod(pod, values.MacAddressStatic, addresses[0])
	if err != nil {
		return nil, err
	}

	pod, err = oc.kube.SetAnnotationOnPod(pod, values.PodGatewayIP, gatewayIP)
	if err != nil {
		return nil, err
	}
	return pod, nil

	//oc.addPodToNamespaceAddressSet(pod.Namespace, addresses[1])
}

// AddLogicalPortWithIP add logical port with static ip address
// and mac adddress for the pod, to use this func, you must confirm
// the pod annotations have ip and mac
func (oc *Controller) AddLogicalPortWithIPAndMac(pod *kapi.Pod, portName, ip, mac string) error {
	if pod.Spec.HostNetwork {
		return nil
	}

	if portName == "" {
		portName = fmt.Sprintf("%s_%s", pod.Namespace, pod.Name)
	}
	logicalSwitch := pod.Namespace
	logrus.Debugf("Creating logical port for %s on switch %s", portName,
		logicalSwitch)

	logrus.Debugf("start create pod lsp with ip:%s and mac:%s for pod:%s",
		ip, mac, pod.Name)

	out, stderr, err := util.RunOVNNbctl("--may-exist", "lsp-add",
		logicalSwitch, portName, "--", "lsp-set-addresses", portName,
		fmt.Sprintf("%s %s", mac, ip), "--", "set",
		"logical_switch_port", portName,
		"external-ids:namespace="+pod.Namespace,
		"external-ids:logical_switch="+logicalSwitch,
		"external-ids:deployment="+pod.Labels["app"],
		"external-ids:pod=true", "--", "--if-exists",
		"clear", "logical_switch_port", portName, "dynamic_addresses")
	if err != nil {
		logrus.Errorf("Failed to add logical port to switch "+
			"stdout: %q, stderr: %q (%v)",
			out, stderr, err)
		return err
	}

	return nil
}

// findIPsWithPod return wheather this pod should create with static ip
// returned:
// string: the ipPool info for the pods
// bool: use or not use static ip
// err: some unexpected err
func (oc *Controller) findIPsWithPod(pod *kapi.Pod) (string, bool, error) {
	// get labels for this pod
	deploymentName := pod.Labels["app"]
	deployment, err := oc.kube.GetDeployment(pod.Namespace, deploymentName)
	if err != nil {
		logrus.Errorf("err:%s when get deployment for pod %s", err, pod.Name)
		return "", false, err
	}
	if _, ok := deployment.Annotations[values.DeploymentStaticIPSwitch]; !ok {
		return "", false, err
	}
	// we sleep for 5 seconds for the deployment annotations

	for i := 0; i < 100000000; i++ {
		ipPool, ipStatic := deployment.Annotations[values.IPPoolStatic]
		if ipStatic {
			return ipPool, true, nil
		}
		time.Sleep(time.Second)
	}
	return "", false, fmt.Errorf("the deployment %s which controlled pod %s open ip static func,"+
		"but doesn't have ipPool value", deploymentName, pod.Name)
}

func (oc *Controller) deleteLsp(ls, ip string) error {
	output, stderr, err := util.RunOVNNbctl("--data=bare", "--no-heading",
		"--columns=name", "find", "logical_switch_port", "external_ids:pod=true",
		fmt.Sprintf("external_ids:namespace=%s", ls))
	if err != nil {
		logrus.Errorf("Error in obtaining list of logical ports, "+
			"stderr: %q, err: %v",
			stderr, err)
		return err
	}
	allPorts := strings.Fields(output)
	logrus.Debugf("get %d ports in switch %s:%s", len(allPorts), ls, allPorts)
	fakePort := ""
	// the first port which contains fakeport is we needed
	for _, port := range allPorts {
		out, _, err := util.RunOVNNbctl("get",
			"logical_switch_port", port, "addresses")
		if err != nil {
			logrus.Errorf("Error while obtaining addresses for %s - %v", port,
				err)
			return err
		}
		// static addresses have format ["0a:00:00:00:00:01 192.168.1.3"], while
		outStr := strings.TrimLeft(out, `[`)
		outStr = strings.TrimRight(outStr, `]`)
		outStr = strings.Trim(outStr, `"`)
		addresses := strings.Split(outStr, " ")
		if len(addresses) != 2 {
			msg := fmt.Sprintf("Error while obtaining addresses for %s", port)
			return errors.New(msg)
		}
		if ip == addresses[1] {
			fakePort = port
		}
	}

	// delete fake lsp port
	out, stderr, err := util.RunOVNNbctl("--if-exists", "lsp-del",
		fakePort)
	if err != nil {
		logrus.Errorf("Error in deleting pod's logical port "+
			"stdout: %q, stderr: %q err: %v",
			out, stderr, err)
	}
	logrus.Debugf("delete fake lsp successfully: %s", fakePort)
	return nil
}

func (oc *Controller) patchPod(pod *kapi.Pod, ip, mac string) error {
	gatewayIP, gatewayMask, err := oc.getGatewayFromSwitch(pod.Namespace)
	if err != nil {
		logrus.Error("cannot get gateway from ls:%s with err:%s", pod.Namespace, err)
	}

	ipAddress := fmt.Sprintf("%s/%s", ip, gatewayMask)

	// now update the pod to k8s api server
	pod, err = oc.kube.SetAnnotationOnPod(pod, values.IPAddressStatic, ipAddress)
	if err != nil {
		return err
	}
	pod, err = oc.kube.SetAnnotationOnPod(pod, values.MacAddressStatic, mac)
	if err != nil {
		return err
	}
	pod, err = oc.kube.SetAnnotationOnPod(pod, values.PodGatewayIP, gatewayIP)
	if err != nil {
		return err
	}
	return nil
}
