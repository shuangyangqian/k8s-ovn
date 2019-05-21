package ovn

import (
	"fmt"
	"github.com/shuangyangqian/k8s-ovn/pkg/util"
	"github.com/shuangyangqian/k8s-ovn/pkg/values"
	"github.com/sirupsen/logrus"
	kapps "k8s.io/api/apps/v1"
	kapi "k8s.io/api/core/v1"
	"net"
	"strings"
)

// syncDeployments main job is clean the ipPool in annotations
func (oc *Controller) syncDeployments(deployments []interface{}) {
	// get the list of logical switch ports (equivalent to pods)
	deploys := make(map[string]*kapps.Deployment)
	for _, deploymentInterface := range deployments {
		deployment, ok := deploymentInterface.(*kapps.Deployment)
		if !ok {
			logrus.Errorf("Spurious object in syncDeployments: %v", deploymentInterface)
			continue
		}
		deploys[deployment.Name] = deployment
	}

	for name, deploy := range deploys {
		logrus.Debugf("sync info for deployment %s", name)

		_, isStaticIP := deploy.Annotations[values.DeploymentStaticIPSwitch]
		if !isStaticIP {
			logrus.Debugf("deployment:%s doesn't open ip static switch", name)
			continue
		}

		ipPool, isStaticIPPool := deploy.Annotations[values.IPPoolStatic]
		if !isStaticIPPool {
			logrus.Warnf("deployment %s doesn't have ipPool info when it open the ip static switch", name)
		}
		pns, err := IpPoolToPodSubnet(ipPool)
		if err != nil {
			logrus.Warnf("cannot convert ipPool info to pod subnet in deployment %s", name)
			continue
		}
		pods, err := oc.getPodsFromNamespacesDeployment(deploy.Namespace, deploy.Name)
		if err != nil {
			logrus.Errorf("get pod from deployment %s in namespace %s", deploy.Name, deploy.Namespace)
		}
		if len(pns) != len(pods) {
			logrus.Warnf("ipPool info doesn't match pod info, %d != %d", len(pns), len(pods))
		}

		var newPodSubnet []podNetwork
		for _, pn := range pns {
			ip := pn.IP.String()
			for _, pod := range pods {
				if pod.Status.PodIP == ip {
					newPodSubnet = append(newPodSubnet, pn)
				}
			}
		}

		// update deploy annotations with new ipPool info
		err = oc.kube.SetAnnotationOnDeployment(deploy, values.IPPoolStatic, podNetworkString(newPodSubnet))
		if err != nil {
			logrus.Errorf("when set annotations to deployment with key:%s value:%s meet err:%s",
				values.IPPoolStatic, podNetworkString(newPodSubnet), err)
		}
	}
}

// AddDeployment when a new deployment added, we should detect whether a ipStatic is needed
// if yes, we should allocated ips for it's pods
func (oc *Controller) AddDeployment(deployment *kapps.Deployment) error {
	logrus.Debugf("Adding deployment: %s", deployment.Name)

	// if there is a switch in deployment's annotations, means that we should
	// add ip static function for this deployment
	_, isStaticIP := deployment.Annotations[values.DeploymentStaticIPSwitch]
	_, isStaticIPPool := deployment.Annotations[values.IPPoolStatic]
	if isStaticIP && !isStaticIPPool {
		err := oc.addAnnotationsToDeployment(deployment)
		if err != nil {
			return err
		}
	}
	return nil

}

// getIPsFromLogicalSwitch return ips from logicalSwitch which counts is number
func (oc *Controller) getIPsFromLogicalSwitch(deployment *kapps.Deployment, logicalSwitch string,
	number int) ([]podNetwork, error) {

	// todo check whether there is enough ip, if not, return err directly
	logrus.Debugf("return %d ips from logical switch %s", number, logicalSwitch)
	var pns []podNetwork
	for i := 0; i < int(number); i++ {
		var pn podNetwork
		// fake port Name, will deleted when pod create
		portName := fmt.Sprintf("%s-%s-%d", deployment.Name, "fakeport", i)
		// firsth add lsp add logical switch with deloyment-i
		out, stderr, err := util.RunOVNNbctl("--may-exist", "lsp-add",
			logicalSwitch, portName, "--", "lsp-set-addresses",
			portName, "dynamic", "--", "set",
			"logical_switch_port", portName,
			"external-ids:namespace="+deployment.Namespace,
			"external-ids:logical_switch="+logicalSwitch,
			"external-ids:pod=true", "external_ids:deployment="+deployment.Name)
		//"--", "--if-exists",
		//"clear", "logical_switch_port", portName, "dynamic_addresses")
		if err != nil {
			logrus.Errorf("Failed to add logical port to switch "+
				"stdout: %q, stderr: %q (%v)",
				out, stderr, err)
			return nil, err
		}

		out, stderr, err = util.RunOVNNbctl("get",
			"logical_switch_port", portName, "dynamic_addresses")
		logrus.Debugf("out-%s", out)
		//if err == nil && out != "[]" {
		//	break
		//}
		if err != nil {
			logrus.Errorf("Error while obtaining addresses for %s - %v", portName,
				err)
			return nil, err
		}

		// dynamic addresses have format "0a:00:00:00:00:01 192.168.1.3".
		outStr := strings.Trim(out, `"`)
		addresses := strings.Split(outStr, " ")
		if len(addresses) != 2 {
			logrus.Errorf("Error while obtaining addresses for %s", portName)
			return nil, fmt.Errorf("Error while obtaining addresses for %s", portName)
		}

		ip := net.ParseIP(addresses[1])
		mac, err := net.ParseMAC(addresses[0])
		if err != nil {
			return nil, err
		}
		pn.IP = ip
		pn.MAC = mac
		pns = append(pns, pn)
	}
	if len(pns) != int(number) {
		// todo should release lsp created before
		return nil, fmt.Errorf("cannot release enough ip and mac for deployment %s", deployment.Name)
	}
	return pns, nil
}

// UpdateDeployment handle when deployment update
func (oc *Controller) UpdateDeployment(old, new *kapps.Deployment) error {
	if old.Name != new.Name || old.Namespace != new.Namespace {
		logrus.Debugf("deployment name or namespace changed when update")
		return nil
	}
	_, isOldStaticIP := old.Annotations[values.DeploymentStaticIPSwitch]
	_, isNewStaticIP := new.Annotations[values.DeploymentStaticIPSwitch]
	// only both old and new deployment open the ip static switch, we do the operation
	if isOldStaticIP && isNewStaticIP {
		oldReplicas := int(*old.Spec.Replicas)
		newReplicas := int(*new.Spec.Replicas)
		if oldReplicas == newReplicas {
			// Check whether need to get new ips from ls

			_, ipStaticPool := new.Annotations[values.IPPoolStatic]
			if !ipStaticPool {
				// should add annotations to deployment with ipPool
				err := oc.addAnnotationsToDeployment(new)
				if err != nil {
					return err
				}
			}
			// the replicas doesn't changed, do nothing
			return nil
		} else if newReplicas < oldReplicas {
			// when replicas change smaller, do nothing
			// the lsp will be deleted when pod deleted
			// the ip info in deployment will update in syncDeployments
			return nil
		} else {
			if _, ipStaticPool := new.Annotations[values.IPPoolStatic]; !ipStaticPool {
				err := oc.addAnnotationsToDeployment(new)
				if err != nil {
					return err
				}
				logrus.Debugf("success add annotations on deploy:%s", new.Name)
				return nil
			}
			// when replicas change bigger, we should release more ip from logical switch
			number := newReplicas - oldReplicas
			pns, err := oc.getIPsFromLogicalSwitch(new, new.Namespace, number)
			if err != nil {
				return err
			}
			ipPoolOld := new.Annotations[values.IPPoolStatic]
			ipPoolNew := ipPoolOld + "," + podNetworkString(pns)
			// set annotations to deployment
			err = oc.kube.SetAnnotationOnDeployment(new, values.IPPoolStatic, ipPoolNew)
			if err != nil {
				return err
			}
			logrus.Debugf("success add annotations:%s on deploy:%s", ipPoolNew, new.Name)
		}
	}
	return nil

}

func (oc *Controller) DeleteDeployment(deployment *kapps.Deployment) {
	// just no need to do something
	// the lsp will deleted when pod deleted

}

type podNetwork struct {
	IP   net.IP
	Mask string
	MAC  net.HardwareAddr
}

// "10.10.10.10 33:33:33:33:33:33, 10.10.10.12 33:44:44:44:44:44"
func podNetworkString(pns []podNetwork) string {
	var value string
	for _, pn := range pns {
		value = value + pn.IP.String() + "/" + pn.Mask + " " + pn.MAC.String() + ","
	}
	return strings.TrimRight(value, ",")
}

func IpPoolToPodSubnet(ipPool string) ([]podNetwork, error) {
	if ipPool == "" {
		return nil, fmt.Errorf("get ipPool info is empty")
	}
	ipAndMac := strings.Split(ipPool, ",")
	var pns []podNetwork
	for _, item := range ipAndMac {
		var pn podNetwork
		s := strings.Split(item, " ")
		// convert 172.27.0.2/16 to [172.27.0.2 172.27.0.0/16 nil]
		ip, ipNet, err := net.ParseCIDR(s[0])
		if err != nil {
			return nil, err
		}
		mac, _ := net.ParseMAC(s[1])
		pn.IP = ip
		pn.Mask = strings.Split(ipNet.String(), "/")[1]
		pn.MAC = mac
		pns = append(pns, pn)

	}
	logrus.Debugf("convert ipPool %s to podNetwork successfully", ipPool)
	return pns, nil
}

func (oc *Controller) getPodsFromNamespacesDeployment(namespace, deploy string) ([]kapi.Pod, error) {
	podList, err := oc.kube.GetPods(namespace)
	if err != nil {
		return nil, err
	}
	var pods []kapi.Pod
	for _, pod := range podList.Items {
		if strings.HasPrefix(pod.Name, deploy) {
			pods = append(pods, pod)
		}
	}
	return pods, nil

}

func (oc *Controller) addAnnotationsToDeployment(deployment *kapps.Deployment) error {
	number := int(*deployment.Spec.Replicas)
	if number == 0 {
		return fmt.Errorf("replicas is zero, do nothing")
	} else {
		// release ip from logical switch
		logicalSwitch := deployment.Namespace
		// get ip from logical switch's subnet
		pns, err := oc.getIPsFromLogicalSwitch(deployment, logicalSwitch, number)
		if err != nil {
			return err
		}

		// set annotations to deployment
		err = oc.kube.SetAnnotationOnDeployment(deployment, values.IPPoolStatic, podNetworkString(pns))
		if err != nil {
			return err
		}
	}

	return nil
}
