package cluster

import (
	"fmt"
	"github.com/shuangyangqian/k8s-ovn/pkg/util"
	"github.com/shuangyangqian/k8s-ovn/pkg/values"
	"github.com/sirupsen/logrus"
	kapi "k8s.io/api/core/v1"
	"net"
	"strings"
)

func (c *OvnClusterController) syncNamespaces(namespaces []interface{}) {
	existsNamespaces := make(map[string]*kapi.Namespace)
	for _, tmp := range namespaces {
		namespace, ok := tmp.(*kapi.Namespace)
		if !ok {
			logrus.Errorf("Spurious object in syncNodes: %v", tmp)
			continue
		}
		existsNamespaces[namespace.Name] = namespace
	}

	// We only deal with cleaning up namespaces that shouldn't exist here, since
	// watchNamespaces() will be called for all existing namespaces at startup anyway
	namespaceSwitches, stderr, err := util.RunOVNNbctl("--data=bare", "--no-heading",
		"--columns=name,other-config", "find", "logical_switch", "other-config:subnet!=_")
	if err != nil {
		logrus.Errorf("Failed to get node logical switches: stderr: %q, error: %v",
			stderr, err)
		return
	}
	for _, result := range strings.Split(namespaceSwitches, "\n\n") {
		// Split result into name and other-config
		items := strings.Split(result, "\n")
		if len(items) != 2 || len(items[0]) == 0 {
			continue
		}
		if _, ok := existsNamespaces[items[0]]; ok {
			// namespace still exists, no cleanup to do
			continue
		}

		var subnet *net.IPNet
		if strings.HasPrefix(items[1], "subnet=") {
			subnetStr := strings.TrimPrefix(items[1], "subnet=")
			_, subnet, _ = net.ParseCIDR(subnetStr)
		}

		if err := c.deleteNamespace(items[0], subnet); err != nil {
			logrus.Error(err)
		}
	}

}

// addNamespace when a new namespace added, should allocate a subnet to it
func (c *OvnClusterController) addNamespace(namespace *kapi.Namespace) (err error) {

	var namespaceSubnet *net.IPNet
	var subnetAllocator *SubnetAllocator
	namespaceSubnet, subnetAllocator, err = c.ensureNamespaceSubnet(namespace)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil && subnetAllocator != nil {
			_ = subnetAllocator.ReleaseNetwork(namespaceSubnet)
		}
	}()

	// Ensure that the node's logical network has been created
	err = c.ensureNamespaceLogicalNetwork(namespace.Name, namespaceSubnet)
	if err != nil {
		return err
	}

	// update namespace's annotations
	err = c.Kube.SetAnnotationOnNamespace(namespace, values.NamespaceSubnet, namespaceSubnet.String())
	if err != nil {
		logrus.Errorf("Failed to set namespace %s subnet annotation: %v", namespace.Name, namespaceSubnet.String())
		return err
	}

	return nil
}

func (c *OvnClusterController) ensureNamespaceSubnet(namespace *kapi.Namespace) (*net.IPNet, *SubnetAllocator, error) {
	// Do not create a subnet if the node already has a subnet
	subnet, _ := parseNamespaceSubnet(namespace)
	if subnet != nil {
		return subnet, nil, nil
	}

	// Create new subnet
	for _, possibleSubnet := range c.masterSubnetAllocatorList {
		sn, err := possibleSubnet.GetNetwork()
		if err == ErrSubnetAllocatorFull {
			// Current subnet exhausted, check next possible subnet
			continue
		} else if err != nil {
			return nil, nil, fmt.Errorf("Error allocating network for namespace %s: %v", namespace.Name, err)
		}

		// Success
		logrus.Infof("Allocated node %s subnet %s", namespace.Name, sn.String())
		return sn, possibleSubnet, nil
	}
	return nil, nil, fmt.Errorf("error allocating network for node %s: No more allocatable ranges", namespace.Name)
}

func (c *OvnClusterController) ensureNamespaceLogicalNetwork(namespaceName string, subnet *net.IPNet) error {
	// Create a logical switch and set its subnet.
	stdout, stderr, err := util.RunOVNNbctl("--", "--may-exist", "ls-add", namespaceName,
		"--", "set", "logical_switch", namespaceName, "other-config:subnet="+subnet.String(),
		"external-ids:gateway_ip="+c.GatewayIP)
	if err != nil {
		logrus.Errorf("Failed to create a logical switch %v, stdout: %q, stderr: %q, error: %v",
			namespaceName, stdout, stderr, err)
		return err
	}
	return nil
}

// deleteNamespace when a namespace is deleted, should release it's subnet
func (c *OvnClusterController) deleteNamespace(namespaceName string, namespaceSubnet *net.IPNet) error {

	if namespaceSubnet != nil {
		if err := c.deleteNamespaceSubnet(namespaceName, namespaceSubnet); err != nil {
			logrus.Errorf("Error deleting namespace %s HostSubnet: %v", namespaceName, err)
		}
	}

	if err := c.deleteNamespaceLogicalNetwork(namespaceName); err != nil {
		logrus.Errorf("Error deleting namespace %s logical network: %v", namespaceName, err)
	}

	return nil

}

func (cluster *OvnClusterController) deleteNamespaceLogicalNetwork(namespaceName string) error {
	// Remove the logical switch associated with the node
	if _, stderr, err := util.RunOVNNbctl("--if-exist", "ls-del", namespaceName); err != nil {
		return fmt.Errorf("Failed to delete logical switch %s, "+
			"stderr: %q, error: %v", namespaceName, stderr, err)
	}

	return nil
}

func (c *OvnClusterController) deleteNamespaceSubnet(name string, subnet *net.IPNet) error {
	for _, possibleSubnet := range c.masterSubnetAllocatorList {
		if err := possibleSubnet.ReleaseNetwork(subnet); err == nil {
			logrus.Infof("Deleted namespace subnet %v for namespace %s", subnet, name)
			return nil
		}
	}
	// SubnetAllocator.network is an unexported field so the only way to figure out if a subnet is in a network is to try and delete it
	// if deletion succeeds then stop iterating, if the list is exhausted the node subnet wasn't deleteted return err
	return fmt.Errorf("Error deleting subnet %v for namespace %q: subnet not found in any CIDR range or already available", subnet, name)
}

// parseNamespaceSubnet return namespace's subnet
func parseNamespaceSubnet(namespace *kapi.Namespace) (*net.IPNet, error) {
	sub, ok := namespace.Annotations[values.NamespaceSubnet]
	if !ok {
		return nil, fmt.Errorf("Error in obtaining namespace's subnet for namespace %s", namespace.Name)
	}

	_, subnet, err := net.ParseCIDR(sub)
	if err != nil {
		return nil, fmt.Errorf("Error in parsing namespace subnet - %v", err)
	}

	return subnet, nil
}
