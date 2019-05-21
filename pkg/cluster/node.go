package cluster

import (
	"github.com/shuangyangqian/k8s-ovn/pkg/cni"
	"github.com/shuangyangqian/k8s-ovn/pkg/config"
	"github.com/shuangyangqian/k8s-ovn/pkg/util"
	"github.com/sirupsen/logrus"
	"os"
	"path/filepath"


	kapi "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
)

// StartClusterNode learns the subnet assigned to it by the master controller
// and calls the SetupNode script which establishes the logical switch
func (cluster *OvnClusterController) StartClusterNode(name string) error {
	// Make sure br-int is created.
	stdout, stderr, err := util.RunOVSVsctl("--", "--may-exist", "add-br", "br-int")
	if err != nil {
		logrus.Errorf("Failed to create br-int, stdout: %q, stderr: %q, error: %v", stdout, stderr, err)
		return err
	}

	// set ovn connection on node's ovs
	err = setupOVNNode(name)
	if err != nil {
		return err
	}

	confFile := filepath.Join(config.CNI.ConfDir, config.CNIConfFileName)
	_, err = os.Stat(confFile)
	if os.IsNotExist(err) {
		err = config.WriteCNIConfig(config.CNI.ConfDir, config.CNIConfFileName)
		if err != nil {
			return err
		}
	}

	// start the cni server
	cniServer := cni.NewCNIServer("")
	err = cniServer.Start(cni.HandleCNIRequest)

	return err
}

// If default namespace MasterOverlayIP annotation has been chaged, update
// config.OvnNorth and config.OvnSouth auth with new ovn-nb and ovn-remote
// IP address
func (cluster *OvnClusterController) updateOvnNode(masterIP string,
	node *kapi.Node, subnet string) error {
	err := config.UpdateOvnNodeAuth(masterIP)
	if err != nil {
		return err
	}
	err = setupOVNNode(node.Name)
	if err != nil {
		logrus.Errorf("Failed to setup OVN node (%v)", err)
		return err
	}

	var clusterSubnets []string

	for _, clusterSubnet := range cluster.ClusterIPNet {
		clusterSubnets = append(clusterSubnets, clusterSubnet.CIDR.String())
	}

	return nil
}

// watchNamespaceUpdate starts watching namespace resources and calls back
// the update handler logic if there is any namspace update event
func (cluster *OvnClusterController) watchNamespaceUpdate(node *kapi.Node,
	subnet string) error {
	_, err := cluster.watchFactory.AddNamespaceHandler(
		cache.ResourceEventHandlerFuncs{
			UpdateFunc: func(old, newer interface{}) {
				oldNs := old.(*kapi.Namespace)
				oldMasterIP := oldNs.Annotations[MasterOverlayIP]
				newNs := newer.(*kapi.Namespace)
				newMasterIP := newNs.Annotations[MasterOverlayIP]
				if newMasterIP != oldMasterIP {
					err := cluster.updateOvnNode(newMasterIP, node, subnet)
					if err != nil {
						logrus.Errorf("Failed to update OVN node with new "+
							"masterIP %s: %v", newMasterIP, err)
					}
				}
			},
		}, nil)
	return err
}
