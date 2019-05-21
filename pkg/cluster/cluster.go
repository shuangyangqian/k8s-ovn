package cluster

import (
	"fmt"
	"github.com/Sirupsen/logrus"
	"github.com/shuangyangqian/k8s-ovn/pkg/config"
	"github.com/shuangyangqian/k8s-ovn/pkg/factory"
	"github.com/shuangyangqian/k8s-ovn/pkg/kube"
	"github.com/shuangyangqian/k8s-ovn/pkg/util"
	"net"

	"k8s.io/client-go/kubernetes"
)

// OvnClusterController is the object holder for utilities meant for cluster management
type OvnClusterController struct {
	Kube                      kube.Interface
	watchFactory              *factory.WatchFactory
	masterSubnetAllocatorList []*SubnetAllocator

	TCPLoadBalancerUUID string
	UDPLoadBalancerUUID string

	ClusterServicesSubnet string
	ClusterIPNet          []CIDRNetworkEntry

	GatewayIP string
	MTU       string
	Mask      int
}

// CIDRNetworkEntry is the object that holds the definition for a single network CIDR range
type CIDRNetworkEntry struct {
	CIDR *net.IPNet
	// HostSubnetLength means that how many IP on one node
	HostSubnetLength uint32
}

const (
	// OvnHostSubnet is the constant string representing the annotation key
	OvnHostSubnet = "ovn_host_subnet"
	// DefaultNamespace is the name of the default namespace
	DefaultNamespace = "default"
	// MasterOverlayIP is the overlay IP address on master node
	MasterOverlayIP = "master_overlay_ip"
	// OvnClusterRouter is the name of the distributed router
	OvnClusterRouter = "ovn_cluster_router"
)

// NewClusterController creates a new controller for IP subnet allocation to
// a given resource type (either Namespace or Node)
func NewClusterController(kubeClient kubernetes.Interface, wf *factory.WatchFactory) *OvnClusterController {
	return &OvnClusterController{
		Kube:         &kube.Kube{KClient: kubeClient},
		watchFactory: wf,
	}
}

func setupOVNNode(nodeName string) error {
	// Tell ovn-*bctl how to talk to the database
	for _, auth := range []*config.OvnDBAuth{
		config.OvnNorth.ClientAuth,
		config.OvnSouth.ClientAuth} {
		if err := auth.SetDBAuth(); err != nil {
			return err
		}
	}

	var err error
	nodeIP := config.Default.EncapIP
	if nodeIP == "" {
		nodeIP, err = GetNodeIP(nodeName)
		if err != nil {
			return fmt.Errorf("failed to obtain local IP from hostname %q: %v", nodeName, err)
		}
	} else {
		if ip := net.ParseIP(nodeIP); ip == nil {
			return fmt.Errorf("invalid encapsulation IP provided %q", nodeIP)
		}
	}

	_, stderr, err := util.RunOVSVsctl("set",
		"Open_vSwitch",
		".",
		fmt.Sprintf("external_ids:ovn-encap-type=%s", config.Default.EncapType),
		fmt.Sprintf("external_ids:ovn-encap-ip=%s", nodeIP),
		fmt.Sprintf("external_ids:ovn-remote-probe-interval=%d",
			config.Default.InactivityProbe),
	)
	if err != nil {
		return fmt.Errorf("error setting OVS external IDs: %v\n  %q", err, stderr)
	}
	return nil
}

func setupOVNMaster(nodeName string) error {
	// Configure both server and client of OVN databases, since master uses both
	for _, auth := range []*config.OvnDBAuth{
		config.OvnNorth.ClientAuth,
		config.OvnSouth.ClientAuth} {
		if err := auth.SetDBAuth(); err != nil {
			return err
		}
	}
	return nil
}
func GetNodeIP(nodeName string) (string, error) {
	ip := net.ParseIP(nodeName)
	if ip == nil {
		addrs, err := net.LookupIP(nodeName)
		if err != nil {
			return "", fmt.Errorf("Failed to lookup IP address for node %s: %v", nodeName, err)
		}
		for _, addr := range addrs {
			// Skip loopback and non IPv4 addrs
			if addr.IsLoopback() || addr.To4() == nil {
				logrus.Debugf("Skipping loopback/non-IPv4 addr: %q for node %s", addr.String(), nodeName)
				continue
			}
			ip = addr
			break
		}
	} else if ip.IsLoopback() || ip.To4() == nil {
		logrus.Debugf("Skipping loopback/non-IPv4 addr: %q for node %s", ip.String(), nodeName)
		ip = nil
	}

	if ip == nil || len(ip.String()) == 0 {
		return "", fmt.Errorf("Failed to obtain IP address from node name: %s", nodeName)
	}
	return ip.String(), nil
}

// ParseCIDRMask parses a CIDR string and ensures that it has no bits set beyond the
// network mask length. Use this when the input is supposed to be either a description of
// a subnet (eg, "192.168.1.0/24", meaning "192.168.1.0 to 192.168.1.255"), or a mask for
// matching against (eg, "192.168.1.15/32", meaning "must match all 32 bits of the address
// "192.168.1.15"). Use net.ParseCIDR() when the input is a host address that also
// describes the subnet that it is on (eg, "192.168.1.15/24", meaning "the address
// 192.168.1.15 on the network 192.168.1.0/24").
func ParseCIDRMask(cidr string) (*net.IPNet, error) {
	ip, net, err := net.ParseCIDR(cidr)
	if err != nil {
		return nil, err
	}
	if !ip.Equal(net.IP) {
		maskLen, addrLen := net.Mask.Size()
		return nil, fmt.Errorf("CIDR network specification %q is not in canonical form (should be %s/%d or %s/%d?)", cidr, ip.Mask(net.Mask).String(), maskLen, ip.String(), addrLen)
	}
	return net, nil
}
