package cluster

import (
	"encoding/binary"
	"fmt"
	"github.com/shuangyangqian/k8s-ovn/pkg/values"
	kapi "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
	"net"
	"sync"

	"github.com/sirupsen/logrus"
)

type SubnetAllocator struct {
	network    *net.IPNet
	hostBits   uint32
	leftShift  uint32
	leftMask   uint32
	rightShift uint32
	rightMask  uint32
	next       uint32
	allocMap   map[string]bool
	mutex      sync.Mutex
}

// StartClusterMaster runs a subnet IPAM and a controller that watches arrival/departure
// of namespaces in the cluster
// On an addition to the cluster (namespace create), a new subnet is created for it that will translate
// to creation of a logical switch (done by the namespace, but could be created here at the master process too)
// Upon deletion of a namespace, the switch will be deleted
//
// TODO: Verify that the cluster was not already called with a different global subnet
//  If true, then either quit or perform a complete reconfiguration of the cluster (recreate switches/routers with new subnet values)
func (cluster *OvnClusterController) StartClusterMaster(masterNodeName string) error {

	masterSubnetAllocatorList := make([]*SubnetAllocator, 0)
	for _, clusterEntry := range cluster.ClusterIPNet {
		subrange := make([]string, 0)

		namespaces, err := cluster.Kube.GetNamespaces()
		if err != nil {
			return err
		}
		for _, item := range namespaces.Items {
			if item.Annotations[values.NamespaceSubnet] != "" {
				subrange = append(subrange, item.Annotations[values.NamespaceSubnet])
			}
		}
		subnetAllocator, err := NewSubnetAllocator(clusterEntry.CIDR.String(), 32-clusterEntry.HostSubnetLength, subrange)
		if err != nil {
			return err
		}
		masterSubnetAllocatorList = append(masterSubnetAllocatorList, subnetAllocator)
	}
	cluster.masterSubnetAllocatorList = masterSubnetAllocatorList

	return cluster.watchNamespaces()
}

func (cluster *OvnClusterController) watchNamespaces() error {
	_, err := cluster.watchFactory.AddNamespaceHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			namespace := obj.(*kapi.Namespace)
			logrus.Debugf("Added event for Namespace %q", namespace.Name)
			err := cluster.addNamespace(namespace)
			if err != nil {
				logrus.Errorf("error creating subnet for namespace %s: %v", namespace.Name, err)
			}
		},
		UpdateFunc: func(old, new interface{}) {},
		DeleteFunc: func(obj interface{}) {
			namespace := obj.(*kapi.Namespace)
			logrus.Debugf("Delete event for namespace %q", namespace.Name)
			namespaceSubnet, _ := parseNamespaceSubnet(namespace)
			err := cluster.deleteNamespace(namespace.Name, namespaceSubnet)
			if err != nil {
				logrus.Error(err)
			}
		},
	}, cluster.syncNamespaces)
	return err

}

var ErrSubnetAllocatorFull = fmt.Errorf("No subnets available.")

func NewSubnetAllocator(network string, hostBits uint32, inUse []string) (*SubnetAllocator, error) {
	_, netIP, err := net.ParseCIDR(network)
	if err != nil {
		return nil, fmt.Errorf("Failed to parse network address: %q", network)
	}

	netMaskSize, _ := netIP.Mask.Size()
	if hostBits == 0 {
		return nil, fmt.Errorf("Host capacity cannot be zero.")
	} else if hostBits > (32 - uint32(netMaskSize)) {
		return nil, fmt.Errorf("Subnet capacity cannot be larger than number of networks available.")
	}
	subnetBits := 32 - uint32(netMaskSize) - hostBits

	// In the simple case, the subnet part of the 32-bit IP address is just the subnet
	// number shifted hostBits to the left. However, if hostBits isn't a multiple of
	// 8, then it can be difficult to distinguish the subnet part and the host part
	// visually. (Eg, given network="10.1.0.0/16" and hostBits=6, then "10.1.0.50" and
	// "10.1.0.70" are on different networks.)
	//
	// To try to avoid this confusion, if the subnet extends into the next higher
	// octet, we rotate the bits of the subnet number so that we use the subnets with
	// all 0s in the shared octet first. So again given network="10.1.0.0/16",
	// hostBits=6, we first allocate 10.1.0.0/26, 10.1.1.0/26, etc, through
	// 10.1.255.0/26 (just like we would with /24s in the hostBits=8 case), and only
	// if we use up all of those subnets do we start allocating 10.1.0.64/26,
	// 10.1.1.64/26, etc.
	var leftShift, rightShift uint32
	var leftMask, rightMask uint32
	if hostBits%8 != 0 && ((hostBits-1)/8 != (hostBits+subnetBits-1)/8) {
		leftShift = 8 - (hostBits % 8)
		leftMask = uint32(1)<<(32-uint32(netMaskSize)) - 1
		rightShift = subnetBits - leftShift
		rightMask = (uint32(1)<<leftShift - 1) << hostBits
	} else {
		leftShift = 0
		leftMask = 0xFFFFFFFF
		rightShift = 0
		rightMask = 0
	}

	amap := make(map[string]bool)
	for _, netStr := range inUse {
		_, nIp, err := net.ParseCIDR(netStr)
		if err != nil {
			fmt.Println("Failed to parse network address: ", netStr)
			continue
		}
		if !netIP.Contains(nIp.IP) {
			fmt.Println("Provided subnet doesn't belong to network: ", nIp)
			continue
		}
		amap[nIp.String()] = true
	}
	return &SubnetAllocator{
		network:    netIP,
		hostBits:   hostBits,
		leftShift:  leftShift,
		leftMask:   leftMask,
		rightShift: rightShift,
		rightMask:  rightMask,
		next:       0,
		allocMap:   amap,
	}, nil
}

func (sna *SubnetAllocator) GetNetwork() (*net.IPNet, error) {
	var (
		numSubnets    uint32
		numSubnetBits uint32
	)
	sna.mutex.Lock()
	defer sna.mutex.Unlock()

	baseipu := IPToUint32(sna.network.IP)
	netMaskSize, _ := sna.network.Mask.Size()
	numSubnetBits = 32 - uint32(netMaskSize) - sna.hostBits
	numSubnets = 1 << numSubnetBits

	var i uint32
	for i = 0; i < numSubnets; i++ {
		n := (i + sna.next) % numSubnets
		shifted := n << sna.hostBits
		ipu := baseipu | ((shifted << sna.leftShift) & sna.leftMask) | ((shifted >> sna.rightShift) & sna.rightMask)
		genIp := Uint32ToIP(ipu)
		genSubnet := &net.IPNet{IP: genIp, Mask: net.CIDRMask(int(numSubnetBits)+netMaskSize, 32)}
		if !sna.allocMap[genSubnet.String()] {
			sna.allocMap[genSubnet.String()] = true
			sna.next = n + 1
			return genSubnet, nil
		}
	}

	sna.next = 0
	return nil, ErrSubnetAllocatorFull
}

func IPToUint32(ip net.IP) uint32 {
	return binary.BigEndian.Uint32(ip.To4())
}

func Uint32ToIP(u uint32) net.IP {
	ip := make([]byte, 4)
	binary.BigEndian.PutUint32(ip, u)
	return net.IPv4(ip[0], ip[1], ip[2], ip[3])
}

func (sna *SubnetAllocator) ReleaseNetwork(ipnet *net.IPNet) error {
	sna.mutex.Lock()
	defer sna.mutex.Unlock()
	if !sna.network.Contains(ipnet.IP) {
		return fmt.Errorf("Provided subnet %v doesn't belong to the network %v.", ipnet, sna.network)
	}

	ipnetStr := ipnet.String()
	if !sna.allocMap[ipnetStr] {
		return fmt.Errorf("Provided subnet %v is already available.", ipnet)
	}

	sna.allocMap[ipnetStr] = false

	return nil
}
