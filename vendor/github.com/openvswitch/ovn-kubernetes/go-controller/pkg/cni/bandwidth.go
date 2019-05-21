package cni

import (
	"fmt"
)

func clearPodBandwidth(sandboxID string) error {
	// interfaces will have the same name as ports
	portList, err := ovsFind("interface", "name", "external-ids:sandbox="+sandboxID)
	if err != nil {
		return err
	}

	// Clear the QoS for any ports of this sandbox
	for _, port := range portList {
		if err = ovsClear("port", port, "qos"); err != nil {
			return err
		}
	}

	// Now that the QoS is unused remove it
	qosList, err := ovsFind("qos", "_uuid", "external-ids:sandbox="+sandboxID)
	if err != nil {
		return err
	}
	for _, qos := range qosList {
		if err := ovsDestroy("qos", qos); err != nil {
			return err
		}
	}

	return nil
}

func setPodBandwidth(sandboxID, ifname string, ingressBPS, egressBPS int64) error {
	// note pod ingress == OVS egress and vice versa

	if ingressBPS > 0 {
		qos, err := ovsCreate("qos", "type=linux-htb", fmt.Sprintf("other-config:max-rate=%d", ingressBPS), "external-ids=sandbox="+sandboxID)
		if err != nil {
			return err
		}
		err = ovsSet("port", ifname, fmt.Sprintf("qos=%s", qos))
		if err != nil {
			return err
		}
	}
	if egressBPS > 0 {
		// ingress_policing_rate is in Kbps
		err := ovsSet("interface", ifname, fmt.Sprintf("ingress_policing_rate=%d", egressBPS/1000))
		if err != nil {
			return err
		}
	}

	return nil
}
