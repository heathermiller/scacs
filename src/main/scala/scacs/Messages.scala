
package scacs

// used to tell the master how many nodes we expect to intitialize
case class ClusterSize(numNodes: Int)

// used by the ClusterService actor on each node announce to the MasterService actor
// ClusterService's hostname/port.
case class Announce(hostname: String, port: Int)

