
package scacs

/**
 * Used to tell the master how many nodes we expect to register.
 */
case class ClusterSize(numNodes: Int)

/**
 * Used by the {{ClusterService}} actor on each node to announce itself
 * to the {{MasterService}} actor.
 * 
 * @param hostname  The {{ClusterService}}'s host name
 * @param port      The {{ClusterService}}'s port number
 */
case class Announce(hostname: String, port: Int)

/**
 * Message type used for broadcasting node addresses.
 */
case class Nodes(addresses: List[(String, Int)])
