
package scacs

import akka.actor.Actor

/**
 * Message type used to tell the master how many nodes are expected to be registered.
 *
 * @param numNodes The number of nodes the {{MasterService}} should wait to register.
 */
case class ClusterSize(numNodes: Int)

/**
 * Message type used by the {{ClusterService}} actor on each node to
 * announce itself to the {{MasterService}} actor.
 * 
 * @param hostname  The {{ClusterService}}'s host name
 * @param port      The {{ClusterService}}'s port number
 */
case class Announce(hostname: String, port: Int)

/**
 * Message type used for broadcasting node addresses.
 *
 * @param addresses a list of addresses of the remote nodes
 */
case class Nodes(addresses: List[(String, Int)])

/**
 * Message type used for starting a new actor on a remote node.
 *
 * @param clazz the class of the actor to be started
 */
case class Start(clazz: Class[_ <: Actor])


/**
 * Message type used by {{MasterService}} for submitting a task to a node,
 * when it expects to receive a {{trackingNumber}} for a processed data
 * item in return, rather than the actual data item.
 *
 * @param addresses a list of addresses of the remote nodes
 */
//case class SubmitAt(addresses: List[(String, Int)])

/**
 * Message type used by {{MasterService}} for submitting a task to a node,
 * when it expects to receive directly receive the processed data item in return.
 *
 * @param clazz the class of the actor to be started
 */
//case class InvokeAt(clazz: Class[_ <: Actor])
