
package scacs

import akka.actor.Actor

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
 *
 * @param addresses a list of addresses of the remote nodes
 */
case class Nodes(addresses: List[(String, Int)])

/**
 * Message type used for starting a new actor on a remote node.
 *
 * @param clazz the class of the actor to be started
 */
case class StartActorAt(host: String, port: Int, clazz: Class[_ <: Actor])
