
package scacs

import akka.actor.{Actor, ActorRef}
import Actor._
import scala.collection.mutable.HashMap
import java.util.concurrent.CountDownLatch

class MasterService extends Actor {
  var numNodes = 0
  var addresses: List[(String, Int)] = List()
  var nodeRefs: List[ActorRef] = List()
  
  def receive = {

    case ClusterSize(num) =>
      numNodes = num
      println("[MasterService] waiting for "+
        numNodes+" nodes to register")
      self.reply()

    case Announce(newHost, newPort) =>
      println("[MasterService] new host "+
        newHost+":"+newPort)
      addresses ::= (newHost, newPort)
      if (addresses.length == numNodes) {
        println("[MasterService] all nodes have registered")
        nodeRefs = addresses map { case (hostname, port) =>
          remote.actorFor(classOf[ClusterService].getCanonicalName,
            hostname,
            port)
        }
        nodeRefs foreach { service => service !! Nodes(addresses) }
      }

    case _ =>
      println("[MasterService] unknown message")
  }

  // whenever MasterService actor terminates, the whole remote service should shut down.
  override def postStop() = MasterService.terminate.countDown()
}

object MasterService {
  val terminate = new CountDownLatch(1)
  var TrNumIncrementer = 0

  def newTrackingNumber = {
    TrNumIncrementer += 1
    TrNumIncrementer
  }

  def main(args: Array[String]) = {
    val hostname = args(0)
    val port = args(1).toInt
    val numNodes = args(2).toInt
    
    remote.start(hostname,port)
    remote.register(actorOf[MasterService])

    val master = remote.actorFor(classOf[MasterService].getCanonicalName, hostname, port)
    master !! ClusterSize(numNodes)

    terminate.await()

    println("[EXIT: MasterService] Shutting down.")
    remote.shutdown()
    println("[EXIT: MasterService] Done.")
  }

}
