
package scacs

import akka.actor.{Actor, ActorRef}
import Actor._
import java.util.concurrent.CountDownLatch

class MasterService extends Actor {
  var numNodes = 0
  var nodeRefs: Map[(String, Int), ActorRef] = Map()

  def getNode(host: String, port: Int): ActorRef =
    nodeRefs.get((host, port)) match {
      case None =>
        MasterService.terminate.countDown()
        sys.error("[MasterService] node "+host+":"+port+" not registered")
      case Some(nodeRef) =>
        nodeRef
    }
  
  def receive = {
    case ClusterSize(num) =>
      numNodes = num
      println("[MasterService] waiting for "+numNodes+" nodes to register")
      self.reply()

    case Announce(newHost, newPort) =>
      println("[MasterService] new host "+newHost+":"+newPort)
      val nodeRef = remote.actorFor(
        classOf[ClusterService].getCanonicalName,
        newHost,
        newPort)
      nodeRefs += ((newHost, newPort) -> nodeRef)
      
      if (nodeRefs.size == numNodes) {
        println("[MasterService] all nodes have registered")
        nodeRefs.values foreach { service => service !! Nodes(nodeRefs.keys.toList) }
        MasterService.doneInit.countDown()
      }
    
    case msg @ SubmitAt(host, port, fun, input, tracking) =>
      println("[MasterService] sending SubmitAt to "+host+":"+port)
      val nodeRef = getNode(host, port)
      nodeRef ! msg
    
    case msg @ RetrieveFrom(host, port, tracking) =>
      val nodeRef = getNode(host, port)
      self.reply((nodeRef !! msg).get)
    
    case _ =>
      println("[MasterService] unknown message")
  }
  
  // whenever MasterService actor terminates, the whole remote service should shut down.
  override def postStop() = MasterService.terminate.countDown()
}

object MasterService {
  val terminate = new CountDownLatch(1)
  val doneInit = new CountDownLatch(1)
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

    doneInit.await()

    val fun = { (cs: ClusterService, list: Any) =>
      list.asInstanceOf[List[Int]].map { x => println(x); x + 1 }
    }
    val tn = newTrackingNumber
    master ! SubmitAt("localhost", 9001, fun, List(1, 2, 3), Some(tn))
    val result = master !! RetrieveFrom("localhost", 9001, tn)
    println(result)

    terminate.countDown()
    Thread.sleep(10000)
    terminate.await()

    println("[EXIT: MasterService] Shutting down.")
    remote.shutdown()
    println("[EXIT: MasterService] Done.")
  }

}
