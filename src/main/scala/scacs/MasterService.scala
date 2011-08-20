package scacs

import akka.actor.{Actor, ActorRef}
import Actor._
import java.util.concurrent.CountDownLatch

class MasterService extends Actor {
  var numNodes = 0
  var nodeRefs: Map[(String, Int), ActorRef] = Map()
  
  def receive = {
    case ClusterSize(numNodes) =>
      this.numNodes = numNodes
      println("[Master] waiting for "+numNodes+
              " nodes to register")
      self.reply()

    case Announce(newHost, newPort) =>
      println("[Master] new host "+
        newHost+":"+newPort)
      val nodeRef = remote.actorFor(
        classOf[ClusterService].getCanonicalName,
        newHost,
        newPort)

      nodeRefs += ((newHost, newPort) -> nodeRef)

      if (nodeRefs.size == numNodes) {
        println("[Master] all nodes have registered")
        nodeRefs.values foreach { service =>
          service ? Nodes(nodeRefs.keys.toList)
        }
        MasterService.doneInit.countDown()
      }

    case startMsg @ StartActorAt(host, port, clazz) =>
      nodeRefs((host, port)) ? startMsg
      val startedActor = remote.actorFor(
        clazz.getCanonicalName,
        host,
        port)
      self.reply(startedActor)

    case stopMsg @ StopServiceAt(host, port) =>
      nodeRefs((host, port)) ? stopMsg
      self.reply()

    case _ =>
      println("[Master] unknown message")
  }
}

object MasterService {
  val doneInit = new CountDownLatch(1)
  var master: ActorRef = null

  def main(args: Array[String]) = {
    val hostname = args(0)
    val port = args(1).toInt
    val numNodes = args(2).toInt
    
    remote.start(hostname, port)
    
    master = actorOf[MasterService].start()
    remote.register(master)
    
    master ? ClusterSize(numNodes)
    doneInit.await()
  }

  def shutdown() {
    println("[Master] shutting down")
    registry.shutdownAll()
    remote.shutdown()
  }

}
