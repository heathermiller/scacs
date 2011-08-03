package scacs

import akka.actor.{Actor, ActorRef}
import Actor._
import cluster._
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
      val nodeRef = remoteService.actorFor(
        classOf[ClusterService].getCanonicalName,
        newHost,
        newPort)

      nodeRefs += ((newHost, newPort) -> nodeRef)

      if (nodeRefs.size == numNodes) {
        println("[Master] all nodes have registered")
        nodeRefs.values foreach { service =>
          (service ? Nodes(nodeRefs.keys.toList)).await
        }
        MasterService.doneInit.countDown()
      }

    case startMsg @ StartActorAt(host, port, clazz) =>
      (nodeRefs((host, port)) ? startMsg).await
      val startedActor = remoteService.actorFor(
        clazz.getCanonicalName,
        host,
        port)
      self.reply(startedActor)

    case stopMsg @ StopServiceAt(host, port) =>
      (nodeRefs((host, port)) ? stopMsg).await
      self.reply()

    case _ =>
      println("[Master] unknown message")
  }
}

object MasterService {
  val doneInit = new CountDownLatch(1)

  def main(args: Array[String]) = {
    val hostname = args(0)
    val port = args(1).toInt
    val numNodes = args(2).toInt
    
    remoteService.start(hostname, port)
    
    val master = actorOf[MasterService].start()
    remoteService.register(master)
    
    (master ? ClusterSize(numNodes)).await
    doneInit.await()

    // remotely start EchoActor
    val response =
      (master ? StartActorAt("localhost", 9001, classOf[EchoActor])).await
    val echoActor = response.get.asInstanceOf[ActorRef]
    println((echoActor ? "hello").await)

    (master ? StopServiceAt("localhost", 9001)).await

    println("[Master] shutting down")
    remoteService.shutdown()
  }

}
