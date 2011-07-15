
package scacs

import akka.actor.{Actor, ActorRef}
import Actor._
import java.util.concurrent.CountDownLatch

class MasterService extends Actor {
  var numNodes = 0
  var nodeRefs: Map[(String, Int), ActorRef] = Map()
  
  def receive = {
    case ClusterSize(num) =>
      numNodes = num
      println("[MasterService] waiting for "+
        numNodes+" nodes to register")
      self.reply()

    case Announce(newHost, newPort) =>
      println("[MasterService] new host "+
        newHost+":"+newPort)
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

    case startMsg @ StartActorAt(host, port, clazz) =>
      nodeRefs((host, port)) !! startMsg
      val startedActor = remote.actorFor(
        clazz.getCanonicalName,
        host,
        port)
      self.reply(startedActor)

    case stopMsg @ StopServiceAt(host, port) =>
      nodeRefs((host, port)) !! stopMsg
      self.reply()

    case _ =>
      println("[MasterService] unknown message")
  }

  // whenever MasterService actor terminates, the whole remote service should shut down.
  override def postStop() = MasterService.terminate.countDown()
}

object MasterService {
  val terminate = new CountDownLatch(1)
  val doneInit = new CountDownLatch(1)

  def main(args: Array[String]) = {
    val hostname = args(0)
    val port = args(1).toInt
    val numNodes = args(2).toInt
    
    remote.start(hostname, port)
    remote.register(actorOf[MasterService])

    val master = remote.actorFor(classOf[MasterService].getCanonicalName, hostname, port)
    master !! ClusterSize(numNodes)
    doneInit.await()

    // remotely start EchoActor
    val response =
      master !! StartActorAt("localhost", 9001, classOf[EchoActor])
    val echoActor = response.get.asInstanceOf[ActorRef]
    println(echoActor !! "hello")

    master !! StopServiceAt("localhost", 9001)

    //terminate.await()
    println("[EXIT: MasterService] Shutting down.")
    registry.shutdownAll()
    remote.shutdown()
    println("[EXIT: MasterService] Done.")
  }

}
