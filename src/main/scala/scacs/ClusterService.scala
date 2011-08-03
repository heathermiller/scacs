package scacs

import akka.actor.{Actor, ActorRef}
import Actor._
import cluster._
import java.util.concurrent.CountDownLatch

class ClusterService extends Actor {
  var allAddresses: List[(String, Int)] = List()
  var master: ActorRef = null
  
  def receive = {
    case Announce(hostname, port) =>
      master = remoteService.actorFor(
        classOf[MasterService].getCanonicalName,
        hostname,
        port)
      val localhost = remoteService.address.getHostName()
      val localport = remoteService.address.getPort()
      master ! Announce(localhost, localport)

    case Nodes(addresses) =>
      println("[ClusterService] received node addresses: "+addresses)
      allAddresses = addresses
      self.reply()

    case StartActorAt(_, _, clazz) =>
      println("[ClusterService] starting instance of "+clazz)
      val newActor = actorOf(clazz).start()
      remoteService.register(newActor)
      (newActor ? Nodes(allAddresses)).await
      self.reply()

    case StopServiceAt(_, _) =>
      println("[ClusterService] shutting down...")
      ClusterService.terminate.countDown()

    case _ =>
      println("[ClusterService] unknown message")
  }

}

object ClusterService {
  val terminate = new CountDownLatch(1)

  def run(masterHostname: String, masterPort: Int, hostname: String, port: Int) {
    remoteService.start(hostname, port)
    
    val service = actorOf[ClusterService].start()
    remoteService.register(service)
    
    service ! Announce(masterHostname, masterPort)
    terminate.await()
    remoteService.shutdown()
  }

  def main(args: Array[String]) {
    val masterHostname = args(0)
    val masterPort = args(1).toInt
    val hostname = args(2)
    val port = args(3).toInt
    run(masterHostname, masterPort, hostname, port)
  }
}
