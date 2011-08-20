package scacs

import akka.actor.{Actor, ActorRef}
import Actor._
import akka.config.Supervision.OneForOneStrategy
import java.util.concurrent.CountDownLatch
import java.lang.RuntimeException

class ClusterService extends Actor {
  var allAddresses: List[(String, Int)] = List()
  var master: ActorRef = null
  
  self.faultHandler =
    OneForOneStrategy(List(classOf[NumberFormatException],
                           classOf[RuntimeException]), 5, 5000)
  
  def receive = {
    case Announce(hostname, port) =>
      master = remote.actorFor(
        classOf[MasterService].getCanonicalName,
        hostname,
        port)
      val localhost = remote.address.getHostName()
      val localport = remote.address.getPort()
      master ! Announce(localhost, localport)

    case Nodes(addresses) =>
      println("[ClusterService] received node addresses: "+addresses)
      allAddresses = addresses
      self.reply()

    case StartActorAt(_, _, clazz) =>
      println("[ClusterService] creating instance of "+clazz)
      val newActor = actorOf(clazz)
      println("[ClusterService] starting and linking to "+newActor)
      // start actor and link with ClusterService (self)
      self.startLink(newActor)
      remote.register(newActor)
      newActor ? Nodes(allAddresses)
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
    remote.start(hostname, port)
    
    val service = actorOf[ClusterService].start()
    remote.register(service)
    
    service ! Announce(masterHostname, masterPort)
    terminate.await()
    registry.shutdownAll() // also stops service actor
    remote.shutdown()
  }

  def main(args: Array[String]) {
    val masterHostname = args(0)
    val masterPort = args(1).toInt
    val hostname = args(2)
    val port = args(3).toInt
    run(masterHostname, masterPort, hostname, port)
  }
}
