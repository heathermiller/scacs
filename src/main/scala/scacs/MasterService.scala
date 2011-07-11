
package scacs

import akka.actor.Actor
import Actor._
import java.util.concurrent.CountDownLatch

class MasterService extends Actor {
  var count = 0
  var numNodes = 0

  def receive = { //placeholder
    case ClusterSize(num) => 
      numNodes = num
      println("[MasterService] the master should wait for "+numNodes+" nodes to register.")
      self.reply()
    case Announce(hostname, port) =>
      println("[MasterService] received announce message for host: "+hostname+" and port: "+port)
      count += 1
      if (count ==  numNodes) self.stop()
    case _      => println("[MasterService] received an unknown message.")
  }

  // whenever MasterService actor terminates, the whole remote service should shut down.
  override def postStop() = MasterService.terminate.countDown()
}

object MasterService {

  val terminate = new CountDownLatch(1)

  def main(args: Array[String]) = {
    
    val hostname = args(0)
    val port = args(1).toInt
    val numNodes = args(2).toInt
    
    remote.start(hostname,port)
    remote.register(actorOf[MasterService])

    val master = Actor.remote.actorFor(classOf[MasterService].getCanonicalName,hostname,port)
    master !! ClusterSize(numNodes)

    terminate.await()

    println("[EXIT: MasterService] Shutting down.")
    remote.shutdown()
    registry.shutdownAll()
    println("[EXIT: MasterService] Done.")
  }

}
