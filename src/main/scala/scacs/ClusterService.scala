/*
 * (Intended) Steps to get it running...
 * 1) start MasterService
 * 2) start ClusterService on each node
 *
 * From here, we have to decide... 
 * Either Delite invokes methods on the MasterService object, or it
 * extends a trait version of it.
 */


package scacs
import akka.actor.{Actor, ActorRef}
import Actor._
import scala.collection.mutable.HashMap

class ClusterService extends Actor{
  var allAddresses: List[(String, Int)] = List()
  var data = new HashMap[Int, Any]
  var master: ActorRef = null

  def receive = {
    case Announce(hostname, port) => 
      master = remote.actorFor(classOf[MasterService].getCanonicalName,hostname,port)
      val localhost = remote.address.getHostName()
      val localport = remote.address.getPort()
      master ! Announce(localhost, localport) 

    case Nodes(addresses) =>
      println("[ClusterService] received node addresses: "+addresses)
      allAddresses = addresses
      self.reply()

    case Start(clazz) =>
      println("[ClusterService] starting instance of "+clazz)
      val newActor = actorOf(clazz).start()
      remote.register(newActor)
      newActor !! Nodes(allAddresses)
      self.reply()

    case SubmitAt(_, _, block, input, trackingNumber) =>
      val result = block(this, input)
      data += (trackingNumber.get -> result)

    case InvokeAt(_, _, block, input, trackingNumber) =>
      val result = block(this, input)
      if (!trackingNumber.isEmpty) data += (trackingNumber.get -> result)
      self.reply(result)

    case _ =>
      println("[ClusterService] unknown message")
  }

}

object ClusterService {
  
  def run(masterHostname: String, masterPort: Int, hostname: String, port: Int) {
    
    remote.start(hostname,port)
    remote.register(actorOf[ClusterService])
    val localMaster = remote.actorFor(classOf[ClusterService].getCanonicalName,hostname,port)    

    localMaster ! Announce(masterHostname, masterPort)
  }

  def main(args: Array[String]) {
    val masterHostname = args(0)
    val masterPort = args(1).toInt
    val hostname = args(2)
    val port = args(3).toInt
    run(masterHostname, masterPort, hostname, port)
  }
}
