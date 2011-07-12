/*
 * TODO:
 * 1) start akka remote source on <hostname,port>
 * 2) start a remote actor (ClusterService Actor)
 *   a) allow other actors to be started locally
 *   b) receive info of all neighbors.
 * 3) start custom remote actor
 *   a) needs acces to info of all neighbors
 *      - either local message from ClusterService Actor (what we'll do now)
 *      - or message from Master Actor somehow
 */
/*
 * (Intended) Steps to get it running...
 * 1) start MasterService
 * 2) start ClusterService on each node
 */

package scacs

import akka.actor.Actor
import Actor._

class ClusterService extends Actor{

  def receive = {
    case Announce(hostname, port) => 
      val master = remote.actorFor(classOf[MasterService].getCanonicalName,hostname,port)
      val localhost = remote.address.getHostName()
      val localport = remote.address.getPort()
      master ! Announce(localhost, localport) 
    case Nodes(addresses) =>
      println("[ClusterService] received node addresses: "+addresses)
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
