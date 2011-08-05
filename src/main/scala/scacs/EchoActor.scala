package scacs

import akka.actor.{Actor, ActorRef}
import Actor.remote
import akka.config.Supervision.Permanent

class EchoActor extends Actor {
  var neighbors: List[ActorRef] = List()
  var allAddresses: List[(String, Int)] = List()
  var sum = 0

  self.lifeCycle = Permanent

  def receive = {
    case Nodes(addresses) =>
      allAddresses = addresses
      neighbors = addresses map { case (hostname, port) =>
        remote.actorFor(classOf[ClusterService].getCanonicalName,
          hostname,
          port)
      }
      self.reply()

    case any: String =>
      println("[EchoActor] received "+any)
      // try converting to an Int
      sum += any.toInt
      println("[EchoActor] current sum: "+sum)
  }

  override def preRestart(reason: Throwable) {
    println("[EchoActor] before restart because of "+reason)
  }

}
