package scacs

import akka.actor.{Actor, ActorRef}
import Actor.cluster.remoteService

class EchoActor extends Actor {
  var neighbors: List[ActorRef] = List()
  var allAddresses: List[(String, Int)] = List()

  def receive = {
    case Nodes(addresses) =>
      allAddresses = addresses
      neighbors = addresses map { case (hostname, port) =>
        remoteService.actorFor(classOf[ClusterService].getCanonicalName,
          hostname,
          port)
      }
      self.reply()
    case any =>
      println("[EchoActor] received "+any)
      self.reply(any)
  }
}
