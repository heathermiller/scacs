package scacs

import akka.actor.Actor

class EchoActor extends Actor {

  def receive = {
    case any =>
      println("[EchoActor] received "+any)
      self.reply(any)
  }

}
