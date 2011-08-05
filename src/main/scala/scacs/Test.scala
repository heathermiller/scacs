package scacs
package test

import akka.actor.ActorRef
import MasterService._

object Test {
  def main(args: Array[String]) {
    // initialize MasterService
    MasterService.main(args)
    
    // remotely start EchoActor
    val response =
      master !! StartActorAt("localhost", 9001, classOf[EchoActor])
    val echoActor = response.get.asInstanceOf[ActorRef]
    // this will lead to an exception in echoActor
    echoActor ! "hello"
    // try again; echoActor is restarted automatically
    echoActor ! "17"
    
    Thread.sleep(5000)
    master !! StopServiceAt("localhost", 9001)
    shutdown()
  }
}
