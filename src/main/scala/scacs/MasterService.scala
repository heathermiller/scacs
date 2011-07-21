
package scacs

import akka.actor.{Actor, ActorRef, Channel}
import Actor._
import java.util.concurrent.CountDownLatch

class MasterService extends Actor {
  var numNodes = 0
  var nodeRefs: Map[(String, Int), ActorRef] = Map()
  var results: Map[Int, Any] = Map()
  var channels: Map[Int,Channel[Any]] = Map()

  def getNode(host: String, port: Int): ActorRef =
    nodeRefs.get((host, port)) match {
      case None =>
        MasterService.terminate.countDown()
        sys.error("[MasterService] node "+host+":"+port+" not registered")
      case Some(nodeRef) =>
        nodeRef
    }
  
  def receive = {
    case ClusterSize(num) =>
      numNodes = num
      println("[MasterService] waiting for "+numNodes+" nodes to register")
      self.reply()

    case Announce(newHost, newPort) =>
      println("[MasterService] new host "+newHost+":"+newPort)
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
    
    case msg @ SubmitAt(host, port, fun, input, tracking) =>
      println("[MasterService] sending SubmitAt to "+host+":"+port)
      val nodeRef = getNode(host, port)
      nodeRef ! msg

    case msg @ InvokeAt(host, port, fun, input, tracking) =>
      println("[MasterService] sending InvokeAt to "+host+":"+port)
      val nodeRef = getNode(host, port)
      nodeRef ! msg

    case (trackingNumber: Int, result: Any) =>
      if ( channels.contains(trackingNumber) ) {
        val channel = channels(trackingNumber)
        channel ! (trackingNumber, result)
        channels -= trackingNumber
      }
      else
        results += (trackingNumber -> result)  
    
    case RetrieveFrom("", _, tracking) =>
      if ( results.contains(tracking) )
        self.reply(results(tracking))
      else 
        channels += (tracking -> self.channel) // we're queueing up the request

    case msg @ RetrieveFrom(host, port, tracking) if host != "" =>
      val nodeRef = getNode(host, port)
      self.reply((nodeRef !! msg).get)

    case Shutdown =>
      nodeRefs.values foreach { service => service ! Shutdown }
      self.reply()
  }
  
  // whenever MasterService actor terminates, the whole remote service should shut down.
  override def postStop() = MasterService.terminate.countDown()
}

object MasterService {
  val terminate = new CountDownLatch(1)
  val doneInit = new CountDownLatch(1)
  var TrNumIncrementer = 0
  var master: ActorRef = null

  def newTrackingNumber = {
    TrNumIncrementer += 1
    TrNumIncrementer
  }

  def config(hostname: String, port: Int, numNodes: Int) = {
    remote.start(hostname,port)
    remote.register(actorOf[MasterService])   

    master = remote.actorFor(classOf[MasterService].getCanonicalName, hostname, port)
    master !! ClusterSize(numNodes)

    doneInit.await
  }
  
  def shutdown() = {
    master !! Shutdown
    remote.shutdown()
    registry.shutdownAll()
    println("[MasterService] EXIT. Shutting down.")
  }

  def submitAt[T](nodes: List[(String,Int)], partitionedData: List[T], fun: T=>Any): List[Int] = {
    var tns = List[Int]()
    if (nodes.length == partitionedData.length) {
      for ((node,data) <- nodes zip partitionedData) {
        val (hostname, port) = node
        val internalFun = (cs: ClusterService, data: Any) => fun(data.asInstanceOf[T])
        val tn = newTrackingNumber
        tns = tn :: tns
        master ! SubmitAt(hostname, port, internalFun, data, Some(tn))
      }
      tns
    } else 
      sys.error("[ERROR: submitAt] The number of nodes you'd like to submit a task to and the number of pieces of partitioned must be equal.")
  }

  def invokeAt[T](nodes: List[(String,Int)], partitionedData: List[T], fun: T=>Any): List[Any] = {
    var tns = List[Int]()
    if (nodes.length == partitionedData.length) {
      for ((node,data) <- nodes zip partitionedData) {
        val (hostname, port) = node
        val internalFun = (cs: ClusterService, data: Any) => fun(data.asInstanceOf[T])
        val tn = newTrackingNumber
        tns = tn :: tns
        master ! InvokeAt(hostname, port, internalFun, data, Some(tn))
      }
      for (tn <- tns) yield {
        val res = master !! RetrieveFrom("", 0, tn)
        res.get
      }
    } else 
      sys.error("[ERROR: submitAt] The number of nodes you'd like to submit a task to and the number of pieces of partitioned must be equal.")
  }

  //TODO: it would be nice to structure this such that T could be inferred
  def retrieveFrom[T](node: (String, Int), trackingNumber: Int) = {
    master !! RetrieveFrom(node._1, node._2, trackingNumber) match {
      case Some(result) => result.asInstanceOf[T]
      case None => sys.error("[EROR: retrieveFrom] Data object " + trackingNumber + " could not be retrieved from " + node._1 + ":" + node._2)
    }
  }

  //main used for testing only.
  def main(args: Array[String]) = {
    
    // this is how we configure the master, specify its hostname, its port, and the number of nodes in the cluster
    MasterService.config("localhost", 8000, 1)

    val nodes = List(("localhost",8001))
    val data = List(List(1,2,3))
    val fun = (list: List[Int]) => list.map { x => println(x); x + 1 }

    //val tns = submitAt(nodes,data,fun)
    val res = invokeAt(nodes,data,fun)
    println(res)

    //val result = master !! RetrieveFrom("localhost", 8001, tns(0))
    //println(result)

    MasterService.shutdown

//    terminate.countDown()
//    terminate.await()
  }

}
