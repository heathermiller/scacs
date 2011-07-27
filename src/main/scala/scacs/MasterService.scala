
package scacs

import akka.actor.{Actor, ActorRef, Channel}
import Actor._
import java.util.concurrent.CountDownLatch

class MasterService extends Actor {
  import MasterService._
  
  var numNodes = 0
  var numBuffers = 0
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
    case ClusterSize(numNodes, numBuffers) =>
      this.numNodes = numNodes
      this.numBuffers = numBuffers
      println("[MasterService] waiting for "+this.numNodes+" nodes to register")
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
        nodeRefs.values foreach { service => service !! InitializeClusterService(nodeRefs.keys.toList, numBuffers) }
        MasterService.doneInit.countDown()
      }
    
    case msg @ SubmitAt(host, port, fun, input, tracking) =>
      if (debug) println("[MasterService] (class): sending SubmitAt to "+host+":"+port)
      val nodeRef = getNode(host, port)
      nodeRef ! msg

    case msg @ InvokeAt(host, port, fun, input, tracking) =>
      if (debug) println("[MasterService] (class): sending InvokeAt to "+host+":"+port)
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
      if (debug) println("[MasterService] (class): recieved a RetrieveFrom message, with empty hostname.")
      if ( results.contains(tracking) )
        self.reply(results(tracking))
      else { 
        if (debug) println("[MasterService] (class): queueing up request for tn "+tracking)
        channels += (tracking -> self.channel) // we're queueing up the request
      }
    
    case msg @ RetrieveFrom(host, port, tracking) if host != "" =>
      if (debug) println("[MasterService] (class): recieved a RetrieveFrom message, with non-empty hostname.")
      val nodeRef = getNode(host, port)
      if (debug) println("[MasterService] (class): while handling RetrieveFrom message, sending msg: "+msg+"to "+nodeRef)
      val Some((tn, recvd)) = nodeRef !! msg
      
      val response = recvd 
      if (debug) println("[MasterService] (class): while handling RetrieveFrom message, sending msg: "+response+"to "+self)
      self.reply(response)

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
  var debug = false

  def newTrackingNumber = {
    TrNumIncrementer += 1
    TrNumIncrementer
  }

  def config(hostname: String, port: Int, numNodes: Int, numBuffers: Int = 4) = {
    remote.start(hostname,port)
    remote.register(actorOf[MasterService])   

    master = remote.actorFor(classOf[MasterService].getCanonicalName, hostname, port)
    master !! ClusterSize(numNodes, numBuffers)

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
        if (debug) println("[MasterService] (object): sending SubmitAt to master with tn "+tn)
        master ! SubmitAt(hostname, port, internalFun, data, tn)
      }
      tns
    } else 
      sys.error("[ERROR: MasterService.submitAt] The number of nodes you'd like to submit a task to and the number of pieces of partitioned must be equal.")
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
      sys.error("[ERROR: MasterService.invokeAt] The number of nodes you'd like to submit a task to and the number of pieces of partitioned must be equal.")
  }

  //TODO: it would be nice to structure this such that T could be inferred
  //TODO: extend to list of nodes.
  def retrieveFrom[T](node: (String, Int), trackingNumber: Int) = {
    val endResult = master !! RetrieveFrom(node._1, node._2, trackingNumber)
    
    endResult match {
      case Some(result) => result.asInstanceOf[T]
      case None => sys.error("[ERROR: MasterService.retrieveFrom] Data object " + trackingNumber + " could not be retrieved from " + node._1 + ":" + node._2)
    }
  }

  //main used for testing only.
  def main(args: Array[String]) = {
    
    // this is how we configure the master, specify its hostname, its port, and the number of nodes in the cluster
    MasterService.config("localhost", 8000, 2)

    
    /*
     * EXAMPLE #1 
     * testing `submitAt` and `retrieveFrom`
     */
    val nodes = List(("localhost",8001))
    val data = List(List(1,2,3))
    val fun = (list: List[Int]) => list.map { x => println(x); x + 1 }
   
    val tns = submitAt(nodes, data, fun)
    println("[Program Output] MAIN: tns returned from submitAt: "+tns)
    //val res = invokeAt(nodes,data,fun)
    //println(res)

    val res = retrieveFrom[List[Int]](nodes(0), tns(0))
    println(res)

    /*
     * EXAMPLE #2 
     * testing `getFrom` and `putAt` on one-place buffers remotely and locally
     */
    
    val nodes2 = List(("localhost",8002))
    
    // this function waits for 2 secs and then gets an item from a local buffer
    val localGetFun = (str: String) => {
      Thread.sleep(2000)
      
      // assuming this is running on node 0
      println("getting item from buffer 0")
      val item = ClusterService.getFrom(0)
      println(item)
      
      // return item
      item
    }

    val tn1 = submitAt(nodes, List(""), localGetFun)
    
    
    // this function puts an item into a buffer on a remote node
    val remotePutFun = (str: String) => {
      // assuming this is running on node 1
      println("putting item into buffer 0")
      ClusterService.putAt(0, 999)
    }
    
    val tn2 = submitAt(nodes2, List(""), remotePutFun)
    
    val res2 = retrieveFrom[Int](nodes(0), tn1(0))
    println(res2)
    
    /*
    val localPutFun = {
      // assuming this is running on node 1
      ClusterService.putAt(4,999)
    }
    
    val remoteGetFun  = (str: String) => {
      // assuming this is running on node 0
      val item = ClusterService.getFrom(4)
      println(item)
    }
    */

    
    MasterService.shutdown

//    terminate.countDown()
//    terminate.await()
  }

}
