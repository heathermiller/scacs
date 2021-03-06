
package scacs

import akka.actor.{Actor, ActorRef, Channel}
import Actor._
import java.util.concurrent.CountDownLatch

class MasterService extends Actor {
  import MasterService._
  
  var numNodes = 0
  var maxConsumers = 0
  var bufferMultiplier = 0
  
  //Maps
  // (host, port) -> NodeID
  // ActorRef -> NodeID
  // ActorRef -> (host,port)
  //keep track of tracking numbers? A way to keep track of multiple distributed data structures? Fault tolerance by keeping track of transformations on tns.
  var results: Map[Int, Any] = Map() // this keeps track of some tracking numbers + results. Those received with retrieveFrom. But what about remote and thus distributed?
  var channels: Map[Int,Channel[Any]] = Map() //duplicate this in ClusterService (with pair, (channel, fun) ) 
  
  def receive = {
    case ClusterSize(numNodes, maxConsumers, bufferMultiplier) =>
      this.numNodes = numNodes
      this.maxConsumers = maxConsumers
      this.bufferMultiplier = bufferMultiplier
      println("[MasterService] waiting for "+this.numNodes+" nodes to register")
      self.reply()

    // Handling new nodes announcing themselves during start-up
    case Announce(newHost, newPort) =>
      println("[MasterService] new host "+newHost+":"+newPort)
      // populate the (host,port)->ActorRef map
      val nodeRef = remote.actorFor(
        classOf[ClusterService].getCanonicalName,
        newHost,
        newPort)
      
      // populate the (host, port) -> ActorRef map, used for checking 
      nodeRefs += ((newHost, newPort) -> nodeRef)
      
      if (nodeRefs.size == numNodes) {
        println("[MasterService] all nodes have registered")
        var i = 0
        for (((host, port), ref ) <- nodeRefs) {
        	// populate arrays of actorRefs (nodes), and (host, port) (addresses)  
        	MasterService.nodes(i) = ref
        	MasterService.addresses(i) = (host, port)
        	i += 1            
        }
        val allNodes = (0 until nodes.length).toList
        if (debug) println("[MasterService] (class): registered nodes: "+ allNodes)
        if (debug) println("[MasterService] (class): corresponding addresses:"+addresses.mkString(","))
        nodeRefs.values foreach { service => service !! InitializeClusterService(MasterService.addresses, maxConsumers, bufferMultiplier) }
        MasterService.doneInit.countDown()
      }

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
      val response = nodeRef !! msg
      if (debug) println("[MasterService] (class): while handling RetrieveFrom message, received response: "+response)
      
      response match {
        case Some((tn, recvd)) =>
          if (debug) println("[MasterService] (class): while handling RetrieveFrom message, sending msg: "+recvd+" to MasterService object.")
          self.reply(recvd)
        case None =>
          sys.error("[MasterService] (class): received None from ("+host+":"+port+")")
      }          

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
  var nodes: Array[ActorRef]= null
  var nodeRefs: Map[(String, Int), ActorRef] = Map() // (host, port)->ActorRef  
  var addresses: Array[(String, Int)] = null
  var tn2nodeID: Map[Int, ActorRef] = Map() // trackingNumber -> ActorRef
  
  // SET THIS TO ENABLE DEBUG OUTPUT
  var debug = false
  
  def getNode(host: String, port: Int): ActorRef =
    nodeRefs.get((host, port)) match {
      case None =>
        MasterService.terminate.countDown() 
        sys.error("[MasterService] node "+host+":"+port+" not registered")
      case Some(nodeRef) =>
        nodeRef
  } // should there be more serious checking here? ...at least make sure there's a host/port?  

  def newTrackingNumber = {
    TrNumIncrementer += 1
    TrNumIncrementer
  }

  def config(hostname: String, port: Int, numNodes: Int, maxConsumers: Int = 10, bufferMultiplier: Int = 1) = {
    remote.start(hostname,port)
    remote.register(actorOf[MasterService])   

    master = remote.actorFor(classOf[MasterService].getCanonicalName, hostname, port)
    master !! ClusterSize(numNodes, maxConsumers, bufferMultiplier)
    
    nodes = Array.ofDim[ActorRef](numNodes)
    addresses = Array.ofDim[(String,Int)](numNodes)

    doneInit.await
  }
  
  def shutdown() = {
    master !! Shutdown
    remote.shutdown()
    registry.shutdownAll()
    println("[MasterService] EXIT. Shutting down.")
  }

  def submitAt[T](i: Int, data: T, fun: T=>Any): Int = {
    // "One" variant of submitAt
    val (host, port) = addresses(i)
    val internalFun = (data: Any) => fun(data.asInstanceOf[T])
    val tn = newTrackingNumber
    if (debug) println("[MasterService] (object): sending SubmitAt to "+host+":"+port)
    val nodeRef = getNode(host, port)
    nodeRef ! SubmitAt(host, port, internalFun, data, tn)
    tn
  }
 
  def submitAt[T](someNodes: List[Int], partitionedData: List[T], fun: T=>Any): List[Int] = {
    // "Some" variant of submitAt
    var tns = List[Int]()
    if (someNodes.length == partitionedData.length) {
      for ((node,data) <- someNodes zip partitionedData) {        
        val tn = submitAt(node, data, fun)
        tns = tn :: tns
      }
      tns
    } else 
      sys.error("[ERROR: MasterService.submitAt] The number of nodes you'd like to submit a task to and the number of pieces of partitioned must be equal.")
  }
  
  def submitAtAll[T](partitionedData: List[T], fun: T=>Any): List[Int] = {
    // "All" variant of submitAt
    // Note, we assume that the user has figured out that they need to provide # data chunks = # nodes
    if (nodes.length == partitionedData.length) {
      val allNodes = (0 until nodes.length).toList
      val tns = submitAt(allNodes, partitionedData, fun)
      tns
    } else 
      sys.error("[ERROR: MasterService.submitAtAll] The number of data chunks fed to submitAtAll must be equal to the total number of nodes in the cluster.")    
    
  }
  
  def invokeAt[T](i: Int, data: T, fun: T=>Any): Any = {
    // "One" variant of invokeAt (Note: due to async receive of results, can't do this one/some/all recursively)
    val (host, port) = addresses(i)
    val internalFun = (data: Any)=> fun(data.asInstanceOf[T])
    val tn = newTrackingNumber
    if (debug) println("[MasterService] (object): sending InvokeAt(One) to master with tn "+tn)
    val nodeRef = getNode(host, port)
    nodeRef ! InvokeAt(host, port, internalFun, data, tn)

    val Some((trNum, res)) = master !! RetrieveFrom("", 0, tn)
    res
    
  }        
  
  def invokeAt[T](someNodes: List[Int], partitionedData: List[T], fun: T=>Any): List[Any] = {
    // "Some" variant of invokeAt   
    var tns = List[Int]()
    if (someNodes.length == partitionedData.length) {
      for ((node,data) <- someNodes zip partitionedData) {
        val (hostname, port) = addresses(node)
        val internalFun = (data: Any) => fun(data.asInstanceOf[T])
        val tn = newTrackingNumber
        tns = tn :: tns
        val nodeRef = getNode(hostname, port)
        if (debug) println("[MasterService] (object): sending InvokeAt(Some) to master with tn "+tn)
        	nodeRef ! InvokeAt(hostname, port, internalFun, data, tn)
      }
      for (tn <- tns) yield {
        val Some((trNum, res)) = master !! RetrieveFrom("", 0, tn)
        res
      }
    } else 
      sys.error("[ERROR: MasterService.invokeAt] The number of nodes you'd like to submit a task to and the number of pieces of partitioned must be equal.")
  }
  
def invokeAtAll[T](partitionedData: List[T], fun: T=>Any): List[Any] = {
    // "All" variant of invokeAt
    // Note, we assume that the user has figured out that they need to provide # data chunks = # nodes
    val allNodes = (0 until nodes.length).toList
    if (debug) println("[MasterService] (object): sending InvokeAt(All) to master")
    invokeAt(allNodes, partitionedData, fun)
  }

  //TODO: it would be nice to structure this such that T could be inferred
  //TODO: extend to list of nodes.
  def retrieveFrom[T](i: Int, trackingNumber: Int) = {
    // "One" variant of invokeAt    
    val endResult = master !! RetrieveFrom(addresses(i)._1, addresses(i)._2, trackingNumber)
    
    endResult match {
      case Some(result) => result.asInstanceOf[T]
      case None => sys.error("[ERROR: MasterService.retrieveFrom] Data object " + trackingNumber + " could not be retrieved from node " + i)
    }
  }
  
  /*
  def retrieve[T](tn: Int) = {
    val nodeID = tn2nodeID(tn)
    val result = retrieveFrom[T](nodeID, tn)
    result
  }
  */
  
  def operateOn[T](i: Int, fun: T=>Any, inPlace: Boolean, inputTn: Int): Int = {
    // "One" variant of operateOn     
    //
    // case 1: in-place update, result is overwritten remotely with same tracking numbers used as input
    //    return input tracking numbers unchanged
    // case 2: result is written to new tracking numbers generated here.
    val (host, port) = addresses(i)
    val internalFun = (data: Any) => fun(data.asInstanceOf[T])
    val outputTn = if (inPlace) inputTn else newTrackingNumber
    if (debug) println("[MasterService] (object): sending OperateOn to master with tn "+outputTn)
    val nodeRef = getNode(host, port)
    nodeRef ! OperateOn(host, port, internalFun, inputTn, if (inPlace) None else Some(outputTn))
    outputTn
  }
  
  def operateOn[T](someNodes: List[Int], fun: T=>Any, inPlace: Boolean, inputTrackingNums: List[Int]): List[Int] = {
    // "Some" variant of operateOn
    var outputTns = List[Int]()
      if (nodes.length == inputTrackingNums.length) {
        for ((node,inputTn) <- someNodes zip inputTrackingNums) {
          val outputTn = operateOn(node, fun, inPlace, inputTn)
          outputTns = outputTn :: outputTns
        }
        outputTns
      } else 
        sys.error("[ERROR: MasterService.operateOn] The number of nodes you'd like to submit a task to and the number of tracking numbers must be equal.")      
  }

   def operateOnAll[T](fun: T=>Any, inPlace: Boolean, inputTrackingNums: List[Int]): List[Int] = {
    // "All" variant of operateOn
    val allNodes = (0 until nodes.length).toList
    operateOn(allNodes, fun, inPlace, inputTrackingNums)      
  } 
  
  def operateOnAndGet[T](i: Int, fun: T=>Any, inputTn: Int): Any = {
    // "One" variant of operateOnAndGet    
    val (host, port) = addresses(i)
    val internalFun = (data: Any) => fun(data.asInstanceOf[T])
    val outputTn = newTrackingNumber
    if (debug) println("[MasterService] (object): sending OperateOnAndGet(One) to master with tn "+outputTn)
    val nodeRef = getNode(host, port)
    nodeRef ! OperateOnAndGet(host, port, internalFun, inputTn, outputTn)
    
    val Some((trNum, res))= master !! RetrieveFrom("", 0, outputTn)
    res
  }
   
  def operateOnAndGet[T](someNodes: List[Int], fun: T=>Any, inputTrackingNums: List[Int]): List[Any] = {
    // "Some" variant of operateOnAndGet    
    var outputTns = List[Int]()
    if (someNodes.length == inputTrackingNums.length) {
        for ((node,inputTn) <- someNodes zip inputTrackingNums) {
          val (hostname, port) = addresses(node)
          val internalFun = (data: Any) => fun(data.asInstanceOf[T])
          val outputTn = newTrackingNumber
          outputTns = outputTn :: outputTns
          val nodeRef = getNode(hostname, port)
          if (debug) println("[MasterService] (object): sending OperateOnAndGet(Some) to master with tn "+outputTn)
          nodeRef ! OperateOnAndGet(hostname, port, internalFun, inputTn, outputTn)
        }
        for (tn <- outputTns) yield {
          val Some((trNum, res))= master !! RetrieveFrom("", 0, tn)
          res
        }        
      } else 
        sys.error("[ERROR: MasterService.operateOnAndGet] The number of nodes you'd like to submit a task to and the number of tracking numbers must be equal.")      
  }  

  def operateOnAndGetAll[T](fun: T=>Any, inputTrackingNums: List[Int]): List[Any] = {
    // "All" variant of operateOnAndGet    
    val allNodes = (0 until nodes.length).toList
    if (debug) println("[MasterService] (object): sending OperateOnAndGet(All) to master")
    operateOnAndGet(allNodes, fun, inputTrackingNums)    
  }
  
  def storeAt(i: Int, data: Any): Int = {
    // "One" variant of storeAt
    val tn = newTrackingNumber
    val (host, port) = addresses(i)
    val nodeRef = getNode(host, port)
    nodeRef ! StoreAt(host, port, data, tn)
    tn
  }
  
  //main used for testing only.
  def main(args: Array[String]) = {
    
    // this is how we configure the master- specify its host name, its port, and the number of nodes in the cluster
    MasterService.config("localhost", 8000, 2)
    
    //nothing here. tests live in Tests.scala.

    MasterService.shutdown
    
  }

}
