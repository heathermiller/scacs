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

import akka.actor.{Actor, ActorRef, Channel}
import Actor._
import scala.concurrent.SyncVar

class ClusterService extends Actor{
  
  import MasterService._
  
//  var allAddresses: List[(String, Int)] = List()
  var data = Map[Int, (Option[Any], Any=>Unit)]()
  var master: ActorRef = null
  var worker = new ClusterWorker(this) //add numBuffers to create
  val emptyFunction = (param: Any)=>{}

  def receive = {
    case Announce(hostname, port) => 
      master = remote.actorFor(classOf[MasterService].getCanonicalName,hostname,port)
      val localhost = remote.address.getHostName()
      val localport = remote.address.getPort()
      master ! Announce(localhost, localport) 

    case InitializeClusterService(addresses, maxConsumers, bufferMultiplier) =>
      println("[ClusterService] received node addresses: "+addresses)
      
      // build list of all (hostname,port)
      //allAddresses = addresses
      
      // build array of with all ActorRefs
      ClusterService.nodes = Array.ofDim[ActorRef](addresses.length)
      for (((host, port), i) <- addresses.zipWithIndex)
        ClusterService.nodes(i) = remote.actorFor(classOf[ClusterService].getCanonicalName,host,port)
        
      // create array of multi-buffers
      var idx = 0  
      ClusterService.allBuffers = Array.ofDim(maxConsumers * bufferMultiplier)
      for (i <- 1 to bufferMultiplier; numConsumers <- 1 to maxConsumers) {
        ClusterService.allBuffers(idx) = new MultiBuffer(numConsumers)//Array.fill(maxConsumers * bufferMultiplier)(new SyncVar[Any])
        idx += 1
      }  
      
      // pass local ActorRef to ClusterService object
      ClusterService.localActorRef = self
      
      worker.start
      self.reply()
      
    case SubmitAt(_, _, block, input, trackingNumber) => 
      worker.todo.put((block, input, trackingNumber))

    case OperateOn(_, _, block, inputTrackingNumber, outputTrackingNumber) => 
      val outputLocation = outputTrackingNumber match {
    	case Some(trackingNumber) => trackingNumber
   		case None => inputTrackingNumber
      }
      
      // this function should be called when the input becomes available
      val onResult = (result: Any) => {
        worker.todo.put((block, result, outputLocation))
      }
      
      // obtain input for operation
      // if the input is not yet available register the `onResult` function
      data.get(inputTrackingNumber) match {
        case None =>
          data += (inputTrackingNumber -> (None, onResult))
        
        case Some((dataOpt, fun)) =>
          if (!dataOpt.isEmpty)
            worker.todo.put((block, dataOpt.get, outputLocation))
      }
      
    case OperateOnAndGet(_, _, block, inputTrackingNumber,outputTrackingNumber) => 
      
      // this function should be called when the input becomes available
      val onResult = (result: Any) => {
        worker.todo.put((block, result, outputTrackingNumber))
      }
      // this function should be called when the output becomes available
      val thisChannel = self.channel // save ActorRef of sender
      val onOutput = (result: Any) => {
        if (debug) println("[ClusterService] (class), in OperateOnAndGet: sending result: "+result+" to channel "+thisChannel)
        thisChannel ! (outputTrackingNumber, result)
      }      
      data += ( outputTrackingNumber -> (None, onOutput))
      
      // obtain input for operation
      // if the input is not yet available register the `onResult` function
      data.get(inputTrackingNumber) match {
        case None =>
          data += (inputTrackingNumber -> (None, onResult))
        
        case Some((dataOpt, fun)) =>
          if (!dataOpt.isEmpty)
            worker.todo.put((block, dataOpt.get, outputTrackingNumber))
      }
      
    case StoreAt(_, _, receivedData, trackingNumber) =>
      data += (trackingNumber -> (Some(receivedData), emptyFunction))
    
    case InvokeAt(_, _, block, input, trackingNumber) =>
      val result = block(input)
      data += (trackingNumber -> (Some(result), emptyFunction))
      self.sender.get ! (trackingNumber, result)

    case RetrieveFrom(_, _, trackingNumber) =>
      if (debug) println("[ClusterService] (class): received a RetrieveFrom message")
      
      // save ActorRef of sender
      val thisChannel = self.channel
      // this function is called when result is insert into the data table
      val onResult = (result: Any) => {
        if (debug) println("[ClusterService] (class): sending result to channel "+thisChannel)
        thisChannel ! (trackingNumber, result)
      }
      
      data.get(trackingNumber) match {
        case None =>
          if (debug) println("[ClusterService] (class): no data under tn "+trackingNumber)
          data += (trackingNumber -> (None, onResult))
        
        case Some((dataOpt, fun)) =>
	      if (!dataOpt.isEmpty) {
	    	if (debug) println("[ClusterService] (class): responding with data "+dataOpt.get)
	        self.reply(dataOpt.get) 
	      }
	      else {
	        if (debug) println("[ClusterService] (class): dataOpt.isEmpty")
	        data += (trackingNumber -> (None, onResult))     
	      }
      }
      
    case Shutdown =>
      worker.shouldShutdown = true
      remote.shutdown()
      registry.shutdownAll() //this *should* also shutdown the worker actor.
      println("[ClusterService] EXIT. Shutting down.")
      
    case msg@ Result(trackingNumber, input) => 
      // data maps tracking numbers to pairs of (result, function)
      // where function says what should be done when result comes in as new
      if (debug) println("[ClusterService] (class): received a Result message: "+msg)
      data.get(trackingNumber) match {
        
        case None =>
          if (debug) println("[ClusterService] (class): received result for tn "+trackingNumber)
          data += (trackingNumber -> (Some(input), emptyFunction))          
        
        case Some((dataOpt, fun)) =>
	      if (!dataOpt.isEmpty) {
	        /*do nothing*/
	    	if (debug) println("[ClusterService] (class): result for tn already there: "+trackingNumber)
	      }
	      else {
	        if (debug) println("[ClusterService] (class): received result for tn "+trackingNumber)
	        data += (trackingNumber -> (Some(input), emptyFunction))
	        if (debug) println("[ClusterService] (class): invoking callback function")
	        fun(input)
	      }
      }
      
      val (dataOpt, fun) = data(trackingNumber)
      fun(input)
      data += (trackingNumber -> (Some(input), emptyFunction))

    case PutAt(idx, item) =>       
      ClusterService.allBuffers(idx).put(item)
      self.reply()
      
    case GetFrom(bufferIndex, consumerIndex) =>       
      val item = ClusterService.allBuffers(bufferIndex).get(consumerIndex)
      self.reply(item)      
      
    case _ =>
      println("[ClusterService] unknown message")
  }

}

object ClusterService {
  
  // these fields are initialized by the ClusterService actor
  var nodes: Array[ActorRef] = null
  var allBuffers: Array[MultiBuffer[Any]] = null
  var localActorRef: ActorRef = null
  
  def putAt(i: Int, buffer: Int, data: Any) = {
    // check whether buffer is local or remote
    if (isLocal(nodes(i))) {
      /* put directly in that local buffer */
      allBuffers(buffer).put(data)
    }
    else {
      // send PutAt message directly to remote ClusterService
      nodes(i) !! PutAt(buffer, data)
    }
  }
  
  def getFrom[T](i: Int, buffer: Int, consumer: Int): T = {
    // check whether buffer is local or remote
    if (isLocal(nodes(i))) {
      /* get directly from that local buffer */
      allBuffers(buffer).get(consumer).asInstanceOf[T]
    }
    else {
      // send PutAt message directly to remote ClusterService
      (nodes(i) !! GetFrom(buffer, consumer)).asInstanceOf[T]
    }
  }
  
  def isLocal(actorRef: ActorRef) : Boolean = actorRef.uuid == localActorRef.uuid
  
  /*
  //returns location of a specific one-place buffer, (ActorRef, localBufferNumber), given a globalBufferNumber
  def locationOf(globalBufferNumber: Int): (ActorRef, Int) = {
    val i = globalBufferNumber / allBuffers.length
    val localBufferIndex = globalBufferNumber % allBuffers.length
    (nodes(i), localBufferIndex)
  }
  */
  
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
