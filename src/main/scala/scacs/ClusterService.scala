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

class ClusterService extends Actor{
  var allAddresses: List[(String, Int)] = List()
  var data = Map[Int, (Option[Any], Any=>Unit)]()
  var master: ActorRef = null
  var worker = new ClusterWorker(this)
  val emptyFunction = (param: Any)=>{}

  def receive = {
    case Announce(hostname, port) => 
      master = remote.actorFor(classOf[MasterService].getCanonicalName,hostname,port)
      val localhost = remote.address.getHostName()
      val localport = remote.address.getPort()
      master ! Announce(localhost, localport) 

    case Nodes(addresses) =>
      println("[ClusterService] received node addresses: "+addresses)
      allAddresses = addresses
      worker.start
      self.reply()
      
    case SubmitAt(_, _, block, input, trackingNumber) => 
      worker.todo.put((block, input, trackingNumber))

    case OperateOn(_, _, block, inputTrackingNumber, outputTrackingNumber) => 
      val outputLocation = outputTrackingNumber match {
    	case Some(trackingNumber) => trackingNumber
   		case None => inputTrackingNumber
      }
      val (dataOpt, fun) = data(inputTrackingNumber)
      if (!dataOpt.isEmpty) worker.todo.put((block, dataOpt.get, outputLocation))
      else {
        val onResult = (result: Any)=>
          {
            worker.todo.put((block, result, outputLocation))
          }
        data += (inputTrackingNumber -> (None, onResult))
      }

    case InvokeAt(_, _, block, input, trackingNumber) =>
      val result = block(this, input)
      if (!trackingNumber.isEmpty) {
        data += (trackingNumber.get -> (Some(result), emptyFunction))
        self.sender.get ! (trackingNumber.get, result)
      } else 
        self.reply(result)

    case RetrieveFrom(_, _, trackingNumber) =>
      val thisChannel = self.channel
      val onResult = (result: Any) => {
        println("CS: sending result to channel "+thisChannel)
        thisChannel ! result
      }
      
      data.get(trackingNumber) match {
        case None =>
          println("CS: no data under tn "+trackingNumber)
          data += (trackingNumber -> (None, onResult))          
        case Some((dataOpt, fun)) =>
	      if (!dataOpt.isEmpty) {
	    	println("CS: responding with data "+dataOpt.get)
	        self.reply(dataOpt.get) 
	      }
	      else {
	        println("CS: dataOpt.isEmpty")
	        data += (trackingNumber -> (None, onResult))     
	      }
      }
      
    case Shutdown =>
      remote.shutdown()
      registry.shutdownAll() //this *should* also shutdown the worker actor.
      println("[ClusterService] EXIT. Shutting down.")
      
    case Result(trackingNumber, input) => 
      // data maps tracking numbers to pairs of (result, function)
      // where function says what should be done when result comes in as new
      data.get(trackingNumber) match {
        
        case None =>
          println("CS: received result for tn "+trackingNumber)
          data += (trackingNumber -> (Some(input), emptyFunction))          
        
        case Some((dataOpt, fun)) =>
	      if (!dataOpt.isEmpty) {
	        /*do nothing*/
	    	println("CS: result for tn already there: "+trackingNumber)
	      }
	      else {
	        println("CS: received result for tn "+trackingNumber)
	        data += (trackingNumber -> (Some(input), emptyFunction))
	        println("CS: invoking callback function")
	        fun(input)
	      }
      }
      
      
      val (dataOpt, fun) = data(trackingNumber)
      fun(input)
      data += (trackingNumber -> (Some(input), emptyFunction))

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
