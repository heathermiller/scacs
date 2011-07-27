
package scacs

import scala.concurrent.SyncVar
import akka.actor.ActorRef

class ClusterWorker(localMaster: ClusterService) extends Thread {

  import MasterService._
  
  var todo : SyncVar[((ClusterService,Any)=>Any, Any, Int)] = new SyncVar
  //var buffers: 

  @volatile var shouldShutdown = false
  
  override def run = {
	while (!shouldShutdown) {
	  if (debug) println("[ClusterWorker] (class): waiting for a new task")
	  val (block, input, trackingNumber) = todo.take() 

	  if (debug) println("[ClusterWorker] (class): executing code with result tn " + trackingNumber)
	  val result = block(localMaster, input)

	  val msg = Result(trackingNumber, result)
	  if (debug) println("[ClusterWorker] (class): sending " + msg + " to CS")
	  localMaster.self ! msg
	}
	if (debug) println("[ClusterWorker] (class): terminating thread")
  }

}


