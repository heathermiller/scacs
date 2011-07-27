
package scacs

import scala.concurrent.SyncVar

class ClusterWorker(localMaster: ClusterService) extends Thread {

  var todo : SyncVar[((ClusterService,Any)=>Any, Any, Int)] = new SyncVar

  @volatile var shouldShutdown = false
  
  def putAt = sys.error("not implemented yet")
  def getFrom = sys.error("not implemented yet")
  
  override def run = {
	while (!shouldShutdown) {
	  println("CW: waiting for a new task")
	  val (block, input, trackingNumber) = todo.take() //removes thing from SyncVar so it doesn't repeatedly take the same task

	  println("CW: executing code with result tn " + trackingNumber)
	  val result = block(localMaster, input)

	  val msg = Result(trackingNumber, result)
	  println("CW: sending " + msg + " to CS")
	  localMaster.self ! msg
	}
	println("CW: terminating thread")
  }

}


