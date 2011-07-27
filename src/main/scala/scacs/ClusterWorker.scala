
package scacs

import scala.concurrent.SyncVar

class ClusterWorker(localMaster: ClusterService) extends Thread {
  var todo : SyncVar[((ClusterService,Any)=>Any, Any, Int)] = new SyncVar
  @volatile var shouldShutdown = false
  
  def putAt = sys.error("not implemented yet")
  def getFrom = sys.error("not implemented yet")
  
  override def run = {
	while(!shouldShutdown) {
	  val (block, input, trackingNumber) = todo.take() //removes thing from SyncVar so it doesn't repeatedly take the same task
	  val result = block(localMaster, input)
	  localMaster.self ! Result(trackingNumber, result)
	}
  }

}


