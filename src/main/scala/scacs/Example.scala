
import scacs._

object Example {

  def main(args: Array[String]) {

    /* this is how you specify the master node. So give
     * it the hostname, port number, and number of nodes you
     * expect the cluster to be.
     */
    MasterService.config("localhost", 8000, 1) 

    //this is a list of nodes. In this example, I'm expecting only 1.
    val nodes = List(("localhost",8001)) 

    //this is a list of data chunks. It must be the same length as nodes.
    val data = List(List(1,2,3)) 
    
    // the function you'd like your list of nodes to apply to your data chunks.
    val fun = (list: List[Int]) => list.map { x => println(x); x + 1 } 

    /*send instructions to nodes, nodes execute instructions, store
     * result locally and return a list of tracking numbers.
     */
    //val tns = submitAt(nodes,data,fun) 

    //send instructions to nodes, and recieve result of fun (not the tracking numbers of the data chunks!)
    val res = MasterService.invokeAt(nodes,data,fun) 
    println(res)

    /* this is only necessary with submitAt. Note: I'll turn this
     * into a method, retrieveFrom, very very soon.
     */
    //val result = master !! RetrieveFrom("localhost", 8001, tns(0)) 
    //println(result)

    MasterService.shutdown 
  }
}
