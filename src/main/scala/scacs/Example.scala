
import scacs._

object Example {

  def main(args: Array[String]) {
    MasterService.config("localhost", 8000, 1)

    val nodes = List(("localhost",8001))
    val data = List(List(1,2,3))
    val fun = (list: List[Int]) => list.map { x => println(x); x + 1 }

    //val tns = submitAt(nodes,data,fun)
    val res = MasterService.invokeAt(nodes,data,fun)
    println(res)

    //val result = master !! RetrieveFrom("localhost", 8001, tns(0)) //do this only with submitAt. Note: I'll still turn this into a method, retrieveFrom.
    //println(result)

    MasterService.shutdown 
  }
}
