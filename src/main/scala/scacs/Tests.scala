
import scacs._

object Tests {

  // node, function, data fields
  val node0 = 0 //nodeID 
  val node1 = 1 //nodeID
  val buffer0 = 0 //local buffer ID
  val consumer0 = 0 //local consumer ID
  var tn234 = -1 // tracking number to reuse later
  val data = List(1,2,3) //some arbitrary data
  val fun = (list: List[Int]) => list.map { x => println(x); x + 1 } //an arbitrary function
  // an arbitrary function, for testing `getFrom`
  val localGetFun = (str: String) => {
    Thread.sleep(1000)
      
    println("[Test] localGetFun closure: getting item from buffer 0")
    val item = ClusterService.getFrom[Int](node0,buffer0,consumer0)
    println(item)
      
    // return item
    item
  }
  //an arbitrary function, for testing `putAt`
  val remotePutFun = (str: String) => {
    println("[Test] remotePutFun closure: putting item into buffer 0")
    ClusterService.putAt(node0,buffer0, 999)
  }
  //an arbitrary function, for testing `await`
  val localAwaitFun = (str: String) => {
    Thread.sleep(1000)
      
    println("[Test] localAwaitFun closure: invoking await on buffer 0")
    ClusterService.await(node0,buffer0,consumer0)
      
    //returning something...
    9999
  }
  // an arbitrary function, for testing `signal`      
  val remoteSignalFun = (str: String) => {  
    println("[Test] remoteSignalFun closure: putting token into buffer 0")
    ClusterService.signal(node0,buffer0)
  }      

  
  
  def startup(master: String = "localhost", port: Int = 8000, numNodes: Int = 2) = MasterService.config(master, port, numNodes)
  
  def testSubmitRetrieve= { 
     //testing `submitAt` and `retrieveFrom`    
    println("[Test] testSubmitRetrieve: STARTING... submitAt/retrieveFrom test.")
    
    println("[Test] testSubmitRetrieve: calling submitAt...")
    val tn = MasterService.submitAt(node0, data, fun)
    println("[Test] testSubmitRetrieve: tn returned from submitAt: "+tn)
    // save this tn to use later
    tn234 = tn
    
    println("[Test] testSubmitRetrieve: calling retrieveFrom...")
    val res = MasterService.retrieveFrom[List[Int]](node0,tn)
    println("[Test] testSubmitRetrieve: result returned from retrieveFrom: "+res+", stored under tn "+tn)
    
    println("[Test] testSubmitRetrieve: DONE performing submitAt/retrieveFrom test.")
  }
  
  def testInvoke = {
    //testing `invokeAt`
    println("[Test] testInvoke: STARTING... invokeAt test.")
    
    val res = MasterService.invokeAt(node0,data,fun)
    println("[Test] testInvoke: result returned from invokeAt: "+res)
    println("[Test] testInvoke: DONE performing invokeAt test.")
  }
  
  def testPutGet = {
    //testing `putAt` and `getFrom` in ClusterService.
    println("[Test] testPutGet: STARTING... putAt/getFrom test on multibuffers.")
    
    //performing `getFrom` locally on node0
    println("[Test] testPutGet: sending getFrom closure to node 0, will perform getFrom locally on node 0.")
    val tn1 = MasterService.submitAt(node0, "", localGetFun)
    
    //performing `putAt` from node1 to node0
    println("[Test] testPutGet: sending putAt closure to node 1, will perform putAt on node 0.")
    val tn2 = MasterService.submitAt(node1, "", remotePutFun)
    
    //retrieving result stored after `getFrom` on node0
    println("[Test] testPutGet: putting/getting done. Retrieving put on and gotten from node0...")
    val res = MasterService.retrieveFrom[Int](0, tn1)
    println("[Test] testPutGet: result put on and gotten from node0: "+res)
    println("[Test] testPutGet: DONE performing putAt/getFrom on multibuffers.")
  }
  
  def testOperateOn = {
    // testing `operateOn`
    println("[Test] testOperateOn: STARTING... operateOn test.")
    
    println("[Test] testOperateOn: calling operateOn.")
    val tn = MasterService.operateOn(node0, fun, false, tn234)
    println("[Test] testOperateOn: tn returned from operateOn: "+tn)
    
    println("[Test] testOperateOn: now retrieving result stored at tn "+tn+"...")
    val res = MasterService.retrieveFrom[List[Int]](node0, tn)
    println("[Test] testOperateOn: result stored under tn "+tn+" retrieved: "+res)
    
    println("[Test] testOperateOn: DONE performing operateOn test.")
  }
  
  def testOperateOnAndGet = {
    // testing `operateOnAndGet`
    println("[Test] testOperateOnAndGet: STARTING... operateOnAndGet test.")
    
    println("[Test] testOperateOnAndGet: calling operateOnAndGet.")
    val res = MasterService.operateOnAndGet(node0, fun, tn234)
    println("[Test] testOperateOnAndGet: result returned from operateOnAndGet: "+res)

    println("[Test] testOperateOn: DONE performing operateOnAndGet test.")
  }
  
  
  def testStoreAt = {
    // testing `storeAt`
    println("[Test] testStoreAt: STARTING... storeAt test.")
   
    println("[Test] testStoreAt: calling storeAt.")
    val tn = MasterService.storeAt(node0, List(2,3,4))
    println("[Test] testStoreAt: result stored at node "+node0+"under tn: "+tn)
    
    println("[Test] testStoreAt: now retrieving result stored at tn "+tn+"...")
    val res = MasterService.retrieveFrom[List[Int]](node0, tn)
    println("[Test] testStoreAt: Received data stored under "+tn+", result: "+res)
    
    println("[Test] testStoreAt: DONE performing storeAt test.")
  }
  
  
  def testSignalAwait ={
	//testing `signal` and `await` in ClusterService.
    println("[Test] testSignalAwait: STARTING... signal/await test on multibuffers.")
    
    //performing `await` locally on node0
    println("[Test] testSignalAwait: sending await closure to node 0, will perform await locally on node 0.")
    val tn1 = MasterService.submitAt(node0, "", localAwaitFun)
    
    //performing `signal` from node1 to node0
    println("[Test] testSignalAwait: sending signal closure to node 1, will perform signal on node 0.")
    val tn2 = MasterService.submitAt(node1, "", remoteSignalFun)
    
    //retrieving result stored after `getFrom` on node0
    println("[Test] testSignalAwait: signalling/awaiting done. Retrieving arbitrary result returned from localAwaitFun stored on node0...")
    val res = MasterService.retrieveFrom[Int](node0, tn1)
    println("[Test] testSignalAwait: result of localAwaitFun retrieved from node0: "+res)
    
    println("[Test] testSignalAwait: DONE performing signal/await on multibuffers.")
  }
  
  def shutdown = MasterService.shutdown
  
  def main(args: Array[String]) {

    val master = args(0)
    // configures MasterService with hostname "localhost", port 8000, and tells it to expect 2 nodes to register
    startup(master) 
    
    // tests `submitAt` and `retrieveFrom`
    testSubmitRetrieve

    // tests `submitAt` and `retrieveFrom`
    testInvoke
    
    // tests `putAt` and `getFrom` using multibuffers on ClusterService
    testPutGet
    
    // tests `operateOn`
    testOperateOn
    
    // tests `operateOnAndGet`
    testOperateOnAndGet
    
    // tests `storeAt`
    testStoreAt
    
    // tests `signal` and `await` using multibuffers on ClusterService
    testSignalAwait
    
    shutdown
     
  }
}
