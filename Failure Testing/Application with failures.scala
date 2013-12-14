import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef
import scala.util.Random
import akka.actor.Scheduler
import scala.concurrent.duration._

case class init(neighbours:Array[ActorRef])
case class mesg(str:String)
case class end
case class z(ratio:String)
case class status
case class start
case class psmesg(msg:Array[Double])


class Boss(args:Array[String]) extends Actor {

  /*
 * args(0)=noOfNodes
 * args(1)=topology
 * args(2)=algorithm
 */

  
  val noOfNodes= args(0).toInt
  val topology =  args(1)
  val algorithm = args(2)
  var counter:Int =0
  val rand = new Random
  val failures = args(3).toInt
  println("Setting up network..")
  val nodes = setupNodes(noOfNodes,topology,algorithm)
  println("Network set up..")
  
  setupLinks(nodes)
  println("Starting..")
  
  for(i <- 1 to failures)
  {
    val tar = nodes(rand.nextInt(noOfNodes))
    println(tar.path.name)
    context.stop(tar)
  }
  val startTime =System.currentTimeMillis
  
  if(algorithm=="gossip")
   nodes(noOfNodes-1) ! mesg("hi")

  else if(algorithm=="push-sum")
    nodes(1) ! start
  
  
  def getNeighbours(nodeid:Int,numNodes:Int,topology:String):Array[Int]={
   
    var neighbours = Array[Int]()
    var col:Int=0
    var row:Int=0
    var colLen = math.sqrt(numNodes).ceil.toInt
    var rowLen = 0
    
    if((numNodes.toDouble/colLen).isValidInt)
      rowLen = (numNodes.toDouble/colLen).toInt
    else
      rowLen = (numNodes.toDouble/colLen).ceil.toInt
    
      
    col=nodeid%colLen
    row=(nodeid.toDouble/colLen.toDouble).floor.toInt

    
    if(topology=="full"){    
    }
    
    if(topology=="line"){      
 
      if(nodeid==0){
        neighbours = neighbours :+ (nodeid + 1)
      }
      
      else if(nodeid==numNodes-1){
        neighbours = neighbours :+ (nodeid - 1)
      }
      
      else{
        neighbours = neighbours :+ (nodeid - 1)
        neighbours = neighbours :+ (nodeid + 1)
      }
      
    }
    
    
    if(topology=="2D"){
        
        if(nodeid==numNodes-1){
          
          if(col==0){
        	  neighbours = neighbours :+ (nodeid-colLen)
          	}
      
          else{
        	  neighbours = neighbours :+ (nodeid-colLen)
        	  neighbours = neighbours :+ (nodeid - 1)
          }
          
        }
        
        else{
        if(col==0){
          neighbours = neighbours :+ (nodeid + 1)
        }
        else if(col==colLen-1){
          neighbours = neighbours :+ (nodeid - 1)
        }
        else{
          neighbours = neighbours :+ (nodeid - 1)
          neighbours = neighbours :+ (nodeid + 1)
        }
          
        if(row==0){
          neighbours = neighbours :+ (nodeid+colLen)
        }
        else if(row==rowLen-1){
          neighbours = neighbours :+ (nodeid-colLen)
        }
        else if(row==rowLen-2){
          if(col>(colLen-((colLen*rowLen)-numNodes)))
            neighbours = neighbours :+ (nodeid-colLen)
        }
        else{          
          neighbours = neighbours :+ (nodeid+colLen)
          neighbours = neighbours :+ (nodeid-colLen)   
        }
        }
        
 
      
    }
   
    if(topology=="imp2D"){
      neighbours = getNeighbours(nodeid,numNodes,"2D")
      val randum = getNeighbours(nodeid,numNodes,"full")
      var randomo = new Random
      neighbours = neighbours :+ randomo.nextInt(numNodes)
    }
   
    //  neighbours.foreach(a => println(nodeid.toString+": "+a.toString+" "+colLen.toString+" "+rowLen.toString+" "+col.toString+" "+row.toString))
   
      neighbours

  }

  def setupNodes(numNodes:Int,topology:String,algorithm:String):Array[ActorRef] ={
    
    var nodeArray = Array[ActorRef]()
    
    for(i <-0 until numNodes){
   
      nodeArray = nodeArray :+ context.actorOf(Props(new Worker(getNeighbours(i,numNodes,topology),topology)),
    		  										"Node"+i.toString)
    }   
    
    nodeArray     
  }
  
  def setupLinks(nodes:Array[ActorRef])={   
    nodes.foreach(n => ( n ! init(nodes) ) )
  }
  
  
  def receive ={
       
  case z(ratio) =>{
      if(counter<=noOfNodes){
        print(ratio)
        counter = counter + 1
        if(counter==noOfNodes-failures){
        println("Time taken to converge in millisecs: "+(System.currentTimeMillis-startTime).toString)
        context.system.shutdown
        }
      }
      
    }

    case end=>{
      if(counter<noOfNodes){
      println(sender.path+" received message")
      counter = counter + 1
      if(counter==noOfNodes-failures){
        println("All actors received the message , shutting down..")
        println("Time taken to converge in millisecs: "+(System.currentTimeMillis-startTime).toString)
        context.system.shutdown

      }
      }
    }
    

    
  }
  
}

class Worker(neighbours:Array[Int],topology:String) extends Actor {
  
  import context._
  
  var network = Array[ActorRef]()
  var random = new Random
  var counter:Int =0
  var target:akka.actor.ActorRef = context.self
  var log:String=""
  var reclog:String=""
  var s1 = context.self.path.name.split("Node")
  var s = s1(1).toInt.toDouble + 1
  var w:Double = 1
  var sbyw:Array[Double] = Array(0,0,0)
  var schedulor:akka.actor.Cancellable = _

  
  
  def receive = {
    
    case init(net) =>{ 
     network = net
    }
   
    case mesg(str) =>{
     
      if(counter==0){
          context.parent ! end
              schedulor = context.system.scheduler.schedule(0 milliseconds,0.001 milliseconds){
             if(topology=="full"){
               target = network(random.nextInt(network.length))
             }
             else{
              target = network(neighbours(random.nextInt(neighbours.length)))
             }
             target ! mesg(str)
          }
        }
      
      if(counter<=10){     
        counter = counter + 1
      }
      
      else{
        schedulor.cancel()
      }
      
    }
    
    
    case psmesg(msg) =>{
      s= s+ msg(0)
      w= w+ msg(1)
      sbyw(counter)=s/w
      var t2:Int=0
      if(counter == 0)
        t2=1
      else if(counter==1)
        t2=2
      else
        t2=0
      var diff = (sbyw(counter)-sbyw(t2)).abs
      if(diff<1e-10){
         context.parent ! z(context.self.path.name+" reported "+sbyw(counter).toString+"\n")
         
      }

      counter = counter + 1
      counter = counter % 3      
      var msg1 = Array[Double]()
      msg1 = msg1 :+ s/2
      msg1 = msg1 :+ w/2
      s=s/2
      w=w/2

      if(topology=="full"){
               target = network(random.nextInt(network.length))
             }
             else{
              target = network(neighbours(random.nextInt(neighbours.length)))
             }
      target ! psmesg(msg1)

    }
    
    
    case start=>{
      var msg1 = Array[Double]()
      msg1 = msg1 :+ s/2
      msg1 = msg1 :+ w/2
      s=s/2
      w=w/2
      if(topology=="full"){
               target = network(random.nextInt(network.length))
             }
             else{
              target = network(neighbours(random.nextInt(neighbours.length)))
             }
      target ! psmesg(msg1)
      
      
    }
    

    
  }
  
}


object project2bonus extends App {

  val system = ActorSystem("sys00")
  val supervisor = system.actorOf(Props(new Boss(args)),"TheBoss")

}