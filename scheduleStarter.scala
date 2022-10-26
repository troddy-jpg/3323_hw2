//
// Program to simulate CPU scheduling.
//
// Input to this class's main method is a file (name given as a command-line
// argument) containing one or more lines, each representing one job to
// schedule, and containing:
//   Job ID (string)
//   Arrival time (integer)
//   Running time (integer)
//
// (Optional second command-line argument "--verbose" gives more-verbose 
// output.  You do not have to use this in your code, but it may be
// useful for debugging.)
//
// Output is, for each scheduling algorithm, the following
//     information for each job, plus average turnaround and response:
//   Job ID and other input info
//   Start time
//   End time
//   Turnaround time
//   Response time (defined as time waiting to first be scheduled)
//

// FIXME comments show where you must add/change code.  

import scala.util.Sorting
import scala.collection.mutable.Queue
import scala.collection.mutable.PriorityQueue
//
// ---- Main program ----
//
if (args.length < 1) {
  System.err.println("parameters:  inputfile [--verbose]")
  sys.exit(1)
}
val verbose = (args.length > 1) && (args(1) == "--verbose")
val jobsIn = parseFile(args(0))

scheduleAndPrint("First come, first served", scheduleFCFS, jobsIn, None)
scheduleAndPrint("Shortest job first", scheduleSJF, jobsIn, None);
scheduleAndPrint("Round robin, time slice = 1", scheduleRR, jobsIn, Some(1));
scheduleAndPrint("Round robin, time slice = 4", scheduleRR, jobsIn, Some(4));

// ---- Classes for representing each job ----

case class JobInfoIn(jobID:String, arrival:Int, runtime:Int)
class JobInfo(info:JobInfoIn) {
  val jobID = info.jobID
  val arrival = info.arrival
  val runtime = info.runtime
  var start = -1 // -1 for not started yet
  var end = -1 // -1 for not done yet
  var timeLeft = 0 // optional for algorithms that might need it

  override def toString : String = {
    ("ID %s, arrival %s, runtime %s, "+
      "start %s, end %s, timeLeft %s").format(
      jobID, arrival, runtime, start, end, timeLeft)
  }
}

// ---- Functions ---- //

def parseFile(fileName:String) : List[JobInfoIn] = {
  val lines = 
    try {
      val file = io.Source.fromFile(fileName)
      val tmp = file.getLines.toList
      file.close
      tmp
    }
    catch {
      case e:java.io.FileNotFoundException => {
        System.err.println("file "+fileName+" not found")
      sys.exit(1)
    }
    case t:Throwable => {
      System.err.println("error reading file "+fileName+t)
      sys.exit(1)
    }
  }
  lines.map(parseLine(_))
}

def parseLine(line:String) : JobInfoIn = {
  val fields = line.trim.split("""\s+""")
  if (fields.length != 3) {
    System.err.println("invalid input line "+line)
    sys.exit(1)
  }
  try {
    JobInfoIn(fields(0), fields(1).toInt, fields(2).toInt)
  }
  catch {
    case e:NumberFormatException => {
      System.err.println("invalid input line "+line)
      sys.exit(1)
    }
  }
}

def scheduleAndPrint(headerMsg:String, 
  schedulerFunction : (Array[JobInfo], Option[Int]) => Boolean,
  jobsIn:Seq[JobInfoIn], 
  timeSlice:Option[Int]) 
{
  val workArray = jobsIn.map(j => new JobInfo(j)).toArray
  Sorting.stableSort(workArray, 
    (j1:JobInfo, j2:JobInfo) => (j1.arrival < j2.arrival))

  val fmt = "%-4s %8s %8s %8s %8s %12s %8s"
  println()
  println(headerMsg)

  if (!schedulerFunction(workArray, timeSlice)) {
    println()
    println("Not implemented")
    println()
  }
  else {
    println()
    println(fmt.format(
      "job", "arrival", "runtime", "start", "end", "turnaround", "response"))
    println(fmt.format(
      "---", "-------", "-------", "-----", "---", "----------", "--------"))
    var totalTurnaround = 0
    var totalResponse = 0
    workArray.foreach(j => {
      if ((j.start >= 0) && (j.end >= 0)) {
         val turnaround = j.end - j.arrival
         val response = j.start - j.arrival
         println(fmt.format(
           j.jobID, j.arrival, j.runtime, j.start, j.end, turnaround, response))
         totalTurnaround += turnaround
         totalResponse += response
      } else {
        System.err.println(
          "internal error:  job %s not both started and ended", j.jobID)
        System.exit(1)
      }
    })
    println()
    println("Average turnaround time = %.2f".format(
      totalTurnaround.toDouble / workArray.length))
    println("Average response time = %.2f".format(
      totalResponse.toDouble / workArray.length))
    println()
  }
}

//
// ---- Scheduler functions ----
//
// These functions should either:
// (*) Fill in the start and end fields of each element of an array of JobInfo objects 
//     and return "true", or
// (*) Return false (to indicate not implemented yet)
//
// Parameters:
// (*) Array of JobInfo objects built from input and
//     guaranteed to be in order by arrival time.
// (*) Optional integer argument to be used to pass time slice
//     to function for Round Robin.
//
//
// Hints:
//
// I recommend a strategy like the one I use for FCFS:
//
// Simulate actual scheduling, with a "time" counter
// that advances as the algorithm works its way through
// its input, with a "ready queue" of jobs that have arrived but
// have not been completed yet.  This copes gracefully with
// the (perhaps rare) case in which all jobs that have arrived
// have been completed but more jobs will arrive later.
//
// The Scala PriorityQueue may be helpful with some algorithms.
//
// For preemptive algorithms, note that the JobInfo class includes
// a variable timeLeft that will likely be useful.
//

//
// First come, first served.
//
def scheduleFCFS(workArray:Array[JobInfo], unused:Option[Int]) : Boolean = {
  if (verbose) {
    println("\nStarting FCFS scheduling")
  }
 
  // this is a queue of indices into the array
  val readyQueue = new Queue[Int]

  var time = 0
  var nextInput = 0

  // schedule jobs until no more input and ready queue is empty
  while ((nextInput < workArray.length || (readyQueue.length > 0))) {
    // add jobs with arrival times <= current time to ready queue
    while ((nextInput < workArray.length) && (workArray(nextInput).arrival <= time)) {
      readyQueue.enqueue(nextInput)
      nextInput += 1
    }
    if (verbose) {
      println("At time %d, ready queue %s".format(
        time, listOfIDs(readyQueue.toList, workArray)))
    }
    // if there's anything ready, schedule it
    if (readyQueue.length > 0) {
      val jobIndex = readyQueue.dequeue
      val job = workArray(jobIndex)
      if (verbose) {
        println("starting job %s".format(job.jobID))
      }
      job.start = time
      time += job.runtime
      job.end = time
    }
    else {
      // maybe there's more input but with later arrival time
      // quick and dirty fix
      time += 1
    }
  }
  true
}

// Shortest job first.

def scheduleSJF(workArray:Array[JobInfo], unused:Option[Int]) : Boolean = {
  if (verbose) {
    println("\nStarting SJF scheduling")
  }

//list of jobs to be ran, doing it this way lets use auto sort using priority queue
var readyQueue = new PriorityQueue[JobInfo]()(Ordering.by(sortRT))

var time = 0
var nextInput = 0

//schedule jobs until no more input and ready queue is empty
while ((nextInput < workArray.length || (readyQueue.length > 0))) {
	//add jobs with arrival times <= current time to ready queue
	while ((nextInput < workArray.length) && (workArray(nextInput).arrival <= time)) {
		readyQueue.enqueue(workArray(nextInput))
		nextInput += 1
	}
	//if anything is ready, schedule it
	//... but sort by runtime!
	if (readyQueue.length >0) {
		val job = readyQueue.dequeue
		job.start = time
		time += job.runtime
		job.end = time
	}
	else {
		time += 1
	}
	}
 true
}

// Round robin.
/*
(*) Every time it chooses from the ready queue, the queue (function
listOfIDs() could help).

(*) Every time it starts or restarts a job, or ends a job.
*/
def scheduleRR(workArray:Array[JobInfo], timeSlice:Option[Int]) : Boolean = {
  // FIXME your code goes here 
  if (verbose) {
    println("\nStarting round robin scheduling (time slice %d)".format(
      timeSlice.get))
  }
  // this is a queue of indices into the array
  val readyQueue = new Queue[Int]

  var time = 0
  var nextInput = 0
  var i = 0
// schedule jobs until no more input and ready queue is empty
  while ((nextInput < workArray.length || (readyQueue.length > 0))) {
    // add jobs with arrival times <= current time to ready queue
    while ((nextInput < workArray.length) && (workArray(nextInput).arrival <= time)) {
	readyQueue.enqueue(nextInput)
	nextInput += 1
    }
    // if there's anything ready, schedule it
    if (verbose) {
      println("At time %d, ready queue %s".format(
        time, listOfIDs(readyQueue.toList, workArray)))
    }
	
    if (readyQueue.length > 0) {
	
	var jobIndex = readyQueue.front
	var job = workArray(jobIndex)

	if (verbose) {println("working on " + job.jobID)}	

	//check if its been enough time to switch jobs
	if (i == timeSlice.get){
		readyQueue.dequeue
		readyQueue += jobIndex //cycle current jobIndex
		jobIndex = readyQueue.front//new job
		job = workArray(jobIndex)
		i = 0
	}
	//check if the job hasn't been run yet
	if (job.start == -1) {
		job.start = time
		job.timeLeft = job.runtime
	}
	//increment!!!
	time += 1
	i += 1
	job.timeLeft -= 1
	//check if the job is done
	if (job.timeLeft <= 0) {
		if (verbose) {println("ending " + job.jobID)}
		job.end = time
		i = 0
		readyQueue.dequeue
	}
}
    else {
      // maybe there's more input but with later arrival time
      // quick and dirty fix
      time += 1
    }
  }
  true
}


def sortRT(v:JobInfo) = -v.runtime
def listOfIDs(readyQueueList:Seq[Int], workArray:Array[JobInfo]) : String = {
  readyQueueList.map(i => workArray(i).jobID).mkString(",")
}
