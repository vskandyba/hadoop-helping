import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object FutureWorker {

  def main(args: Array[String]): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val fut_1: Future[Int] = Future {
      Thread.sleep(1000)
      println("Task: create table table_1")
      1 + 1
    }

    val fut_2: Future[Int] = Future {
      Thread.sleep(5000)
      println("Task: create table table_2")
      2 + 3
    }
    val fut_3: Future[Int] = Future {
      Thread.sleep(7000)
      println("Task: create table table_3")
      5 + 6
    }
    val fut_4: Future[Int] = Future {
      Thread.sleep(3000)
      println("Task: create table table_4")
      11 + 12
    }

    val tasksMap: Map[String, Future[Int]] = Map("task_1" -> fut_1, "task_2" -> fut_2, "task_3" -> fut_3, "task_4" -> fut_4)
    // val statusFutures: List[Int] = Await.result(Future.sequence(tasksMap.values.toList), Duration.Inf)
    val statusTasks: Map[String, Int] = tasksMap.map(
      pair => (pair._1, Await.result(pair._2, Duration.Inf))
    )

    println("Status tasks:")
    statusTasks.foreach(
      pair => println(s"${pair._1}: ${pair._2}")
    )

  }
}