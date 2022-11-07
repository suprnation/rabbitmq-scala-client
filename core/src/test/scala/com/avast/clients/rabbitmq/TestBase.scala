package com.avast.clients.rabbitmq
import cats.effect.{IO, Resource}
import com.typesafe.scalalogging.StrictLogging
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.concurrent.Eventually
import org.scalatestplus.junit.JUnitRunner
import org.scalatestplus.mockito.MockitoSugar

import scala.concurrent.TimeoutException
import scala.concurrent.duration._
import scala.language.{implicitConversions, postfixOps}
import cats.effect.unsafe.implicits.global

@RunWith(classOf[JUnitRunner])
class TestBase extends FunSuite with MockitoSugar with Eventually with StrictLogging {
//  protected implicit def taskToOps[A](t: Task[A]): TaskOps[A] = new TaskOps[A](t)
  protected implicit def IOToOps[A](t: IO[A]): IOOps[A] = new IOOps[A](t)
  protected implicit def resourceToIOOps[A](t: Resource[IO, A]): ResourceIOOps[A] = new ResourceIOOps[A](t)
//  protected implicit def resourceToTaskOps[A](t: Resource[Task, A]): ResourceTaskOps[A] = new ResourceTaskOps[A](t)
}

object TestBase {
//  val testBlockingScheduler: Scheduler = Scheduler.io(name = "test-blocking")
}

//class TaskOps[A](t: Task[A]) {
//  def await(duration: Duration): A = t.runSyncUnsafe(duration)
//  def await: A = await(10.seconds)
//}

class IOOps[A](t: IO[A]) {
  def await(duration: FiniteDuration): A = (t.timeout(duration)).unsafeRunSync()
  def await: A = await(10.seconds)
}

class ResourceIOOps[A](val r: Resource[IO, A]) extends AnyVal {
  def withResource[B](f: A => B): B = {
    withResource(f, 1000 seconds)
  }

  def withResource[B](f: A => B, timeout: FiniteDuration): B = {
    r.use(a => IO(f).map(_(a))).unsafeRunTimed(timeout).getOrElse(throw new TimeoutException("Timeout has occurred"))
  }
}

//class ResourceTaskOps[A](val r: Resource[Task, A]) extends AnyVal {
//  def withResource[B](f: A => B): B = {
//    withResource(f, Duration.Inf)
//  }
//
//  def withResource[B](f: A => B, timeout: Duration): B = {
//    r.use(a => Task.delay(f(a))).runSyncUnsafe(timeout)(TestBase.testBlockingScheduler, CanBlock.permit)
//  }
//}
