package com.avast.clients.rabbitmq
import cats.Monad
import cats.effect._
import cats.effect.implicits.{genSpawnOps, genSpawnOps_}
import cats.effect.kernel.Resource
import cats.effect.kernel.Resource.ExitCase
import cats.effect.std.{Dispatcher, Queue, Semaphore}
import cats.implicits._
import com.avast.clients.rabbitmq.DefaultRabbitMQStreamingConsumer.{DeferredResult, DeliveryQueue}
import com.avast.clients.rabbitmq.api.DeliveryResult.Reject
import com.avast.clients.rabbitmq.api._
import com.rabbitmq.client.{Delivery => _, _}
import fs2.Stream
import fs2.concurrent.SignallingRef
import org.slf4j.event.Level

import java.time.Instant
import scala.concurrent.duration.FiniteDuration

class DefaultRabbitMQStreamingConsumer[F[_], A] private (base: ConsumerBase[F, A],
                                                         channelOpsFactory: ConsumerChannelOpsFactory[F, A],
                                                         initialConsumerTag: String,
                                                         consumerListener: ConsumerListener[F],
                                                         processTimeout: FiniteDuration,
                                                         timeoutAction: DeliveryResult,
                                                         timeoutLogLevel: Level,
                                                         recoveringMutex: Semaphore[F])(createQueue: F[DeliveryQueue[F, A]])(
    implicit val sync: Sync[F],
    async: Async[F],
    dispatcher: Dispatcher[F])
    extends RabbitMQStreamingConsumer[F, A] {
  import base._

  implicit val monadCancel: MonadCancel[F, Throwable] = implicitly[MonadCancel[F, Throwable]]

//  private lazy val streamFailureMeter = consumerRootMonitor.meter("streamFailures")

  private lazy val consumer = Ref.unsafe[F, Option[StreamingConsumer]](None)
  private lazy val isClosed = Ref.unsafe[F, Boolean](false)
  private lazy val isOk = Ref.unsafe[F, Boolean](false)

  lazy val deliveryStream: fs2.Stream[F, StreamedDelivery[F, A]] = {
    val stream: Stream[F, DeliveryQueue[F, A]] = Stream.resource(Resource.eval(checkNotClosed) >> recoverIfNeeded)
    stream
      .flatMap { queue =>
        Stream.eval {
          queue.take.flatMap {
            case (del: StreamedDelivery[F, A], ref: SignallingRef[F, Option[DeferredResult[F]]]) =>
              ref.get.map((x: Option[DeferredResult[F]]) =>
                x.map { _ =>
                  del
              })
          }
        }
      }
      .unNone
      .onFinalizeCase(handleStreamFinalize)
  }

  private def createStreamedDelivery(delivery: Delivery[A],
                                     deffRef: SignallingRef[F, Option[DeferredResult[F]]]): StreamedDelivery[F, A] = {
    (f: DeliveryReadAction[F, A]) =>
      deffRef.get.flatMap {
        case Some(deff) =>
          f(delivery).start.flatMap { fiber =>
            val waitForCancel: F[Unit] = {
              deffRef.discrete
                .collect {
                  // We don't care about Some. None means cancel; Some appears only "sometimes" as the initial value update.
                  case None => fiber.cancel
                }
                .take(1) // wait for a single (first) update
                .compile
                .last
                .flatMap(_.getOrElse(throw new IllegalStateException("This must not happen!")))
            }

            val waitForFinish = {
              fiber.joinWithNever
                .map(ConfirmedDeliveryResult(_))
                .attempt
                .flatMap { dr =>
                  // wait for completion AND confirmation of the result (if there was any)
                  deff.complete(dr) >> dr.map(_.awaitConfirmation).getOrElse(Sync[F].unit)
                }
            }

            (waitForCancel race waitForFinish).as(())
          }

        case None => Sync[F].unit // we're not starting the task
      }
  }

  private lazy val recoverIfNeeded: Resource[F, DeliveryQueue[F, A]] = {
    Resource(recoveringMutex.permit.use { _ =>
      Resource
        .eval(isOk.get)
        .flatMap {
          case true => Resource.eval(getCurrentQueue)
          case false => recover
        }
        .allocated // this is plumbing... we need to _stick_ the resource through plain F here, because of the mutex
    })
  }

  private lazy val getCurrentQueue: F[DeliveryQueue[F, A]] = {
    consumer.get.map { cons =>
      cons
        .getOrElse(throw new IllegalStateException("Consumer has to be initialized at this stage! It's probably a BUG"))
        .queue
    }
  }

  private lazy val recover: Resource[F, DeliveryQueue[F, A]] = {
    Resource.eval(createQueue).flatTap { newQueue =>
      channelOpsFactory.create.flatMap { channelOps =>
        Resource
          .make {
            SignallingRef[F, Boolean](true)
              .map(new StreamingConsumer(channelOps, newQueue, _))
              .flatMap { newConsumer =>
                consumer
                  .getAndSet(Some(newConsumer))
                  .flatMap { oc =>
                    val consumerTag = oc match {
                      case Some(oldConsumer) => oldConsumer.getConsumerTag
                      case None => initialConsumerTag
                    }

                    consumerLogger.plainDebug(s"[$consumerName] Starting consuming") >>
                      DefaultRabbitMQClientFactory.startConsumingQueue(channelOps.channel, queueName, consumerTag, newConsumer) >>
                      isOk.set(true)
                  }
                  .as(newConsumer)
              }
          } { oldConsumer =>
            val consumerTag = oldConsumer.getConsumerTag
            consumerLogger.plainDebug(s"[$consumerName] Cancelling consumer with consumer tag $consumerTag") >>
              oldConsumer.stopConsuming()
          }
      }
    }
  }

  private lazy val close: F[Unit] = recoveringMutex.permit.use { _ =>
    isOk.get.flatMap { isOk =>
      if (isOk) consumer.get.flatMap {
        case Some(streamingConsumer) => streamingConsumer.stopConsuming()
        case None => Sync[F].unit
      } else Sync[F].unit
    } >> isOk.set(false) >> isClosed.set(true)
  }

  private lazy val stopConsuming: F[Unit] = {
    recoveringMutex.permit.use { _ =>
      isOk.set(false)
    // the consumer is stopped by the Resource
    }
  }

  private lazy val checkNotClosed: F[Unit] = {
    isClosed.get.flatMap(cl =>
      if (cl) Sync[F].raiseError[Unit](new IllegalStateException("This consumer is already closed")) else Sync[F].unit)
  }

  private def handleStreamFinalize(e: Resource.ExitCase): F[Unit] = e match {
    case ExitCase.Succeeded =>
      stopConsuming
        .flatTap(_ => consumerLogger.plainDebug(s"[$consumerName] Delivery stream was completed"))

    case ExitCase.Canceled =>
      stopConsuming
        .flatTap(_ => consumerLogger.plainDebug(s"[$consumerName] Delivery stream was cancelled"))

    case ExitCase.Errored(e: ShutdownSignalException) =>
      stopConsuming
        .flatTap { _ =>
//          streamFailureMeter.mark >>
          consumerLogger.plainError(e)(
            s"[$consumerName] Delivery stream was terminated because of channel shutdown. It might be a BUG int the client")
        }

    case ExitCase.Errored(e) =>
      stopConsuming
        .flatTap(
          _ =>
//            streamFailureMeter.mark >>
            consumerLogger.plainDebug(e)(s"[$consumerName] Delivery stream was terminated"))
  }

  private def enqueueDelivery(delivery: Delivery[A], deferred: DeferredResult[F]): F[SignallingRef[F, Option[DeferredResult[F]]]] = {
    for {
      consumerOpt <- recoveringMutex.permit.use { _ =>
        this.consumer.get
      }
      consumer = consumerOpt.getOrElse(throw new IllegalStateException("Consumer has to be initialized at this stage! It's probably a BUG"))
      ref <- SignallingRef(Option(deferred))
      streamedDelivery = createStreamedDelivery(delivery, ref)
      _ <- consumer.queue.offer((streamedDelivery, ref))
    } yield {
      ref
    }
  }

  private class StreamingConsumer(channelOps: ConsumerChannelOps[F, A],
                                  val queue: DeliveryQueue[F, A],
                                  receivingEnabled: SignallingRef[F, Boolean])
      extends ConsumerWithCallbackBase(base, channelOps, DeliveryResult.Retry, consumerListener) {

    override protected def handleNewDelivery(d: DeliveryWithContext[A]): F[Option[ConfirmedDeliveryResult[F]]] = {
      import d._
      import context._

      val ignoreDelivery: F[Option[ConfirmedDeliveryResult[F]]] = consumerLogger
        .debug(
          s"[$consumerName] The consumer was discarded, throwing away delivery $messageId ($deliveryTag) - it will be redelivered later")
        .as(None)

      receivingEnabled.get
        .flatMap {
          case true =>
            Deferred[F, Either[Throwable, ConfirmedDeliveryResult[F]]]
              .flatMap { waitForResult(delivery, messageId, deliveryTag) }
              .flatMap { dr =>
                receivingEnabled.get.flatMap {
                  case false => ignoreDelivery
                  case true => Sync[F].pure(Some(dr))
                }
              }

          case false => ignoreDelivery
        }
    }

    def stopConsuming(): F[Unit] = {
      receivingEnabled.getAndSet(false).flatMap {
        case true =>
          consumerLogger.plainDebug(s"[$consumerName] Stopping consummation for $getConsumerTag") >>
            Sync[F].delay {
              channelOps.channel.basicCancel(getConsumerTag)
              channelOps.channel.setDefaultConsumer(null)
            }

        case false => consumerLogger.plainDebug(s"Can't stop consummation for $getConsumerTag because it's already stopped")
      }
    }

    private def waitForResult(delivery: Delivery[A], messageId: MessageId, deliveryTag: DeliveryTag)(deferred: DeferredResult[F])(
        implicit dctx: DeliveryContext): F[ConfirmedDeliveryResult[F]] = {
      enqueueDelivery(delivery, deferred)
        .flatMap { ref =>
          val enqueueTime = Instant.now()

          val discardedNotification = receivingEnabled.discrete
            .filter(en => !en) // filter only FALSE
            .take(1) // wait for a single (first) update
            .compile
            .last
            .map(_.getOrElse(throw new IllegalStateException("This must not happen!")))

          val result = Sync[F].race(discardedNotification, deferred.get).flatMap {
            case Right(Right(r)) => Sync[F].pure(r)
            case Left(_) => Sync[F].pure(ConfirmedDeliveryResult[F](Reject)) // it will be ignored later anyway...

            case Right(Left(err)) =>
              consumerLogger.debug(err)(s"[$consumerName] Failure when processing delivery $messageId ($deliveryTag)") >>
                Sync[F].raiseError[ConfirmedDeliveryResult[F]](err)
          }

          watchForTimeoutIfConfigured(processTimeout, timeoutAction, timeoutLogLevel)(delivery, result) {
            Sync[F].defer {
              val l = java.time.Duration.between(enqueueTime, Instant.now())
              consumerLogger.debug(s"[$consumerName] Timeout after $l, cancelling processing of $messageId ($deliveryTag)")
            } >> ref.set(None) // cancel by this!
          }
        }
    }
  }
}

object DefaultRabbitMQStreamingConsumer {

  private type DeferredResult[F[_]] = Deferred[F, Either[Throwable, ConfirmedDeliveryResult[F]]]
  private type QueuedDelivery[F[_], A] = (StreamedDelivery[F, A], SignallingRef[F, Option[DeferredResult[F]]])
  private type DeliveryQueue[F[_], A] = Queue[F, QueuedDelivery[F, A]]

  def make[F[_]: Monad: Sync: Temporal, A: DeliveryConverter](
      base: ConsumerBase[F, A],
      channelOpsFactory: ConsumerChannelOpsFactory[F, A],
      initialConsumerTag: String,
      consumerListener: ConsumerListener[F],
      queueBufferSize: Int,
      timeout: FiniteDuration,
      timeoutAction: DeliveryResult,
      timeoutLogLevel: Level)(implicit cs: Dispatcher[F], genConcurrent: Async[F]): Resource[F, DefaultRabbitMQStreamingConsumer[F, A]] = {
    val newQueue: F[DeliveryQueue[F, A]] = createQueue(queueBufferSize)(genConcurrent)

    Resource.make(Semaphore[F](1).map { mutex =>
      new DefaultRabbitMQStreamingConsumer(
        base,
        channelOpsFactory,
        initialConsumerTag,
        consumerListener,
        timeout,
        timeoutAction,
        timeoutLogLevel,
        mutex,
      )(newQueue)
    })(_.close)
  }

  private def createQueue[F[_]: Async, A](queueBufferSize: Int): F[DeliveryQueue[F, A]] = {
    Queue.bounded[F, QueuedDelivery[F, A]](queueBufferSize)
  }
}
