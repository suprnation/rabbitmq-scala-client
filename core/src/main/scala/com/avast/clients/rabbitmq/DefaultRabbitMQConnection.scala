package com.avast.clients.rabbitmq

import cats.effect._
import cats.effect.std.Dispatcher
import cats.syntax.flatMap._
import com.avast.clients.rabbitmq.api._
import com.avast.clients.rabbitmq.logging.ImplicitContextLogger
import com.rabbitmq.client.ShutdownSignalException

import scala.util.control.NonFatal

class DefaultRabbitMQConnection[F[_]] private (
    connection: ServerConnection,
    info: RabbitMQConnectionInfo,
    republishStrategy: RepublishStrategyConfig,
    override val connectionListener: ConnectionListener[F],
    override val channelListener: ChannelListener[F],
    override val consumerListener: ConsumerListener[F])(implicit F: Dispatcher[F], timer: Temporal[F], async: Async[F])
    extends RabbitMQConnection[F] {

  private val logger = ImplicitContextLogger.createLogger[F, DefaultRabbitMQConnection[F]]

  private val factory = new DefaultRabbitMQClientFactory[F](this, info, republishStrategy)

  def newChannel(): Resource[F, ServerChannel] = {
    createChannel()
  }

  private val createChannelF: F[ServerChannel] = {
    Sync[F]
      .delay {
        try {
          connection.createChannel() match {
            case channel: ServerChannel =>
              channel.addShutdownListener { (cause: ShutdownSignalException) =>
                channelListener.onShutdown(cause, channel).unsafeStartAndForget()
              }
              channelListener.onCreate(channel).unsafeStartAndForget()
              channel

            case null => throw new IllegalStateException(s"New channel could not be created, maybe the max. count limit was reached?")

            // since the connection is `Recoverable`, the channel should always be `Recoverable` too (based on docs), so the exception will never be thrown
            case n => throw new IllegalStateException(s"Required Recoverable Channel")
          }
        } catch {
          case NonFatal(e) =>
            channelListener.onCreateFailure(e).unsafeStartAndForget()
            throw e
        }
      }
      .flatTap(channel => logger.plainDebug(s"Created channel: $channel ${channel.hashCode()}"))
  }

  override def newStreamingConsumer[A: DeliveryConverter](
      consumerConfig: StreamingConsumerConfig
  ): Resource[F, RabbitMQStreamingConsumer[F, A]] = {
    factory.StreamingConsumer
      .create[A](consumerConfig, consumerListener)
      .map(identity[RabbitMQStreamingConsumer[F, A]]) // type inference... :-(
  }

  def newConsumer[A: DeliveryConverter](consumerConfig: ConsumerConfig)(
      readAction: DeliveryReadAction[F, A]): Resource[F, RabbitMQConsumer[F]] = {
    factory.Consumer.create[A](consumerConfig, consumerListener, readAction)
  }

  def newPullConsumer[A: DeliveryConverter](pullConsumerConfig: PullConsumerConfig): Resource[F, RabbitMQPullConsumer[F, A]] = {
    factory.PullConsumer.create[A](pullConsumerConfig)
  }

  private def createChannel(): Resource[F, ServerChannel] =
    Resource.make(createChannelF)(
      channel =>
        logger.plainDebug(s"Closing channel: $channel ${channel.hashCode()}") >>
          Sync[F].delay {
            channel.close()
        })

  override def newProducer[A: ProductConverter](producerConfig: ProducerConfig): Resource[F, RabbitMQProducer[F, A]] = {
    factory.Producer
      .create[A](producerConfig)
  }

  override def declareExchange(config: DeclareExchangeConfig): F[Unit] = withChannel { ch =>
    factory.Declarations.declareExchange(config, ch)
  }

  override def declareQueue(config: DeclareQueueConfig): F[Unit] = withChannel { ch =>
    factory.Declarations.declareQueue(config, ch)
  }

  override def bindExchange(config: BindExchangeConfig): F[Unit] = withChannel { ch =>
    factory.Declarations.bindExchange(config, ch)
  }

  override def bindQueue(config: BindQueueConfig): F[Unit] = withChannel { ch =>
    factory.Declarations.bindQueue(config, ch)
  }

  def withChannel[A](f: ServerChannel => F[A]): F[A] = {
    createChannel().use(f)
  }

  // prepare exchange for republishing
  private[rabbitmq] val setUpRepublishing: F[Unit] = {
    withChannel { channel =>
      republishStrategy match {
        case RepublishStrategyConfig.CustomExchange(exchangeName, exchangeDeclare, _) if exchangeDeclare =>
          factory.declareExchange(
            name = exchangeName,
            `type` = ExchangeType.Direct,
            durable = true,
            autoDelete = false,
            arguments = DeclareArgumentsConfig(),
            channel = channel,
          )(logger)

        case _ => Sync[F].unit // no-op
      }
    }
  }
}

object DefaultRabbitMQConnection {
  def make[F[_]](connection: ServerConnection,
                 info: RabbitMQConnectionInfo,
                 republishStrategy: RepublishStrategyConfig,
                 connectionListener: ConnectionListener[F],
                 channelListener: ChannelListener[F],
                 consumerListener: ConsumerListener[F])(implicit F: Dispatcher[F],
                                                        timer: Temporal[F],
                                                        async: Async[F]): F[DefaultRabbitMQConnection[F]] =
    Sync[F]
      .delay {
        new DefaultRabbitMQConnection(connection, info, republishStrategy, connectionListener, channelListener, consumerListener)
      }
      .flatTap { _.setUpRepublishing }

}
