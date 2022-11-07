package com.avast.clients.rabbitmq

import cats.effect._
import cats.effect.std.Dispatcher
import cats.implicits.{catsSyntaxFlatMapOps, toFunctorOps, toTraverseOps}
import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.DefaultRabbitMQClientFactory.startConsumingQueue
import com.avast.clients.rabbitmq.api._
import com.avast.clients.rabbitmq.logging.ImplicitContextLogger
//import com.avast.metrics.scalaeffectapi.Monitor
import com.rabbitmq.client.Consumer

import scala.collection.compat._
import scala.collection.immutable
import scala.jdk.CollectionConverters._
import scala.language.implicitConversions

private[rabbitmq] class DefaultRabbitMQClientFactory[F[_]: Async : Dispatcher](
    connection: RabbitMQConnection[F],
    connectionInfo: RabbitMQConnectionInfo,
    republishStrategy: RepublishStrategyConfig) {

  private type ArgumentsMap = Map[String, Any]

  object Producer {

    def create[A: ProductConverter](producerConfig: ProducerConfig /*, monitor: Monitor[F]*/): Resource[F, DefaultRabbitMQProducer[F, A]] = {
      prepareProducer[A](producerConfig, connection)
    }
  }

  object Consumer {

    def create[A: DeliveryConverter](
        consumerConfig: ConsumerConfig,
        consumerListener: ConsumerListener[F],
        readAction: DeliveryReadAction[F, A],
//        monitor: Monitor[F]
    ): Resource[F, DefaultRabbitMQConsumer[F, A]] = {
      prepareConsumer[A](consumerConfig, consumerListener, readAction)
    }
  }

  object PullConsumer {

    def create[A: DeliveryConverter](consumerConfig: PullConsumerConfig,
//                                     monitor: Monitor[F]
                                    ): Resource[F, DefaultRabbitMQPullConsumer[F, A]] = {

      preparePullConsumer(consumerConfig)
    }
  }

  object StreamingConsumer {

    def create[A: DeliveryConverter](consumerConfig: StreamingConsumerConfig,
//                                     monitor: Monitor[F],
                                     consumerListener: ConsumerListener[F]): Resource[F, DefaultRabbitMQStreamingConsumer[F, A]] = {

      prepareStreamingConsumer(consumerConfig, consumerListener)
    }
  }

  object Declarations {
    private val logger = ImplicitContextLogger.createLogger[F, DefaultRabbitMQClientFactory[F]]

    def declareExchange(config: DeclareExchangeConfig, channel: ServerChannel): F[Unit] = {
      import config._

      DefaultRabbitMQClientFactory.this.declareExchange(name, `type`, durable, autoDelete, arguments, channel)(logger)
    }

    def declareQueue(config: DeclareQueueConfig, channel: ServerChannel): F[Unit] = {
      import config._

      DefaultRabbitMQClientFactory.this.declareQueue(channel, name, durable, exclusive, autoDelete, arguments)(logger)
    }

    def bindQueue(config: BindQueueConfig, channel: ServerChannel): F[Unit] = {
      import config._

      DefaultRabbitMQClientFactory.this.bindQueue(channel, queueName, exchangeName, routingKeys, arguments)(logger)
    }

    def bindExchange(config: BindExchangeConfig, channel: ServerChannel): F[Unit] = {
      import config._

      routingKeys
        .map {
          DefaultRabbitMQClientFactory.this.bindExchange(channel, sourceExchangeName, destExchangeName, arguments.value)(_)(logger)
        }
        .sequence
        .as(())
    }
  }

  // scalastyle:off method.length
  private def prepareStreamingConsumer[A: DeliveryConverter](consumerConfig: StreamingConsumerConfig,
                                                             consumerListener: ConsumerListener[F]): Resource[F, DefaultRabbitMQStreamingConsumer[F, A]] = {
    import consumerConfig._

    val logger = ImplicitContextLogger.createLogger[F, DefaultRabbitMQStreamingConsumer[F, A]]

    Resource.eval(connection.withChannel { channel =>
      // auto declare exchanges
      declareExchangesFromBindings(channel, consumerConfig.bindings)(logger) >>
        // auto declare queue; if configured
        consumerConfig.declare.map { declareQueue(consumerConfig.queueName, channel, _)(logger) }.getOrElse(Sync[F].unit) >>
        // auto bind
        bindQueue(channel, consumerConfig.queueName, consumerConfig.bindings)(logger) >>
        bindQueueForRepublishing(channel, consumerConfig.queueName, republishStrategy)(logger)
    }) >>
      PoisonedMessageHandler
        .make[F, A](consumerConfig.poisonedMessageHandling, connection, redactPayload)
        .flatMap { pmh =>
          val base = new ConsumerBase[F, A](
            name,
            queueName,
            redactPayload,
            ImplicitContextLogger.createLogger[F, DefaultRabbitMQStreamingConsumer[F, A]]
          )

          val channelOpsFactory = new ConsumerChannelOpsFactory[F, A](
            name,
            queueName,
            republishStrategy.toRepublishStrategy[F],
            pmh,
            connectionInfo,
            ImplicitContextLogger.createLogger[F, DefaultRabbitMQStreamingConsumer[F, A]],
            connection.newChannel().evalTap(ch => Sync[F].delay(ch.basicQos(consumerConfig.prefetchCount)))
          )

          DefaultRabbitMQStreamingConsumer.make(
            base,
            channelOpsFactory,
            consumerTag,
            consumerListener,
            queueBufferSize,
            processTimeout,
            timeoutAction,
            timeoutLogLevel,
          )
        }
  }
  // scalastyle:on method.length

  // scalastyle:off method.length
  private def prepareConsumer[A: DeliveryConverter](consumerConfig: ConsumerConfig,
                                                    consumerListener: ConsumerListener[F],
                                                    readAction: DeliveryReadAction[F, A]): Resource[F, DefaultRabbitMQConsumer[F, A]] = {
    import consumerConfig._

    val logger = ImplicitContextLogger.createLogger[F, DefaultRabbitMQConsumer[F, A]]

    connection
      .newChannel()
      .evalTap { channel =>
        // auto declare exchanges
        declareExchangesFromBindings(channel, consumerConfig.bindings)(logger) >>
          // auto declare queue; if configured
          consumerConfig.declare.map { declareQueue(consumerConfig.queueName, channel, _)(logger) }.getOrElse(Sync[F].unit) >>
          // set prefetch size (per consumer)
          Sync[F].delay { channel.basicQos(consumerConfig.prefetchCount) } >>
          // auto bind
          bindQueue(channel, consumerConfig.queueName, consumerConfig.bindings)(logger) >>
          bindQueueForRepublishing(channel, consumerConfig.queueName, republishStrategy)(logger)
      }
      .flatMap { channel =>
        PoisonedMessageHandler
          .make[F, A](consumerConfig.poisonedMessageHandling, connection, redactPayload)
          .map { pmh =>
            val base = new ConsumerBase[F, A](
              name,
              queueName,
              redactPayload,
              logger
            )

            val channelOps = new ConsumerChannelOps[F, A](
              name,
              queueName,
              channel,
              republishStrategy.toRepublishStrategy[F],
              pmh,
              connectionInfo,
              ImplicitContextLogger.createLogger[F, DefaultRabbitMQStreamingConsumer[F, A]]
            )

            new DefaultRabbitMQConsumer[F, A](
              base,
              channelOps,
              processTimeout,
              timeoutAction,
              timeoutLogLevel,
              failureAction,
              consumerListener
            )(readAction)
          }
          .evalTap { consumer =>
            startConsumingQueue(channel, queueName, consumerTag, consumer)
          }
      }
  }
  // scalastyle:on method.length

  private def preparePullConsumer[A: DeliveryConverter](consumerConfig: PullConsumerConfig): Resource[F, DefaultRabbitMQPullConsumer[F, A]] = {
    import consumerConfig._

    val logger = ImplicitContextLogger.createLogger[F, DefaultRabbitMQPullConsumer[F, A]]

    connection
      .newChannel()
      .evalTap { channel =>
        // auto declare exchanges
        declareExchangesFromBindings(channel, consumerConfig.bindings)(logger) >>
          // auto declare queue; if configured
          declare.map { declareQueue(consumerConfig.queueName, channel, _)(logger) }.getOrElse(Sync[F].unit) >>
          // auto bind
          bindQueue(channel, consumerConfig.queueName, consumerConfig.bindings)(logger) >>
          bindQueueForRepublishing(channel, consumerConfig.queueName, republishStrategy)(logger)
      }
      .flatMap { channel =>
        PoisonedMessageHandler
          .make[F, A](consumerConfig.poisonedMessageHandling, connection, redactPayload)
          .map { pmh =>
            val base = new ConsumerBase[F, A](
              name,
              queueName,
              redactPayload,
              logger,
            )

            val channelOps = new ConsumerChannelOps[F, A](
              name,
              queueName,
              channel,
              republishStrategy.toRepublishStrategy[F],
              pmh,
              connectionInfo,
              ImplicitContextLogger.createLogger[F, DefaultRabbitMQStreamingConsumer[F, A]],
            )

            new DefaultRabbitMQPullConsumer[F, A](base, channelOps)
          }
      }
  }

  private def prepareProducer[A: ProductConverter](producerConfig: ProducerConfig,
                                                   connection: RabbitMQConnection[F],
//                                                   monitor: Monitor[F]
                                                  ): Resource[F, DefaultRabbitMQProducer[F, A]] = {
    val logger = ImplicitContextLogger.createLogger[F, DefaultRabbitMQProducer[F, A]]

    connection
      .newChannel()
      .evalTap { channel =>
        // auto declare exchange; if configured
        producerConfig.declare.map { declareExchange(producerConfig.exchange, channel, _)(logger) }.getOrElse(Sync[F].unit)
      }
      .map { channel =>
        val defaultProperties = MessageProperties(
          deliveryMode = DeliveryMode.fromCode(producerConfig.properties.deliveryMode),
          contentType = producerConfig.properties.contentType,
          contentEncoding = producerConfig.properties.contentEncoding,
          priority = producerConfig.properties.priority.map(Integer.valueOf)
        )

        new DefaultRabbitMQProducer[F, A](
          producerConfig.name,
          producerConfig.exchange,
          channel,
          defaultProperties,
          producerConfig.reportUnroutable,
          producerConfig.sizeLimitBytes,
          logger
        )
      }
  }

  private[rabbitmq] def declareExchange(name: String, channel: ServerChannel, autoDeclareExchange: AutoDeclareExchangeConfig,
  )(logger: ImplicitContextLogger[F]): F[Unit] = {
    import autoDeclareExchange._

    if (enabled) {
      declareExchange(name, `type`, durable, autoDelete, arguments, channel)(logger)
    } else Sync[F].unit

  }

  private[rabbitmq] def declareExchange(name: String,
                                        `type`: ExchangeType,
                                        durable: Boolean,
                                        autoDelete: Boolean,
                                        arguments: DeclareArgumentsConfig,
                                        channel: ServerChannel,
  )(logger: ImplicitContextLogger[F]): F[Unit] = {
    logger.plainInfo(s"Declaring exchange '$name' of type ${`type`} in virtual host '${connectionInfo.virtualHost}'") >>
      Sync[F].delay {
        val javaArguments = argsAsJava(arguments.value)
        channel.exchangeDeclare(name, `type`.value, durable, autoDelete, javaArguments)
        ()
      }
  }

  private def bindQueue(
      channel: ServerChannel,
      queueName: String,
      bindings: immutable.Seq[AutoBindQueueConfig],
  )(logger: ImplicitContextLogger[F]): F[Unit] = {
    bindings
      .map { bind =>
        import bind._
        val exchangeName = bind.exchange.name

        bindQueue(channel, queueName, exchangeName, routingKeys, bindArguments)(logger)
      }
      .sequence
      .as(())
  }

  private def bindQueue(channel: ServerChannel,
                        queueName: String,
                        exchangeName: String,
                        routingKeys: immutable.Seq[String],
                        bindArguments: BindArgumentsConfig,
  )(logger: ImplicitContextLogger[F]): F[Unit] = {
    if (routingKeys.nonEmpty) {
      routingKeys
        .map { bindQueue(channel, queueName)(exchangeName, _, bindArguments.value)(logger) }
        .sequence
        .as(())
    } else {
      // binding without routing key, possibly to fanout exchange

      bindQueue(channel, queueName)(exchangeName, "", bindArguments.value)(logger)
    }
  }

  private def bindQueueForRepublishing(
      channel: ServerChannel,
      queueName: String,
      strategyConfig: RepublishStrategyConfig,
  )(logger: ImplicitContextLogger[F]): F[Unit] = {
    import RepublishStrategyConfig._

    strategyConfig match {
      case CustomExchange(exchangeName, _, exchangeAutoBind) if exchangeAutoBind =>
        bindQueue(channel, queueName)(exchangeName, queueName, Map.empty)(logger)

      case _ => Sync[F].unit // no-op
    }
  }

  private def declareQueue(queueName: String, channel: ServerChannel, declare: AutoDeclareQueueConfig,
  )(logger: ImplicitContextLogger[F]): F[Unit] = {
    import declare._

    if (enabled) {
      declareQueue(channel, queueName, durable, exclusive, autoDelete, arguments)(logger)
    } else Sync[F].unit
  }

  private[rabbitmq] def declareQueue(channel: ServerChannel,
                                     queueName: String,
                                     durable: Boolean,
                                     exclusive: Boolean,
                                     autoDelete: Boolean,
                                     arguments: DeclareArgumentsConfig,
  )(logger: ImplicitContextLogger[F]): F[Unit] = {
    logger.plainInfo(s"Declaring queue '$queueName' in virtual host '${connectionInfo.virtualHost}'") >>
      Sync[F].delay {
        channel.queueDeclare(queueName, durable, exclusive, autoDelete, arguments.value)
        ()
      }
  }

  private[rabbitmq] def bindQueue(
      channel: ServerChannel,
      queueName: String)(exchangeName: String, routingKey: String, arguments: ArgumentsMap)(logger: ImplicitContextLogger[F]): F[Unit] = {
    logger.plainInfo(s"Binding exchange $exchangeName($routingKey) -> queue '$queueName' in virtual host '${connectionInfo.virtualHost}'") >>
      Sync[F].delay {
        channel.queueBind(queueName, exchangeName, routingKey, arguments)
        ()
      }
  }

  private[rabbitmq] def bindExchange(channel: ServerChannel, sourceExchangeName: String, destExchangeName: String, arguments: ArgumentsMap)(
      routingKey: String)(logger: ImplicitContextLogger[F]): F[Unit] = {
    logger.plainInfo(
      s"Binding exchange $sourceExchangeName($routingKey) -> exchange '$destExchangeName' in virtual host '${connectionInfo.virtualHost}'"
    ) >> Sync[F].delay {
      channel.exchangeBind(destExchangeName, sourceExchangeName, routingKey, arguments)
      ()
    }
  }

  private def declareExchangesFromBindings(channel: ServerChannel, bindings: Seq[AutoBindQueueConfig])(
      logger: ImplicitContextLogger[F]): F[Unit] = {
    bindings
      .map { bind =>
        import bind.exchange._

        // auto declare exchange; if configured
        declare
          .map {
            declareExchange(name, channel, _)(logger)
          }
          .getOrElse(Sync[F].unit)
      }
      .toList
      .sequence
      .as(())
  }

  private[rabbitmq] def convertDelivery[A: DeliveryConverter](d: Delivery[Bytes]): Delivery[A] = {
    d.flatMap { d =>
      implicitly[DeliveryConverter[A]].convert(d.body) match {
        case Right(a) => d.mapBody(_ => a)
        case Left(ce) => Delivery.MalformedContent(d.body, d.properties, d.routingKey, ce)
      }
    }
  }

  private implicit def argsAsJava(value: ArgumentsMap): java.util.Map[String, Object] = {
    value.view.mapValues(_.asInstanceOf[Object]).toMap.asJava
  }

}

object DefaultRabbitMQClientFactory {
  private[rabbitmq] def startConsumingQueue[F[_]: Sync](channel: ServerChannel,
                                                                      queueName: String,
                                                                      consumerTag: String,
                                                                      consumer: Consumer): F[Unit] = {
    Sync[F].delay {
      channel.setDefaultConsumer(consumer) // see `setDefaultConsumer` javadoc; this is possible because the channel is here exclusively for this consumer
      channel.basicConsume(queueName, false, if (consumerTag == "Default") "" else consumerTag, consumer)
    }
  }
}
