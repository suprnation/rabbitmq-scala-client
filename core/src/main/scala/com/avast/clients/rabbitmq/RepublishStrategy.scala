package com.avast.clients.rabbitmq

import cats.effect.Sync
import cats.implicits.{catsSyntaxApplicativeError, catsSyntaxFlatMapOps, toFlatMapOps}
import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.logging.ImplicitContextLogger
import com.rabbitmq.client.AMQP.BasicProperties

import scala.util.{Left, Right}

trait RepublishStrategy[F[_]] {
  def republish(channel: ServerChannel, consumerName: String)(originalQueueName: String,
                                                                                properties: BasicProperties,
                                                                                rawBody: Bytes)(implicit dctx: DeliveryContext): F[Unit]
}

object RepublishStrategy {

  case class CustomExchange[F[_]: Sync](exchangeName: String) extends RepublishStrategy[F] {
    private val logger = ImplicitContextLogger.createLogger[F, CustomExchange[F]]

    def republish(channel: ServerChannel, consumerName: String)(
        originalQueueName: String,
        properties: BasicProperties,
        rawBody: Bytes)(implicit dctx: DeliveryContext): F[Unit] = {
      import dctx._

      logger.debug {
        s"[$consumerName] Republishing delivery ($messageId, $deliveryTag) to end of queue '$originalQueueName' through '$exchangeName'($originalQueueName)"
      } >>
        Sync[F]
          .delay {
            if (!channel.isOpen) throw new IllegalStateException("Cannot republish delivery on closed channel")
            channel.basicPublish(exchangeName, originalQueueName, properties, rawBody.toByteArray)
            channel.basicAck(deliveryTag.value, false)
          }
          .attempt
          .flatMap {
            case Right(()) => Sync[F].unit
            case Left(e) => logger.warn(e)(s"[$consumerName] Error while republishing the delivery $messageId")
          }
    }
  }

  case class DefaultExchange[F[_]: Sync]() extends RepublishStrategy[F] {
    private val logger = ImplicitContextLogger.createLogger[F, DefaultExchange[F]]

    def republish(channel: ServerChannel, consumerName: String)(
        originalQueueName: String,
        properties: BasicProperties,
        rawBody: Bytes)(implicit dctx: DeliveryContext): F[Unit] = {
      import dctx._

      logger.debug {
        s"[$consumerName] Republishing delivery ($messageId, $deliveryTag) to end of queue '$originalQueueName' (through default exchange)"
      } >>
        Sync[F]
          .delay {
            if (!channel.isOpen) throw new IllegalStateException("Cannot republish delivery on closed channel")
            channel.basicPublish("", originalQueueName, properties, rawBody.toByteArray)
            channel.basicAck(deliveryTag.value, false)
          }
          .attempt
          .flatMap {
            case Right(()) => Sync[F].unit
            case Left(e) => logger.warn(e)(s"[$consumerName] Error while republishing the delivery $messageId")
          }
    }
  }
}
