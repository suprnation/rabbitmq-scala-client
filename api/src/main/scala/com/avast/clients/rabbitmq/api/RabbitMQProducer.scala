package com.avast.clients.rabbitmq.api

import com.avast.bytes.Bytes
import mainecoon.autoFunctorK

import scala.language.higherKinds

@autoFunctorK(autoDerivation = false)
trait RabbitMQProducer[F[_]] {
  def send(routingKey: String, body: Bytes, properties: Option[MessageProperties] = None): F[Unit]
}
