package com.avast.clients.rabbitmq.api

import com.rabbitmq.client.AMQP


trait RabbitMQSender extends AutoCloseable {
  def send(routingKey: String, body: Array[Byte]): Unit

  def send(routingKey: String, body: Array[Byte], properties: AMQP.BasicProperties): Unit
}
