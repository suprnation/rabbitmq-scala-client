package com.avast.clients.rabbitmq

import cats.syntax._
import cats.implicits._
import cats.effect.{Async, Concurrent, Deferred}
import com.avast.clients.rabbitmq.api.DeliveryResult

private[rabbitmq] trait ConfirmedDeliveryResult[F[_]] {
  def deliveryResult: DeliveryResult
  def confirm: F[Unit]
  def awaitConfirmation: F[Unit]
}

private[rabbitmq] object ConfirmedDeliveryResult {
  def apply[F[_]: Async](dr: DeliveryResult): ConfirmedDeliveryResult[F] = {
    new ConfirmedDeliveryResult[F] {
      private val deff = Deferred.unsafe[F, Unit]

      override val deliveryResult: DeliveryResult = dr

      override def confirm: F[Unit] = deff.complete(()).map(_ => ())
      override def awaitConfirmation: F[Unit] = deff.get
    }
  }
}
