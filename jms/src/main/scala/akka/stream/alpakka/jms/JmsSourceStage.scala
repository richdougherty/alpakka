/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.jms

import java.util.concurrent.Semaphore
import javax.jms._

import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler, StageLogging}
import akka.stream.{ActorAttributes, Attributes, Outlet, SourceShape}

import scala.util.{Failure, Success}

final class JmsSourceStage(settings: JmsSourceSettings) extends GraphStage[SourceShape[Message]] {

  private val out = Outlet[Message]("JmsSource.out")

  override def shape: SourceShape[Message] = SourceShape[Message](out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with JmsConnector with StageLogging {

      override private[jms] def jmsSettings = settings

      private[jms] def getDispatcher =
        inheritedAttributes.get[ActorAttributes.Dispatcher](
          ActorAttributes.Dispatcher("akka.stream.default-blocking-io-dispatcher")
        ) match {
          case ActorAttributes.Dispatcher("") =>
            ActorAttributes.Dispatcher("akka.stream.default-blocking-io-dispatcher")
          case d => d
        }

      /**
       * Permits to control push volume. Available permits are equal to the
       * number of unfulfilled pulls.
       */
      private val pushPermits = new Semaphore(0)

      private val asyncFail = getAsyncCallback[Throwable](e => {
        fail(out, e)
      })

      private val asyncPush = getAsyncCallback[Message](msg => {
        push(out, msg)
      })

      override def preStart(): Unit =
        initSessionAsync(getDispatcher)

      override private[jms] def onSessionOpened(): Unit =
        jmsSession.createConsumer(settings.selector).onComplete {
          case Success(consumer) =>
            consumer.setMessageListener(new MessageListener {
              override def onMessage(message: Message): Unit = {
                pushPermits.acquire() // Acquire a permit to push this message
                try {
                  message.acknowledge()
                  asyncPush.invoke(message)
                } catch {
                  case e: JMSException =>
                    asyncFail.invoke(e)
                }
              }
            })
          case Failure(e) =>
            fail.invoke(e)
        }

      setHandler(out, new OutHandler {
        override def onPull(): Unit =
          pushPermits.release() // Release a permit to push a message
      })

      override def postStop(): Unit =
        Option(jmsSession).foreach(_.closeSessionAsync().failed.foreach {
          case e => log.error(e, "Error closing jms session")
        })
    }
}
