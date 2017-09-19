/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.jms

import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue, Semaphore, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean
import javax.jms._

import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler, StageLogging}
import akka.stream.{ActorAttributes, Attributes, Outlet, SourceShape}

import scala.annotation.tailrec
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

      @volatile private var shuttingDown: Boolean = false
      private val pulls = new Semaphore(0)
      private val pushing = new AtomicBoolean(false)
      private val pushQueue: BlockingQueue[Message] = new ArrayBlockingQueue[Message](settings.bufferSize)

      private val failAsync = getAsyncCallback[Throwable](e => {
        fail(out, e)
      })

      private val pushAllAsync = getAsyncCallback[Unit](unit => {
        @tailrec
        def loop(): Unit = if (!shuttingDown) {
          val msg: Message = pushQueue.poll(0, TimeUnit.MILLISECONDS)
          if (msg == null) {
            pushing.set(false)
          } else {
            push(out, msg)
            loop()
          }
        }
        loop()
      })

      override def preStart(): Unit =
        initSessionAsync(getDispatcher)

      override private[jms] def onSessionOpened(): Unit =
        jmsSession.createConsumer(settings.selector).onComplete {
          case Success(consumer) =>
            // Start a receive loop in the blocking dispatcher. We use MessageConsumer.receive
            // instead of a MessageListener callback because we need to block to control
            // backpressure and buffering and we should only block on a thread that we control.
            // If we use a MessageListener then we're running on an unknown thread controlled
            // by the JMS implementation and we shouldn't block that thread.
            ec.execute(new Runnable {
              override def run() =
                try {

                  /*
                   * A helper method that wraps a polling function. This method calls the
                   * polling function while periodically checking if we need to shut down.
                   *
                   * If the poll function returns successfully then it returns Some value;
                   * if we're shutting down then None is returned. Using Option values allows
                   * us to use nice for-comprehension.
                   */
                  @tailrec
                  def pollOrShutdown[T](pollFailedValue: T)(pollThunk: => T): Option[T] =
                    if (shuttingDown) None
                    else {
                      val result = pollThunk
                      if (result != pollFailedValue) Some(result) else pollOrShutdown(pollFailedValue)(pollThunk)
                    }

                  // How long to poll for
                  val PollLength: Long = 50

                  while (!shuttingDown) {
                    for {
                      // 1. Acquire a pull from the consumer, blocking until one is made.
                      _ <- pollOrShutdown(false) { pulls.tryAcquire(PollLength, TimeUnit.MILLISECONDS) }
                      // 2. Ask for a message, blocking until one is available.
                      msg <- pollOrShutdown[Message](null) { consumer.receive(PollLength) }
                      // 3. Push the message onto the queue, blocking if there isn't any space.
                      _ <- pollOrShutdown(false) { pushQueue.offer(msg, PollLength, TimeUnit.MILLISECONDS) }
                    } {
                      // 4. Acknowledge the message now that it's on the queue.
                      msg.acknowledge()
                      // 5. Start pushing, if necessary.
                      val notAlreadyPushing: Boolean = pushing.compareAndSet(false, true)
                      if (notAlreadyPushing) {
                        // As a small optimisation, check if the message we just added to the queue
                        // has already been pushed. This is possible if pushing was running at the
                        // moment we added the message to the pushQueue, but the pushing has now
                        // stopped. We can check this by looking to see if the pushQueue is empty.
                        if (!pushQueue.isEmpty) {
                          pushAllAsync.invoke(())
                        }
                      }
                    }
                  }
                } catch {
                  case e: JMSException =>
                    failAsync.invoke(e)
                }
            })
          case Failure(e) =>
            fail.invoke(e)
        }

      setHandler(out, new OutHandler {
        override def onPull(): Unit = pulls.release()
      })

      override def postStop(): Unit = {
        shuttingDown = true
        pushQueue.clear()
        Option(jmsSession).foreach(_.closeSessionAsync().failed.foreach {
          case e => log.error(e, "Error closing jms session")
        })
      }
    }
}
