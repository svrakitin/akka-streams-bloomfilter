package com.github.svrakitin.akka.streams.bloomfilter

import akka.stream.Attributes.name
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import bloomfilter.{CanGenerateHashFrom, mutable}

final case class BloomFilter[T](numberOfItems: Long, falsePositiveRate: Double)(implicit hashFunc: (T) => Long) extends GraphStage[FlowShape[T, T]] {
  private val in = Inlet[T]("BloomFilter.in")
  private val out = Outlet[T]("BloomFilter.out")

  override def shape: FlowShape[T, T] = FlowShape(in, out)

  override def initialAttributes: Attributes = name("bloomFilter")

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    import BloomFilter._

    val bf: mutable.BloomFilter[T] = bloomfilter.mutable.BloomFilter[T](numberOfItems, falsePositiveRate)(hashFunc)

    setHandler(in, new InHandler {
      override def onPush(): Unit = {
        val elem = grab(in)
        if (!bf.mightContain(elem)) {
          bf.add(elem)
          push(out, elem)
        } else pull(in)
      }
    })
    setHandler(out, new OutHandler {
      override def onPull(): Unit = pull(in)
    })
  }

  override def toString = "BloomFilter"
}

object BloomFilter {
  private implicit def hashFuncConversion[T](hf: (T) => Long): CanGenerateHashFrom[T] = hf(_)
}
