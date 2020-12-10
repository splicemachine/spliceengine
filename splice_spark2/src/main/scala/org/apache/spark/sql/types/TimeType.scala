package org.apache.spark.sql.types

import scala.math.Ordering
import scala.reflect.runtime.universe.typeTag

// The companion object and this class are separated so the companion object also subclasses
// this type. Otherwise, the companion object would be of type "TimeType$" in byte code.
// Defined with a private constructor so the companion object is the only possible instantiation.
class TimeType private() extends AtomicType {
  private[sql] type InternalType = Long

  @transient private[sql] lazy val tag = typeTag[InternalType]

  private[sql] val ordering = implicitly[Ordering[InternalType]]

  override def defaultSize: Int = 8

  private[spark] override def asNullable: TimeType = this
}

case object TimeType extends TimeType
