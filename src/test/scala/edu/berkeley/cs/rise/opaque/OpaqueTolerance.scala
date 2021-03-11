package edu.berkeley.cs.rise.opaque

import org.scalactic.TolerantNumerics
import org.scalactic.Equality

trait OpaqueTolerance {

  // Modify the behavior of === for Double and Array[Double] to use a numeric tolerance
  implicit val tolerantDoubleEquality = TolerantNumerics.tolerantDoubleEquality(1e-6)
  implicit val tolerantDoubleArrayEquality = equalityToArrayEquality[Double]

  def equalityToArrayEquality[A: Equality](): Equality[Array[A]] = {
    new Equality[Array[A]] {
      def areEqual(a: Array[A], b: Any): Boolean = {
        b match {
          case b: Array[_] =>
            (a.length == b.length
              && a.zip(b).forall { case (x, y) =>
                implicitly[Equality[A]].areEqual(x, y)
              })
          case _ => false
        }
      }
      override def toString: String = s"TolerantArrayEquality"
    }
  }
}
