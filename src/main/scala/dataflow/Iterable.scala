package dataflow

object DataFlowIterable {
  implicit def convert[A](i: Iterable[A]): DataFlowIterable[A] =
    new DataFlowIterable[A](i)
}

class DataFlowIterable[+A](val iterable: Iterable[A]) {
  
  // implicit conversion hint
  def dataflow: DataFlowIterable[A] = this
  
  def foreach[U](f: A => U @dataflow): Unit @dataflow = {
    val it = iterable.iterator
    while (it.hasNext)
      f( it.next )
  }
  
  def map[B](f: A => B @dataflow): Iterable[B] @dataflow = {
    var bs: List[B] = Nil
    val it = iterable.iterator
    while (it.hasNext)
      bs = f( it.next ) :: bs
    bs.reverse
  }
}