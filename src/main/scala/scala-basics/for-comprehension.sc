val seqInt  = Seq(1, 2, 3, 4, 5, 6)
val seqInt2 = Seq(10, 20, 30, 40, 50, 60)

val doubledInts = for {
  i <- seqInt
} yield i * 2

val doubledEvenInts = for {
  i <- seqInt if i % 2 == 0
} yield i * 2

val addTwoSeqInts = for {
  i1 <- seqInt
  i2 <- seqInt2
} yield i1 + i2
