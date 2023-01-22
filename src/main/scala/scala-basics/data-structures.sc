// Data structures

// Immutable data structures
val exampleSeq: Seq[Int]    = Seq(1, 2, 3)
val exampleList: Seq[Int]   = List(1, 2, 3)
val exampleList2: List[Int] = List(1, 2, 3)

val exampleSet: Set[Int] = Set(1, 1, 2, 3)

val exampleMap: Map[Int, String]  = Map((1 -> "1"), (2 -> "2"), (3 -> "3"))
val exampleMap2: Map[Int, String] = Map((1, "1"), (2, "2"), (3, "3"))

def thisTakesAllSeq(seq: Seq[Int])      = seq.map(_ + 1)
def thisTakesOnlyLists(list: List[Int]) = list.map(_ + 1)

// Can pass seq as well as list
thisTakesAllSeq(exampleSeq)

// Type miss match
//thisTakesOnlyLists(exampleSeq)

// Mutable data structures
val mutableSeq: scala.collection.mutable.Seq[Int] =
  scala.collection.mutable.Seq(1, 2, 3)

// Cannot assign immutable Seq to immutable one
// val immutableMutableSeq: Seq[Int] = scala.collection.mutable.Seq(1, 2, 3)

val mutableMap: scala.collection.mutable.Map[Int, String] =
  scala.collection.mutable.Map(1 -> "1", 2 -> "2", 3 -> "3")
