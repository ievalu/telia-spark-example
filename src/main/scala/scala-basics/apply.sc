// apply function makes the functions to behave as objects as well as functions
val doubleInt: Function[Int, Int] = (x: Int) => x * 2

// doubleInt is an object so we can call toString method on it
doubleInt.toString

// To execute the function, call apply method
doubleInt.apply(10)

// Scala compiler lets to use standard function invocation
doubleInt(10)
