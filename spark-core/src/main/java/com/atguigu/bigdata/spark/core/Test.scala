package com.atguigu.bigdata.spark.core

object Test {
  def main(args: Array[String]): Unit = {
//    println("hello scala");


//    var str : String = "ssss";
//    var amount : Int = 123;
//
//    val map = ("key",123)
//
//    println(str)
//    println(amount)
//    println(map)

//    var count : Int = _;
//    var name : String = _;

//    var tt = 3.3;
    ////
    ////    println(tt)


    val array = new Array[Int](5);


    val ints = array.:+(11)
//    val ints = array.+:(55)

//    array.foreach(println(_))
//    ints.foreach(println)

    println(array.mkString("--"))
    println(ints.mkString("--"))

    var list1 = 12 :: 13 :: 55 :: Nil

    var list2 = 666 :: "b" :: "v" :: Nil

//    println(list1)

//    list1.foreach(println(_));

    println(list1)
    println(list2)

    var list3= list1 ++ list2
    println(list3)

    println(list3==list1)
    println(list2==list1)
    println(list3==list2)

    val list = List(1, 2, 3, 4);

    val value = list.map(_ * 2)
    println(value)
    val value1 = list.map(x => x * x)
    println(value1)



  }
}
