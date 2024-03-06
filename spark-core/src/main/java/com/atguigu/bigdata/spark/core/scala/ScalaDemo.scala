package com.atguigu.bigdata.spark.core.scala

object ScalaDemo {
  def main(args: Array[String]): Unit = {
    //    listDemo();
    mapDemo();
  }


  private def mapDemo(): Unit = {
    var map = Map("a" -> 1, "b" -> 2, "c" -> 3);
    println(map);

    map.toArray.foreach(println)

    val list = List(1, 2, 3, 4)
    println(list.reduce(_+_))

    var tt = 1;
    tt match {
//      case "a" -> 1 => println("map one....")
//      case 2 => println("map two....")
      case _ => println("others....")

    }

  }


  /**
   * List 的方法demo
   */
  private def listDemo(): Unit = {

    var myList: List[Int] = List(1, 2, 3, 4);
    var beforeList: List[Int] = 0 :: myList;
    var afterList: List[Int] = myList :+ 5;


    println("原始List = " + myList)
    println("原始List前追加元素 = " + beforeList)
    println("原始List后追加元素 = " + afterList)


    val mergeList1 = List.concat(beforeList, afterList);
    println(mergeList1);
    val mergeList2 = beforeList ::: afterList
    println(mergeList2);

    val filterList = mergeList2.filter(_ > 2)
    println(filterList);

    val distinct = mergeList2.filter(_ > 2).distinct
    println(distinct);


    val dropList = distinct.drop(2)
    println(dropList);

    val dropRightList = distinct.dropRight(2)
    println(dropRightList);

    val droppedWhileList = distinct.dropWhile(_ < 5);
    println(droppedWhileList);
  }

}
