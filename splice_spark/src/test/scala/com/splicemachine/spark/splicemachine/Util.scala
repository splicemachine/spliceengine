/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 */

package com.splicemachine.spark.splicemachine

object Util {
  import org.apache.spark.sql.functions.udf

  def word_count(input : String): Map[String, Int] = {
    val words = if (input.contains(",")) input.split(", ") else input.split(" ")
    words.foldLeft(Map.empty[String, Int]){
      (count, word) => count + (word -> (count.getOrElse(word, 0) + 1))
    }
  }

  val word_count_udf = udf( (x : String) => word_count(x))


  def cosine(word_counts1 : Map[String, Int], word_counts2 : Map[String, Int]) : Float = {
    val intersection = word_counts1.keySet.intersect(word_counts2.keySet)
    val numerator = intersection.foldLeft(0)( (sum : Int, word : String ) => sum + word_counts1(word) * word_counts2(word))
    
    val squares_sum = (sum : Int, count : Int) => sum + count*count
    val sum_squares1 = word_counts1.values.foldLeft(0)( squares_sum )
    val sum_squares2 = word_counts2.values.foldLeft(0)( squares_sum )

    val denominator = math.sqrt(sum_squares1) * math.sqrt(sum_squares2)
    if (denominator == 0)
      0
    else
      (numerator.toFloat / denominator).toFloat
  }

  val cosine_udf = udf( (word_counts1 : Map[String, Int], word_counts2 : Map[String, Int]) => cosine(word_counts1, word_counts2))

  def is_nan(a: String) : Boolean = {
    try {
      a.toFloat.isNaN
    } catch {
      case e: NumberFormatException => false
    }
  }

  def values_equal(a : String, b : String): Int = {
    if (a == b && a != null && !a.isEmpty && !is_nan(a))
      return 1
    else
      return 0
  }

  val values_equal_udf = udf( (a : String, b : String ) => values_equal(a, b))
}
