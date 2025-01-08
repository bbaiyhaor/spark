/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.plans

import java.util.Locale

import org.apache.spark.sql.catalyst.expressions.Attribute

object JoinType {
  def apply(typ: String): JoinType =
    typ.toLowerCase(Locale.ROOT).replace("_", "") match {
      // Inner Join又称为内连接，平日里书写时，用join或者Inner Join，
      // 它join的结果就是符合关联条件下的两张表数据的交集
      case "inner" => Inner
      // Full Outer Join，又称全外连接，平常使用的时候写full outer join或者full join都行，
      // 其作用是显示全右左表的全部数据，即便这部分数据不满足Join条件
      case "outer" | "full" | "fullouter" => FullOuter
      // Left Outer Join，又称左外连接，平常使用的时候写left outer join或者left join都行，
      // 其作用是显示全左表的数据，即便这部分数据不满足Join条件
      case "leftouter" | "left" => LeftOuter
      // Right Outer Join，又称右外连接，平常使用的时候写right outer join或者right join都行，
      // 与left outer join类似，其作用是显示全右表的数据，即便这部分数据不满足Join条件
      case "rightouter" | "right" => RightOuter
      // Left Semi Join，又名左半连接，算是一种优化策略式的join方式，
      // sparkSql的底层通常会将in操作优化为Left Semi Join
      case "leftsemi" | "semi" => LeftSemi
      // Left Anti Join，名为反左连接，其作用是显示没匹配的数据，其实就类似于not in，
      // 这也是一个经典的优化策略式的join方式，与left semi join相同，右表的数据也仅仅传到map阶段，
      // 并不会输出到最终的结果，其底层的优化与left semi join类似
      case "leftanti" | "anti" => LeftAnti
      // Cross Join，直译就是交叉Join，也就是所谓的笛卡尔积操作，笛卡尔积有很多实现的方法，比如非等值连接
      // 但是有个要注意的地方是，有时候并不会真的在底层做了笛卡尔积操作，因为sparkSql内部存有优化器，
      // 并且在非等值连接且条件允许的情况下，一般是对Broadcast Nested Loop Join进行调用，
      // 所以如何判断是否真的产生了笛卡尔积？我们查询执行计划的时候如果看到 CartesianProduct
      // 那就是底层真正采用了笛卡尔积操作。
      case "cross" => Cross
      case _ =>
        // 上述是spark的七种join方式，spark还有五种Join策略，它们分别是
        // 1、Broadcast Hash Join
        // 2、Shuffle Hash Join
        // 3、Sort Merge Join
        // 4、Cartesian Join
        // 5、Broadcast Nested Loop Join
        val supported = Seq(
          "inner",
          "outer",
          "full",
          "fullouter",
          "full_outer",
          "leftouter",
          "left",
          "left_outer",
          "rightouter",
          "right",
          "right_outer",
          "leftsemi",
          "left_semi",
          "semi",
          "leftanti",
          "left_anti",
          "anti",
          "cross"
        )

        throw new IllegalArgumentException(
          s"Unsupported join type '$typ'. " +
            "Supported join types include: " + supported.mkString(
              "'",
              "', '",
              "'"
            ) + "."
        )
    }
}

sealed abstract class JoinType {
  def sql: String
}

/** The explicitCartesian flag indicates if the inner join was constructed with
  * a CROSS join indicating a cartesian product has been explicitly requested.
  */
sealed abstract class InnerLike extends JoinType {
  def explicitCartesian: Boolean
}

case object Inner extends InnerLike {
  override def explicitCartesian: Boolean = false
  override def sql: String = "INNER"
}

case object Cross extends InnerLike {
  override def explicitCartesian: Boolean = true
  override def sql: String = "CROSS"
}

case object LeftOuter extends JoinType {
  override def sql: String = "LEFT OUTER"
}

case object RightOuter extends JoinType {
  override def sql: String = "RIGHT OUTER"
}

case object FullOuter extends JoinType {
  override def sql: String = "FULL OUTER"
}

case object LeftSemi extends JoinType {
  override def sql: String = "LEFT SEMI"
}

case object LeftAnti extends JoinType {
  override def sql: String = "LEFT ANTI"
}

case class ExistenceJoin(exists: Attribute) extends JoinType {
  override def sql: String = {
    // This join type is only used in the end of optimizer and physical plans, we will not
    // generate SQL for this join type
    throw new UnsupportedOperationException
  }
}

case class NaturalJoin(tpe: JoinType) extends JoinType {
  require(
    Seq(Inner, LeftOuter, RightOuter, FullOuter).contains(tpe),
    "Unsupported natural join type " + tpe
  )
  override def sql: String = "NATURAL " + tpe.sql
}

case class UsingJoin(tpe: JoinType, usingColumns: Seq[String])
    extends JoinType {
  require(
    Seq(Inner, LeftOuter, LeftSemi, RightOuter, FullOuter, LeftAnti, Cross)
      .contains(tpe),
    "Unsupported using join type " + tpe
  )
  override def sql: String = "USING " + tpe.sql
  override def toString: String =
    s"UsingJoin($tpe, ${usingColumns.mkString("[", ", ", "]")})"
}

object LeftExistence {
  def unapply(joinType: JoinType): Option[JoinType] = joinType match {
    case LeftSemi | LeftAnti => Some(joinType)
    case j: ExistenceJoin    => Some(joinType)
    case _                   => None
  }
}

object LeftSemiOrAnti {
  def unapply(joinType: JoinType): Option[JoinType] = joinType match {
    case LeftSemi | LeftAnti => Some(joinType)
    case _                   => None
  }
}

object AsOfJoinDirection {

  def apply(direction: String): AsOfJoinDirection = {
    direction.toLowerCase(Locale.ROOT) match {
      case "forward"  => Forward
      case "backward" => Backward
      case "nearest"  => Nearest
      case _ =>
        val supported = Seq("forward", "backward", "nearest")
        throw new IllegalArgumentException(
          s"Unsupported as-of join direction '$direction'. " +
            "Supported as-of join direction include: " + supported.mkString(
              "'",
              "', '",
              "'"
            ) + "."
        )
    }
  }
}

sealed abstract class AsOfJoinDirection

case object Forward extends AsOfJoinDirection
case object Backward extends AsOfJoinDirection
case object Nearest extends AsOfJoinDirection
