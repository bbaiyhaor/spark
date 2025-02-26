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

package org.apache.spark.sql.execution.adaptive

import java.util
import java.util.concurrent.LinkedBlockingQueue

import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

import org.apache.spark.SparkException
import org.apache.spark.broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ReturnAnswer}
import org.apache.spark.sql.catalyst.plans.physical.{
  Distribution,
  UnspecifiedDistribution
}
import org.apache.spark.sql.catalyst.rules.{PlanChangeLogger, Rule}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.catalyst.util.sideBySide
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec._
import org.apache.spark.sql.execution.bucketing.{
  CoalesceBucketsInJoin,
  DisableUnnecessaryBucketedScan
}
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.exchange._
import org.apache.spark.sql.execution.ui.{
  SparkListenerSQLAdaptiveExecutionUpdate,
  SparkListenerSQLAdaptiveSQLMetricUpdates,
  SQLPlanMetric
}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.{SparkFatalException, ThreadUtils}

/** A root node to execute the query plan adaptively. It splits the query plan
  * into independent stages and executes them in order according to their
  * dependencies. The query stage materializes its output at the end. When one
  * stage completes, the data statistics of the materialized output will be used
  * to optimize the remainder of the query.
  *
  * To create query stages, we traverse the query tree bottom up. When we hit an
  * exchange node, and if all the child query stages of this exchange node are
  * materialized, we create a new query stage for this exchange node. The new
  * stage is then materialized asynchronously once it is created.
  *
  * When one query stage finishes materialization, the rest query is
  * re-optimized and planned based on the latest statistics provided by all
  * materialized stages. Then we traverse the query plan again and create more
  * stages if possible. After all stages have been materialized, we execute the
  * rest of the plan.
  */
case class AdaptiveSparkPlanExec(
    inputPlan: SparkPlan,
    @transient context: AdaptiveExecutionContext,
    @transient preprocessingRules: Seq[Rule[SparkPlan]],
    @transient isSubquery: Boolean,
    @transient override val supportsColumnar: Boolean = false
) extends LeafExecNode {

  @transient private val lock = new Object()

  @transient private val logOnLevel: (=> String) => Unit =
    conf.adaptiveExecutionLogLevel match {
      case "TRACE" => logTrace(_)
      case "DEBUG" => logDebug(_)
      case "INFO"  => logInfo(_)
      case "WARN"  => logWarning(_)
      case "ERROR" => logError(_)
      case _       => logDebug(_)
    }

  @transient private val planChangeLogger = new PlanChangeLogger[SparkPlan]()

  // The logical plan optimizer for re-optimizing the current logical plan.
  // optimizer：对运行时物理计划进行重新优化的优化器，比如DynamicJoinSelection规则
  // 进行动态join调整，EliminateLimits规则剔除掉不必要的GlobalLimit计划
  @transient private val optimizer = new AQEOptimizer(
    conf,
    session.sessionState.adaptiveRulesHolder.runtimeOptimizerRules
  )

  // `EnsureRequirements` may remove user-specified repartition and assume the query plan won't
  // change its output partitioning. This assumption is not true in AQE. Here we check the
  // `inputPlan` which has not been processed by `EnsureRequirements` yet, to find out the
  // effective user-specified repartition. Later on, the AQE framework will make sure the final
  // output partitioning is not changed w.r.t the effective user-specified repartition.
  @transient private val requiredDistribution: Option[Distribution] =
    if (isSubquery) {
      // Subquery output does not need a specific output partitioning.
      Some(UnspecifiedDistribution)
    } else {
      AQEUtils.getRequiredDistribution(inputPlan)
    }

  // costEvaluator：物理计划开销计算器，如果经过重新优化的物理计划比原计划开销小，那么将会替换掉原物理计划，默认是SimpleCostEvaluator
  @transient private val costEvaluator =
    conf.getConf(SQLConf.ADAPTIVE_CUSTOM_COST_EVALUATOR_CLASS) match {
      case Some(className) =>
        CostEvaluator.instantiate(className, session.sparkContext.getConf)
      case _ =>
        SimpleCostEvaluator(
          conf.getConf(SQLConf.ADAPTIVE_FORCE_OPTIMIZE_SKEWED_JOIN)
        )
    }

  // A list of physical plan rules to be applied before creation of query stages. The physical
  // plan should reach a final status of query stages (i.e., no more addition or removal of
  // Exchange nodes) after running these rules.
  // queryStagePreparationRules：对AdaptiveSparkPlanExec持有的原物理计划做初始化的应用规则
  // 这里面核心的便是EnsureRequirements，它的作用就是在原物理计划链上插入shuffle物理计划
  @transient private val queryStagePreparationRules: Seq[Rule[SparkPlan]] = {
    // For cases like `df.repartition(a, b).select(c)`, there is no distribution requirement for
    // the final plan, but we do need to respect the user-specified repartition. Here we ask
    // `EnsureRequirements` to not optimize out the user-specified repartition-by-col to work
    // around this case.
    val ensureRequirements =
      EnsureRequirements(requiredDistribution.isDefined, requiredDistribution)
    // CoalesceBucketsInJoin can help eliminate shuffles and must be run before
    // EnsureRequirements
    Seq(
      CoalesceBucketsInJoin,
      RemoveRedundantProjects,
      ensureRequirements,
      AdjustShuffleExchangePosition,
      ValidateSparkPlan,
      ReplaceHashWithSortAgg,
      RemoveRedundantSorts,
      RemoveRedundantWindowGroupLimits,
      DisableUnnecessaryBucketedScan,
      OptimizeSkewedJoin(ensureRequirements)
    ) ++ context.session.sessionState.adaptiveRulesHolder.queryStagePrepRules
  }

  // A list of physical optimizer rules to be applied to a new stage before its execution. These
  // optimizations should be stage-independent.
  // queryStageOptimizerRules：在执行QueryStage前应用的一些规则，比如应用CoalesceShufflePartitions规则进行shuffle后的分区缩减
  // QueryStage的概念类似于rdd中的stage，以shuffle进行划分
  @transient private val queryStageOptimizerRules: Seq[Rule[SparkPlan]] = Seq(
    PlanAdaptiveDynamicPruningFilters(this),
    ReuseAdaptiveSubquery(context.subqueryCache),
    OptimizeSkewInRebalancePartitions,
    CoalesceShufflePartitions(context.session),
    // `OptimizeShuffleWithLocalRead` needs to make use of 'AQEShuffleReadExec.partitionSpecs'
    // added by `CoalesceShufflePartitions`, and must be executed after it.
    OptimizeShuffleWithLocalRead
  ) ++ context.session.sessionState.adaptiveRulesHolder.queryStageOptimizerRules

  // This rule is stateful as it maintains the codegen stage ID. We can't create a fresh one every
  // time and need to keep it in a variable.
  @transient private val collapseCodegenStagesRule: Rule[SparkPlan] =
    CollapseCodegenStages()

  // A list of physical optimizer rules to be applied right after a new stage is created. The input
  // plan to these rules has exchange as its root node.
  // postStageCreationRules：在创建完QueryStage后应用的一些规则，比如应用CollapseCodegenStages规则进行全阶段代码生成，插入一个WholeStageCodegenExec计划
  private def postStageCreationRules(outputsColumnar: Boolean) = Seq(
    ApplyColumnarRulesAndInsertTransitions(
      context.session.sessionState.columnarRules,
      outputsColumnar
    ),
    collapseCodegenStagesRule
  )

  private def optimizeQueryStage(
      plan: SparkPlan,
      isFinalStage: Boolean
  ): SparkPlan = {
    val optimized = queryStageOptimizerRules.foldLeft(plan) {
      case (latestPlan, rule) =>
        val applied = rule.apply(latestPlan)
        val result = rule match {
          case _: AQEShuffleReadRule if !applied.fastEquals(latestPlan) =>
            val distribution = if (isFinalStage) {
              // If `requiredDistribution` is None, it means `EnsureRequirements` will not optimize
              // out the user-specified repartition, thus we don't have a distribution requirement
              // for the final plan.
              requiredDistribution.getOrElse(UnspecifiedDistribution)
            } else {
              UnspecifiedDistribution
            }
            if (ValidateRequirements.validate(applied, distribution)) {
              applied
            } else {
              logDebug(
                s"Rule ${rule.ruleName} is not applied as it breaks the " +
                  "distribution requirement of the query plan."
              )
              latestPlan
            }
          case _ => applied
        }
        planChangeLogger.logRule(rule.ruleName, latestPlan, result)
        result
    }
    planChangeLogger.logBatch("AQE Query Stage Optimization", plan, optimized)
    optimized
  }

  // initialPlan：初始化后的物理计划，也就是应用完queryStagePreparationRules规则后的物理计划，这时计划链中已经添加到shuffle物理计划
  @transient val initialPlan = context.session.withActive {
    applyPhysicalRules(
      inputPlan,
      queryStagePreparationRules,
      Some((planChangeLogger, "AQE Preparations"))
    )
  }

  @volatile private var currentPhysicalPlan = initialPlan

  @volatile private var _isFinalPlan = false

  private var currentStageId = 0

  /** Return type for `createQueryStages`
    * @param newPlan
    *   the new plan with created query stages.
    * @param allChildStagesMaterialized
    *   whether all child stages have been materialized.
    * @param newStages
    *   the newly created query stages, including new reused query stages.
    */
  private case class CreateStageResult(
      newPlan: SparkPlan,
      allChildStagesMaterialized: Boolean,
      newStages: Seq[QueryStageExec]
  )

  def executedPlan: SparkPlan = currentPhysicalPlan

  def isFinalPlan: Boolean = _isFinalPlan

  override def conf: SQLConf = context.session.sessionState.conf

  override def output: Seq[Attribute] = inputPlan.output

  override def doCanonicalize(): SparkPlan = inputPlan.canonicalized

  override def resetMetrics(): Unit = {
    metrics.valuesIterator.foreach(_.reset())
    executedPlan.resetMetrics()
  }

  private def getExecutionId: Option[Long] = {
    Option(
      context.session.sparkContext.getLocalProperty(
        SQLExecution.EXECUTION_ID_KEY
      )
    )
      .map(_.toLong)
  }

  private lazy val shouldUpdatePlan: Boolean = {
    // There are two cases that should not update plan:
    // 1. When executing subqueries, we can't update the query plan in the UI as the
    //    UI doesn't support partial update yet. However, the subquery may have been
    //    optimized into a different plan and we must let the UI know the SQL metrics
    //    of the new plan nodes, so that it can track the valid accumulator updates later
    //    and display SQL metrics correctly.
    // 2. If the `QueryExecution` does not match the current execution ID, it means the execution
    //    ID belongs to another (parent) query, and we should not call update UI in this query.
    //    e.g., a nested `AdaptiveSparkPlanExec` in `InMemoryTableScanExec`.
    //
    // That means only the root `AdaptiveSparkPlanExec` of the main query that triggers this
    // query execution need to do a plan update for the UI.
    !isSubquery && getExecutionId.exists(
      SQLExecution.getQueryExecution(_) eq context.qe
    )
  }

  def finalPhysicalPlan: SparkPlan = withFinalPlanUpdate(identity)

  private def getFinalPhysicalPlan(): SparkPlan = lock.synchronized {
    if (isFinalPlan) return currentPhysicalPlan

    // In case of this adaptive plan being executed out of `withActive` scoped functions, e.g.,
    // `plan.queryExecution.rdd`, we need to set active session here as new plan nodes can be
    // created in the middle of the execution.
    context.session.withActive {
      val executionId = getExecutionId
      // Use inputPlan logicalLink here in case some top level physical nodes may be removed
      // during `initialPlan`
      var currentLogicalPlan = inputPlan.logicalLink.get
      // 在getFinalPhysicalPlan会将currentPhysicalPlan传给createQueryStages方法
      // 这个方法的输出类型是 CreateStageResult，这个方法会从下到上递归的遍历物理计划树
      // 生成新的Querystage，这个 createQueryStages 方法在每次计划发生变化时都会被调用
      // 首次创建QueryStage，第一个需要shuffle的stage，并非是把全部的stage都创建出来
      var result = createQueryStages(currentPhysicalPlan)
      val events = new LinkedBlockingQueue[StageMaterializationEvent]()
      val errors = new mutable.ArrayBuffer[Throwable]()
      var stagesToReplace = Seq.empty[QueryStageExec]
      // [1] 是否所有的孩子stage都已经被物化
      // 接下来有哪些Stage要执行，参考 createQueryStages(plan: SparkPlan) 方法
      // 在所有子stage物化前，逐步的创建stage，物化stage
      while (!result.allChildStagesMaterialized) {
        // 替换物理计划为创建stage之后的物理计划，这会在原物理计划链上插入QueryStageExec计划等
        currentPhysicalPlan = result.newPlan
        if (result.newStages.nonEmpty) {
          // [2] 通知监听器物理计划已经变更
          stagesToReplace = result.newStages ++ stagesToReplace
          // onUpdatePlan 通过listener更新UI
          executionId.foreach(onUpdatePlan(_, result.newStages.map(_.plan)))

          // SPARK-33933: we should submit tasks of broadcast stages first, to avoid waiting
          // for tasks to be scheduled and leading to broadcast timeout.
          // This partial fix only guarantees the start of materialization for BroadcastQueryStage
          // is prior to others, but because the submission of collect job for broadcasting is
          // running in another thread, the issue is not completely resolved.
          // [3] 先提交广播阶段的任务，避免等待任务并导致广播超时
          val reorderedNewStages = result.newStages
            .sortWith {
              case (_: BroadcastQueryStageExec, _: BroadcastQueryStageExec) =>
                false
              case (_: BroadcastQueryStageExec, _) => true
              case _                               => false
            }

          // Start materialization of all new stages and fail fast if any stages failed eagerly
          // [4] 等待下一个完成的stage，这表明新的统计数据可用，并且可能可以创建新的阶段。
          // 物化stage，也就是实际执行stage，以job的方式进行提交执行
          reorderedNewStages.foreach { stage =>
            try {
              // materialize() 方法对Stage的作为一个单独的Job提交执行，并返回 SimpleFutureAction 来接收执行结果
              // QueryStageExec: materialize() -> doMaterialize() ->
              // ShuffleExchangeExec: -> mapOutputStatisticsFuture -> ShuffleExchangeExec
              // SparkContext: -> submitMapStage(shuffleDependency)
              stage
                .materialize()
                .onComplete { res =>
                  if (res.isSuccess) {
                    events.offer(StageSuccess(stage, res.get))
                  } else {
                    events.offer(StageFailure(stage, res.failed.get))
                  }
                  // explicitly clean up the resources in this stage
                  stage.cleanupResources()
                }(AdaptiveSparkPlanExec.executionContext)
            } catch {
              case e: Throwable =>
                cleanUpAndThrowException(Seq(e), Some(stage.id))
            }
          }
        }

        // Wait on the next completed stage, which indicates new stats are available and probably
        // new stages can be created. There might be other stages that finish at around the same
        // time, so we process those stages too in order to reduce re-planning.
        // 等待，直到有Stage执行完毕
        val nextMsg = events.take()
        val rem = new util.ArrayList[StageMaterializationEvent]()
        events.drainTo(rem)
        // 将stage的执行结果给这个 QueryStageExec ，这个结果是 MapOutputStatistics
        // 统计的是map端对reduce端每个分区的shuffle数据大小
        (Seq(nextMsg) ++ rem.asScala).foreach {
          case StageSuccess(stage, res) =>
            stage.resultOption.set(Some(res))
          case StageFailure(stage, ex) =>
            errors.append(ex)
        }

        // In case of errors, we cancel all running stages and throw exception.
        if (errors.nonEmpty) {
          cleanUpAndThrowException(errors.toSeq, None)
        }

        // Try re-optimizing and re-planning. Adopt the new plan if its cost is equal to or less
        // than that of the current plan; otherwise keep the current physical plan together with
        // the current logical plan since the physical plan's logical links point to the logical
        // plan it has originated from.
        // Meanwhile, we keep a list of the query stages that have been created since last plan
        // update, which stands for the "semantic gap" between the current logical and physical
        // plans. And each time before re-planning, we replace the corresponding nodes in the
        // current logical plan with logical query stages to make it semantically in sync with
        // the current physical plan. Once a new plan is adopted and both logical and physical
        // plans are updated, we can clear the query stage list because at this point the two plans
        // are semantically and physically in sync again.
        // [5] 尝试重新优化和重新规划。如果新计划的成本小于或者等于当前的计划就采用新计划
        // 对前面的Stage替换为 LogicalQueryStage 节点
        val logicalPlan = replaceWithQueryStagesInLogicalPlan(
          currentLogicalPlan,
          stagesToReplace
        )
        val afterReOptimize = reOptimize(logicalPlan)
        if (afterReOptimize.isDefined) {
          val (newPhysicalPlan, newLogicalPlan) = afterReOptimize.get
          val origCost = costEvaluator.evaluateCost(currentPhysicalPlan)
          val newCost = costEvaluator.evaluateCost(newPhysicalPlan)
          if (
            newCost < origCost ||
            (newCost == origCost && currentPhysicalPlan != newPhysicalPlan)
          ) {
            logOnLevel(
              "Plan changed:\n" +
                sideBySide(
                  currentPhysicalPlan.treeString,
                  newPhysicalPlan.treeString
                ).mkString("\n")
            )
            cleanUpTempTags(newPhysicalPlan)
            currentPhysicalPlan = newPhysicalPlan
            currentLogicalPlan = newLogicalPlan
            stagesToReplace = Seq.empty[QueryStageExec]
          }
        }
        // Now that some stages have finished, we can try creating new stages.
        // [6] 现在一些stage已经结束了，我们可以创建新的阶段。
        result = createQueryStages(currentPhysicalPlan)
      }

      // Run the final plan when there's no more unfinished stages.
      // 所有前置stage全部执行完毕，根据stats信息优化物理执行计划，确定最终的 physical plan
      // 等到前面的QueryStage（RDD层面来说就是ShuffleMapStage）都物化完了
      // 这时才会确定下来最终的物理计划，这时也就剩下最后一个stage了，也就是ResultStage
      // ResultStage无需再进行专门创建，因为从最后一个物理计划到上一个ShuffleQueryStageExec这个过程便是ResultStage了
      // 只需要去应用一些规则进行优化即可，同样也会应用之前创建stage时应用的规则，比如CoalesceShufflePartitions和CollapseCodegenStages
      currentPhysicalPlan = applyPhysicalRules(
        optimizeQueryStage(result.newPlan, isFinalStage = true),
        postStageCreationRules(supportsColumnar),
        Some((planChangeLogger, "AQE Post Stage Creation"))
      )
      _isFinalPlan = true
      executionId.foreach(onUpdatePlan(_, Seq(currentPhysicalPlan)))
      currentPhysicalPlan
    }
  }

  // Use a lazy val to avoid this being called more than once.
  @transient private lazy val finalPlanUpdate: Unit = {
    // Subqueries that don't belong to any query stage of the main query will execute after the
    // last UI update in `getFinalPhysicalPlan`, so we need to update UI here again to make sure
    // the newly generated nodes of those subqueries are updated.
    if (shouldUpdatePlan && currentPhysicalPlan.exists(_.subqueries.nonEmpty)) {
      getExecutionId.foreach(onUpdatePlan(_, Seq.empty))
    }
    logOnLevel(s"Final plan:\n$currentPhysicalPlan")
  }

  override def executeCollect(): Array[InternalRow] = {
    withFinalPlanUpdate(_.executeCollect())
  }

  override def executeTake(n: Int): Array[InternalRow] = {
    withFinalPlanUpdate(_.executeTake(n))
  }

  override def executeTail(n: Int): Array[InternalRow] = {
    withFinalPlanUpdate(_.executeTail(n))
  }

  override def doExecute(): RDD[InternalRow] = {
    withFinalPlanUpdate(_.execute())
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    withFinalPlanUpdate(_.executeColumnar())
  }

  override def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    withFinalPlanUpdate { finalPlan =>
      assert(finalPlan.isInstanceOf[BroadcastQueryStageExec])
      finalPlan.doExecuteBroadcast()
    }
  }

  private def withFinalPlanUpdate[T](fun: SparkPlan => T): T = {
    val plan = getFinalPhysicalPlan()
    // 最后开始执行这个物理计划，不过因为这个物理计划前面的stage都被执行过了
    // 在实际执行过程中前面的stage都会被跳过，实际直接执行最后一个stage就行了
    val result = fun(plan)
    finalPlanUpdate
    result
  }

  protected override def stringArgs: Iterator[Any] = Iterator(
    s"isFinalPlan=$isFinalPlan"
  )

  override def generateTreeString(
      depth: Int,
      lastChildren: Seq[Boolean],
      append: String => Unit,
      verbose: Boolean,
      prefix: String = "",
      addSuffix: Boolean = false,
      maxFields: Int,
      printNodeId: Boolean,
      indent: Int = 0
  ): Unit = {
    super.generateTreeString(
      depth,
      lastChildren,
      append,
      verbose,
      prefix,
      addSuffix,
      maxFields,
      printNodeId,
      indent
    )
    if (currentPhysicalPlan.fastEquals(initialPlan)) {
      currentPhysicalPlan.generateTreeString(
        depth + 1,
        lastChildren :+ true,
        append,
        verbose,
        prefix = "",
        addSuffix = false,
        maxFields,
        printNodeId,
        indent
      )
    } else {
      generateTreeStringWithHeader(
        if (isFinalPlan) "Final Plan" else "Current Plan",
        currentPhysicalPlan,
        depth,
        append,
        verbose,
        maxFields,
        printNodeId
      )
      generateTreeStringWithHeader(
        "Initial Plan",
        initialPlan,
        depth,
        append,
        verbose,
        maxFields,
        printNodeId
      )
    }
  }

  private def generateTreeStringWithHeader(
      header: String,
      plan: SparkPlan,
      depth: Int,
      append: String => Unit,
      verbose: Boolean,
      maxFields: Int,
      printNodeId: Boolean
  ): Unit = {
    append("   " * depth)
    append(s"+- == $header ==\n")
    plan.generateTreeString(
      0,
      Nil,
      append,
      verbose,
      prefix = "",
      addSuffix = false,
      maxFields,
      printNodeId,
      indent = depth + 1
    )
  }

  override def hashCode(): Int = inputPlan.hashCode()

  override def equals(obj: Any): Boolean = {
    if (!obj.isInstanceOf[AdaptiveSparkPlanExec]) {
      return false
    }

    this.inputPlan == obj.asInstanceOf[AdaptiveSparkPlanExec].inputPlan
  }

  /** This method is called recursively to traverse the plan tree bottom-up and
    * create a new query stage or try reusing an existing stage if the current
    * node is an [[Exchange]] node and all of its child stages have been
    * materialized.
    *
    * With each call, it returns: 1) The new plan replaced with
    * [[QueryStageExec]] nodes where new stages are created. 2) Whether the
    * child query stages (if any) of the current node have all been
    * materialized. 3) A list of the new query stages that have been created.
    */
  // createQueryStages会从下到上遍历并应用物理计划树的所有节点
  // 根据节点的类型不同，采取不同处理
  private def createQueryStages(plan: SparkPlan): CreateStageResult =
    plan match {
      // 对于Exchange类型的节点，exchange会被 QueryStageExec 节点替代。
      // 否则，从下向上迭代，如果孩子节点都迭代完成，将基于broadcast转换为BroadcastQueryStageExec，shuffle作为ShuffleQueryStageExec，并将其依次封装为CreateStageResult。
      // 在遇到Exchange节点时，判断是否应该创建QueryStage
      case e: Exchange =>
        // First have a quick check in the `stageCache` without having to traverse down the node.
        // 如果开启了stageCache
        context.stageCache.get(e.canonicalized) match {
          // 同时exchange节点是存在的stage，则直接重用stage作为QueryStage, 并封装返回CreateStageResult。
          case Some(existingStage) if conf.exchangeReuseEnabled =>
            val stage = reuseQueryStage(existingStage, e)
            val isMaterialized = stage.isMaterialized
            CreateStageResult(
              newPlan = stage,
              allChildStagesMaterialized = isMaterialized,
              newStages = if (isMaterialized) Seq.empty else Seq(stage)
            )
          case _ =>
            // 否则，从下向上迭代
            val result = createQueryStages(e.child)
            val newPlan =
              e.withNewChildren(Seq(result.newPlan)).asInstanceOf[Exchange]
            // Create a query stage only when all the child query stages are ready.
            // 如果孩子节点都迭代完成，在newQueryStage函数中，
            // 将broadcast转换为 BroadcastQueryStageExec、
            // shuffle转换为ShuffleQueryStageExec，
            // 并将其依次封装为CreateStageResult。
            // 需要保证子stage已被物化，才会进行新stage的创建
            if (result.allChildStagesMaterialized) {
              // 创建新的QueryStage，这会插入一个QueryStageExec计划
              var newStage =
                newQueryStage(newPlan).asInstanceOf[ExchangeQueryStageExec]
              if (conf.exchangeReuseEnabled) {
                // Check the `stageCache` again for reuse. If a match is found, ditch the new stage
                // and reuse the existing stage found in the `stageCache`, otherwise update the
                // `stageCache` with the new stage.
                val queryStage = context.stageCache.getOrElseUpdate(
                  newStage.plan.canonicalized,
                  newStage
                )
                if (queryStage.ne(newStage)) {
                  newStage = reuseQueryStage(queryStage, e)
                }
              }
              val isMaterialized = newStage.isMaterialized
              CreateStageResult(
                newPlan = newStage,
                allChildStagesMaterialized = isMaterialized,
                newStages = if (isMaterialized) Seq.empty else Seq(newStage)
              )
            } else {
              // 若子stage未被物化，便不会进行创建stage，而是返回下层的stage
              CreateStageResult(
                newPlan = newPlan,
                allChildStagesMaterialized = false,
                newStages = result.newStages
              )
            }
        }

      case i: InMemoryTableScanExec =>
        // There is no reuse for `InMemoryTableScanExec`, which is different from `Exchange`. If we
        // hit it the first time, we should always create a new query stage.
        val newStage = newQueryStage(i)
        CreateStageResult(
          newPlan = newStage,
          allChildStagesMaterialized = false,
          newStages = Seq(newStage)
        )

      // 遇到QueryStageExec时，说明这是之前已经创建过的stage节点，就不需再向下递归了，从此处返回
      case q: QueryStageExec =>
        CreateStageResult(
          newPlan = q,
          allChildStagesMaterialized = q.isMaterialized,
          newStages = Seq.empty
        )

      // 如果是常规节点，递归向下进行遍历，直到叶子节点，返回CreateStageResult
      case _ =>
        if (plan.children.isEmpty) {
          CreateStageResult(
            newPlan = plan,
            allChildStagesMaterialized = true,
            newStages = Seq.empty
          )
        } else {
          val results = plan.children.map(createQueryStages)
          CreateStageResult(
            newPlan = plan.withNewChildren(results.map(_.newPlan)),
            allChildStagesMaterialized =
              results.forall(_.allChildStagesMaterialized),
            newStages = results.flatMap(_.newStages)
          )
        }
    }

  // 在创建QueryStageExec计划的同时，也会应用一些规则对整个stage进行优化
  // 比如在创建stage前应用CoalesceShufflePartitions规则进行shuffle后的分区缩减
  // 这会在上一个shuffle/ShuffleQueryStageExec计划之后、这个stage的起点处增加
  // 一个AQEShuffleReadExec计划；在创建stage后应用CollapseCodegenStages规则
  // 进行全阶段代码生成，插入一个WholeStageCodegenExec计划。QueryStageExec属于
  // 整个物理计划的一部分，那么整个物理计划也随之改变了
  private def newQueryStage(plan: SparkPlan): QueryStageExec = {
    val queryStage = plan match {
      case e: Exchange =>
        // 应用queryStageOptimizerRules进行物理计划优化，比如缩减shuffle分区
        val optimized = e.withNewChildren(
          Seq(optimizeQueryStage(e.child, isFinalStage = false))
        )
        // 应用postStageCreationRules进行优化，比如全阶段代码生成
        val newPlan = applyPhysicalRules(
          optimized,
          postStageCreationRules(outputsColumnar = plan.supportsColumnar),
          Some((planChangeLogger, "AQE Post Stage Creation"))
        )
        if (e.isInstanceOf[ShuffleExchangeLike]) {
          if (!newPlan.isInstanceOf[ShuffleExchangeLike]) {
            throw SparkException.internalError(
              "Custom columnar rules cannot transform shuffle node to something else."
            )
          }
          // 在Exchange之上插入一个ShuffleQueryStageExec计划，意味着QueryStage的创建
          ShuffleQueryStageExec(currentStageId, newPlan, e.canonicalized)
        } else {
          assert(e.isInstanceOf[BroadcastExchangeLike])
          if (!newPlan.isInstanceOf[BroadcastExchangeLike]) {
            throw SparkException.internalError(
              "Custom columnar rules cannot transform broadcast node to something else."
            )
          }
          BroadcastQueryStageExec(currentStageId, newPlan, e.canonicalized)
        }
      case i: InMemoryTableScanExec =>
        // Apply `queryStageOptimizerRules` so that we can reuse subquery.
        // No need to apply `postStageCreationRules` for `InMemoryTableScanExec`
        // as it's a leaf node.
        val newPlan = optimizeQueryStage(i, isFinalStage = false)
        if (!newPlan.isInstanceOf[InMemoryTableScanExec]) {
          throw SparkException.internalError(
            "Custom AQE rules cannot transform table scan node to something else."
          )
        }
        TableCacheQueryStageExec(currentStageId, newPlan)
    }
    currentStageId += 1
    setLogicalLinkForNewQueryStage(queryStage, plan)
    queryStage
  }

  private def reuseQueryStage(
      existing: ExchangeQueryStageExec,
      exchange: Exchange
  ): ExchangeQueryStageExec = {
    val queryStage = existing.newReuseInstance(currentStageId, exchange.output)
    currentStageId += 1
    setLogicalLinkForNewQueryStage(queryStage, exchange)
    queryStage
  }

  /** Set the logical node link of the `stage` as the corresponding logical node
    * of the `plan` it encloses. If an `plan` has been transformed from a
    * `Repartition`, it should have `logicalLink` available by itself; otherwise
    * traverse down to find the first node that is not generated by
    * `EnsureRequirements`.
    */
  private def setLogicalLinkForNewQueryStage(
      stage: QueryStageExec,
      plan: SparkPlan
  ): Unit = {
    val link = plan
      .getTagValue(TEMP_LOGICAL_PLAN_TAG)
      .orElse(plan.logicalLink.orElse(plan.collectFirst {
        case p if p.getTagValue(TEMP_LOGICAL_PLAN_TAG).isDefined =>
          p.getTagValue(TEMP_LOGICAL_PLAN_TAG).get
        case p if p.logicalLink.isDefined => p.logicalLink.get
      }))
    assert(link.isDefined)
    stage.setLogicalLink(link.get)
  }

  /** For each query stage in `stagesToReplace`, find their corresponding
    * logical nodes in the `logicalPlan` and replace them with new
    * [[LogicalQueryStage]] nodes.
    *   1. If the query stage can be mapped to an integral logical sub-tree,
    *      replace the corresponding logical sub-tree with a leaf node
    *      [[LogicalQueryStage]] referencing this query stage. For example: Join
    *      SMJ SMJ / \ / \ / \ r1 r2 => Xchg1 Xchg2 => Stage1 Stage2 \| | r1 r2
    *      The updated plan node will be: Join / \ LogicalQueryStage1(Stage1)
    *      LogicalQueryStage2(Stage2)
    *
    * 2. Otherwise (which means the query stage can only be mapped to part of a
    * logical sub-tree), replace the corresponding logical sub-tree with a leaf
    * node [[LogicalQueryStage]] referencing to the top physical node into which
    * this logical node is transformed during physical planning. For example:
    * Agg HashAgg HashAgg \| | | child => Xchg => Stage1 \| HashAgg \| child The
    * updated plan node will be: LogicalQueryStage(HashAgg - Stage1)
    */
  private def replaceWithQueryStagesInLogicalPlan(
      plan: LogicalPlan,
      stagesToReplace: Seq[QueryStageExec]
  ): LogicalPlan = {
    var logicalPlan = plan
    stagesToReplace.foreach {
      // 对于需要替换的每一个查询阶段（stage），如果这个阶段在当前的物理计划中存在，那么就进行替换操作。否则，忽略这个查询阶段。
      case stage if currentPhysicalPlan.exists(_.eq(stage)) =>
        // 进行替换操作时，首先会从查询阶段中获取对应的逻辑节点（Logical Node）
        // 这个逻辑节点可以是原始的逻辑计划，也可以是之前标记（Tag）过的临时逻辑计划
        // 同时，它也会找到当前物理计划中对应的物理节点（Physical Node）
        val logicalNodeOpt =
          stage.getTagValue(TEMP_LOGICAL_PLAN_TAG).orElse(stage.logicalLink)
        assert(logicalNodeOpt.isDefined)
        val logicalNode = logicalNodeOpt.get
        val physicalNode = currentPhysicalPlan.collectFirst {
          case p
              if p.eq(stage) ||
                p.getTagValue(TEMP_LOGICAL_PLAN_TAG).exists(logicalNode.eq) ||
                p.logicalLink.exists(logicalNode.eq) =>
            p
        }
        assert(physicalNode.isDefined)
        // Set the temp link for those nodes that are wrapped inside a `LogicalQueryStage` node for
        // they will be shared and reused by different physical plans and their usual logical links
        // can be overwritten through re-planning processes.
        // LogicalQueryStage 是 QueryStageExec的逻辑计划包装，包含QueryStageExec的物理计划片段
        // QueryStageExec的所有祖先节点都链接到同一逻辑节点
        setTempTagRecursive(physicalNode.get, logicalNode)
        // Replace the corresponding logical node with LogicalQueryStage
        // 创建一个LogicalQueryStage节点，这个节点将物理节点和逻辑节点关联起来
        // 然后，将原始逻辑计划中对应的逻辑节点替换为这个LogicalQueryStage节点
        // 这样，逻辑计划中就包含了查询阶段的信息，可以在后续的优化和执行过程中使用
        val newLogicalNode = LogicalQueryStage(logicalNode, physicalNode.get)
        val newLogicalPlan = logicalPlan.transformDown {
          case p if p.eq(logicalNode) => newLogicalNode
        }
        logicalPlan = newLogicalPlan

      case _ => // Ignore those earlier stages that have been wrapped in later stages.
    }
    logicalPlan
  }

  /** Re-optimize and run physical planning on the current logical plan based on
    * the latest stats.
    */
  private def reOptimize(
      logicalPlan: LogicalPlan
  ): Option[(SparkPlan, LogicalPlan)] = {
    try {
      logicalPlan.invalidateStatsCache()
      val optimized = optimizer.execute(logicalPlan)
      val sparkPlan = context.session.sessionState.planner
        .plan(ReturnAnswer(optimized))
        .next()
      val newPlan = applyPhysicalRules(
        sparkPlan,
        preprocessingRules ++ queryStagePreparationRules,
        Some((planChangeLogger, "AQE Replanning"))
      )

      // When both enabling AQE and DPP, `PlanAdaptiveDynamicPruningFilters` rule will
      // add the `BroadcastExchangeExec` node manually in the DPP subquery,
      // not through `EnsureRequirements` rule. Therefore, when the DPP subquery is complicated
      // and need to be re-optimized, AQE also need to manually insert the `BroadcastExchangeExec`
      // node to prevent the loss of the `BroadcastExchangeExec` node in DPP subquery.
      // Here, we also need to avoid to insert the `BroadcastExchangeExec` node when the newPlan is
      // already the `BroadcastExchangeExec` plan after apply the `LogicalQueryStageStrategy` rule.
      val finalPlan = inputPlan match {
        case b: BroadcastExchangeLike
            if (!newPlan.isInstanceOf[BroadcastExchangeLike]) =>
          b.withNewChildren(Seq(newPlan))
        case _ => newPlan
      }

      Some((finalPlan, optimized))
    } catch {
      case e: InvalidAQEPlanException[_] =>
        logOnLevel(s"Re-optimize - ${e.getMessage()}:\n${e.plan}")
        None
    }
  }

  /** Recursively set `TEMP_LOGICAL_PLAN_TAG` for the current `plan` node.
    */
  private def setTempTagRecursive(
      plan: SparkPlan,
      logicalPlan: LogicalPlan
  ): Unit = {
    plan.setTagValue(TEMP_LOGICAL_PLAN_TAG, logicalPlan)
    plan.children.foreach(c => setTempTagRecursive(c, logicalPlan))
  }

  /** Unset all `TEMP_LOGICAL_PLAN_TAG` tags.
    */
  private def cleanUpTempTags(plan: SparkPlan): Unit = {
    plan.foreach {
      case plan: SparkPlan
          if plan.getTagValue(TEMP_LOGICAL_PLAN_TAG).isDefined =>
        plan.unsetTagValue(TEMP_LOGICAL_PLAN_TAG)
      case _ =>
    }
  }

  /** Notify the listeners of the physical plan change.
    */
  private def onUpdatePlan(
      executionId: Long,
      newSubPlans: Seq[SparkPlan]
  ): Unit = {
    if (!shouldUpdatePlan) {
      val newMetrics = newSubPlans.flatMap { p =>
        p.flatMap(
          _.metrics.values.map(m =>
            SQLPlanMetric(m.name.get, m.id, m.metricType)
          )
        )
      }
      context.session.sparkContext.listenerBus
        .post(SparkListenerSQLAdaptiveSQLMetricUpdates(executionId, newMetrics))
    } else {
      val planDescriptionMode = ExplainMode.fromString(conf.uiExplainMode)
      context.session.sparkContext.listenerBus.post(
        SparkListenerSQLAdaptiveExecutionUpdate(
          executionId,
          context.qe.explainString(planDescriptionMode),
          SparkPlanInfo.fromSparkPlan(context.qe.executedPlan)
        )
      )
    }
  }

  /** Cancel all running stages with best effort and throw an Exception
    * containing all stage materialization errors and stage cancellation errors.
    */
  private def cleanUpAndThrowException(
      errors: Seq[Throwable],
      earlyFailedStage: Option[Int]
  ): Unit = {
    currentPhysicalPlan.foreach {
      // earlyFailedStage is the stage which failed before calling doMaterialize,
      // so we should avoid calling cancel on it to re-trigger the failure again.
      case s: ExchangeQueryStageExec if !earlyFailedStage.contains(s.id) =>
        try {
          s.cancel()
        } catch {
          case NonFatal(t) =>
            logError(s"Exception in cancelling query stage: ${s.treeString}", t)
        }
      case _ =>
    }
    // Respect SparkFatalException which can be thrown by BroadcastExchangeExec
    val originalErrors = errors.map {
      case fatal: SparkFatalException => fatal.throwable
      case other                      => other
    }
    val e = if (originalErrors.size == 1) {
      originalErrors.head
    } else {
      val se = QueryExecutionErrors.multiFailuresInStageMaterializationError(
        originalErrors.head
      )
      originalErrors.tail.foreach(se.addSuppressed)
      se
    }
    throw e
  }
}

object AdaptiveSparkPlanExec {
  private[adaptive] val executionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("QueryStageCreator", 16)
  )

  /** The temporary [[LogicalPlan]] link for query stages.
    *
    * Physical nodes wrapped in a [[LogicalQueryStage]] can be shared among
    * different physical plans and thus their usual logical links can be
    * overwritten during query planning, leading to situations where those nodes
    * point to a new logical plan and the rest point to the current logical
    * plan. In this case we use temp logical links to make sure we can always
    * trace back to the original logical links until a new physical plan is
    * adopted, by which time we can clear up the temp logical links.
    */
  val TEMP_LOGICAL_PLAN_TAG = TreeNodeTag[LogicalPlan]("temp_logical_plan")

  /** Apply a list of physical operator rules on a [[SparkPlan]].
    */
  def applyPhysicalRules(
      plan: SparkPlan,
      rules: Seq[Rule[SparkPlan]],
      loggerAndBatchName: Option[(PlanChangeLogger[SparkPlan], String)] = None
  ): SparkPlan = {
    if (loggerAndBatchName.isEmpty) {
      rules.foldLeft(plan) { case (sp, rule) => rule.apply(sp) }
    } else {
      val (logger, batchName) = loggerAndBatchName.get
      val newPlan = rules.foldLeft(plan) { case (sp, rule) =>
        val result = rule.apply(sp)
        logger.logRule(rule.ruleName, sp, result)
        result
      }
      logger.logBatch(batchName, plan, newPlan)
      newPlan
    }
  }
}

/** The execution context shared between the main query and all sub-queries.
  */
case class AdaptiveExecutionContext(session: SparkSession, qe: QueryExecution) {

  /** The subquery-reuse map shared across the entire query.
    */
  val subqueryCache: TrieMap[SparkPlan, BaseSubqueryExec] =
    new TrieMap[SparkPlan, BaseSubqueryExec]()

  /** The exchange-reuse map shared across the entire query, including
    * sub-queries.
    */
  val stageCache: TrieMap[SparkPlan, ExchangeQueryStageExec] =
    new TrieMap[SparkPlan, ExchangeQueryStageExec]()
}

/** The event type for stage materialization.
  */
sealed trait StageMaterializationEvent

/** The materialization of a query stage completed with success.
  */
case class StageSuccess(stage: QueryStageExec, result: Any)
    extends StageMaterializationEvent

/** The materialization of a query stage hit an error and failed.
  */
case class StageFailure(stage: QueryStageExec, error: Throwable)
    extends StageMaterializationEvent
