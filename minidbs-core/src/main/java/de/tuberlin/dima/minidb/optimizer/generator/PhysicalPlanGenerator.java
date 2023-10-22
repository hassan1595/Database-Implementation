package de.tuberlin.dima.minidb.optimizer.generator;

import de.tuberlin.dima.minidb.optimizer.AbstractJoinPlanOperator;
import de.tuberlin.dima.minidb.optimizer.InterestingOrder;
import de.tuberlin.dima.minidb.optimizer.OptimizerException;
import de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator;
import de.tuberlin.dima.minidb.optimizer.RequestedOrder;
import de.tuberlin.dima.minidb.semantics.AnalyzedSelectQuery;
import de.tuberlin.dima.minidb.semantics.Column;
import de.tuberlin.dima.minidb.semantics.ProducedColumn;
import de.tuberlin.dima.minidb.semantics.Relation;
import de.tuberlin.dima.minidb.semantics.predicate.LocalPredicate;

/**
 * A component that produces a physical plans for a SELECT query and its abstract join 
 * plan.
 * 
 * @author Alexander Alexandrov (alexander.alexandrov@tu-berlin.de)
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public interface PhysicalPlanGenerator
{
	// --------------------------------------------------------------------------------------------
	//                                  Plan Generation Entry Point
	// --------------------------------------------------------------------------------------------
	
	/**
     * The root rule that takes a select query and its abstract join plan and
     * generates a specific plan for it. This rule itself takes care of the ORDER BY
     * requirements and recursively calls further rules for the remainder.
     * 
     * This rule finds the best optimizer plan for that select query with respect to
     * the abstract join plan, the cost model and the plan space spanned by the
     * rules.
     * 
     * @param query The select to create a plan for.
     * @param joinPlan The abstract join plan for the select query.
     * @return The best specific optimizer plan for the given select query and
     * @throws OptimizerException Thrown, when the given condition violates the
     *         conditions of the optimizer.
	 */
	OptimizerPlanOperator generatePhysicalPlan(AnalyzedSelectQuery query, OptimizerPlanOperator joinPlan) throws OptimizerException;
	
	// --------------------------------------------------------------------------------------------
	//                                  Plan Generation Rules
	// --------------------------------------------------------------------------------------------

	/**
	 * This rule itself takes care of the ORDER BY requirements and recursively 
	 * calls the group by rule for the remainder.
	 * 
	 * This rule finds the best optimizer plans for that select query with respect
	 * to the abstract join plan, the cost model and the plan space spanned by
	 * the rules.
	 *  
	 * @param query The select to create a plan for. 
	 * @param joinPlan The abstract join plan for the select query.
	 * @return An array of the cheapest plans for this rule
	 * @throws OptimizerException Thrown, when the given condition violates the
	 *                            conditions of the optimizer.
	 */
	OptimizerPlanOperator[] buildBestOrderByPlans(AnalyzedSelectQuery query, OptimizerPlanOperator joinPlan) throws OptimizerException;


	/**
	 * This rule creates a series of plans that represent the plan of a query after
	 * table accesses, joins, aggregation and grouping. That is basically everything,
	 * except the ORDER BY clause specification.
	 * 
	 * This method recursively invokes the buildBestSubPlans() rule to generate plans for 
	 * all except grouping/aggregation and post-aggregation-predicates and then adds 
	 * operators for those.
	 * 
	 * For grouping, it checks if it can use an order such that this order can be reused
	 * by the rule that invoked this one.
	 * 
	 * @param outCols The columns that should be produced and returned by the plans.
	 * @param grouping Flag indicating if this plan applies grouping and aggregation at all.
	 * @param havingPred The predicate in the having clause. May be null.
	 * @param joinPlan The plan of abstract joins, as determined by the join order optimizer.
	 * @param order A requested order from the rule that processed the ORDER BY clause. May only
	 *              be non-null, if it is on a subset of the grouping columns.
	 * @param orderColIndices The sort columns that correspond to the requested order. Only not null, if the
	 *                 order is not null.
	 * @return An array of the cheapest plans for this rule.
	 * @throws OptimizerException Thrown, if an invalid situation is encountered during
	 *                            plan generation or comparison.
	 */
	OptimizerPlanOperator[] buildBestGroupByPlans(ProducedColumn[] outCols, boolean grouping, LocalPredicate havingPred, OptimizerPlanOperator joinPlan, RequestedOrder[] order, int[] orderColIndices) throws OptimizerException;


	/**
	 * Takes an abstract join plan, a set of required columns and some interesting orders and
	 * builds the cheapest plans that perform joins as indicated by the join plan and produce the
	 * required columns.
	 * 
	 * Internally, this method checks if the plan is an actual join plan or a base table access
	 * and invokes the corresponding rules.
	 * 
	 * @param neededCols The columns that are required to be produced by the plans.
	 * @param abstractJoinPlan The abstract join plan, describing join order and local predicates
	 *                         at the base table access level.
	 * @param intOrders The interesting orders after which the plans are compared and pruned.
	 * @return An array of the cheapest plans for this rule.
	 * @throws OptimizerException Thrown, if an invalid situation is encountered during
	 *                            plan generation or comparison.
	 */
	OptimizerPlanOperator[] buildBestSubPlans(Column[] neededCols, OptimizerPlanOperator abstractJoinPlan, InterestingOrder[] intOrders) throws OptimizerException;


	/**
	 * Build the alternatives to perform the given join. This method recursively calls rules
	 * to build the children of the join and then creates the different alternatives to perform
	 * the join. 
	 *
	 * Those alternatives are: Merge Join (equi-joins only), Index-Nested-Loop-Joins (if one of the
	 * children is not a join plan and has an index in the join keys), Nested-Loop-Joins for
	 * both ways to assign sides (for non-equi-joins).
	 * 
	 * @param neededCols The columns that are required to be produced.
	 * @param join The join to create alternatives for.
	 * @param intOrders The interesting orders after which pruning is done.
	 * @return An array of the cheapest plans for this rule.
	 * @throws OptimizerException Thrown, if an invalid situation is encountered during
	 *                            plan generation or comparison.
	 */
	OptimizerPlanOperator[] buildBestConcreteJoinPlans(Column[] neededCols, AbstractJoinPlanOperator join, InterestingOrder[] intOrders) throws OptimizerException;


	/**
	 * Creates all possibly table access plans and prunes them with respect to the given
	 * interesting orders.
	 * 
	 * @param neededCols The columns required to be created by the table access.
	 * @param toAccess The (yet abstract, no columns) table scan operator representing the
	 *              table access.
	 * @param intOrders The interesting order that apply to columns for this table.
	 * @return An array of best plans for the access to the given table.
	 * @throws OptimizerException Thrown, if a condition is presented that violates optimizer
	 *                            rules. 
	 */
	OptimizerPlanOperator[] buildBestRelationAccessPlans(Column[] neededCols, Relation toAccess, InterestingOrder[] intOrders) throws OptimizerException;
}
