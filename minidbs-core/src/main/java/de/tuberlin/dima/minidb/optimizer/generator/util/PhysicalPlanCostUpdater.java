package de.tuberlin.dima.minidb.optimizer.generator.util;

import de.tuberlin.dima.minidb.optimizer.FetchPlanOperator;
import de.tuberlin.dima.minidb.optimizer.FilterPlanOperator;
import de.tuberlin.dima.minidb.optimizer.GroupByPlanOperator;
import de.tuberlin.dima.minidb.optimizer.IndexLookupPlanOperator;
import de.tuberlin.dima.minidb.optimizer.MergeJoinPlanOperator;
import de.tuberlin.dima.minidb.optimizer.NestedLoopJoinPlanOperator;
import de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator;
import de.tuberlin.dima.minidb.optimizer.SortPlanOperator;
import de.tuberlin.dima.minidb.optimizer.TableScanPlanOperator;
import de.tuberlin.dima.minidb.optimizer.cost.CostEstimator;
import de.tuberlin.dima.minidb.semantics.AnalyzedSelectQuery;

public class PhysicalPlanCostUpdater
{
	/**
	 * Physical operator cost estimator.
	 */
	final private CostEstimator costEstimator;
	
	
	public PhysicalPlanCostUpdater(CostEstimator costEstimator)
	{
		this.costEstimator = costEstimator;
	}

	
	// ------------------------------------------------------------------------
	//                          Cost Computation
	// 
	// TODO: in a separate utility class 
	// ------------------------------------------------------------------------
	
	/**
	 * Computes the costs and cumulative costs for the given operator.
	 * The type of the operator is determined and then the corresponding
	 * code is invoked.
	 * 
	 * @param pop The operator to compute the costs for.
	 */
	public void costGenericOperator(OptimizerPlanOperator pop)
	{
		if (pop.getOperatorCosts() >= 0 && pop.getCumulativeCosts() != 0) {
			// operator has already been costed
			return;
		}
		
		if (pop instanceof TableScanPlanOperator) {
			costTableScanOperator((TableScanPlanOperator) pop);
		}
		else if (pop instanceof IndexLookupPlanOperator) {
			costIndexLookupOperator((IndexLookupPlanOperator) pop);
		}
		else if (pop instanceof SortPlanOperator) {
			costSortOperator((SortPlanOperator) pop);
		}
		else if (pop instanceof FetchPlanOperator) {
			costFetchOperator((FetchPlanOperator) pop);
		}
		else if (pop instanceof FilterPlanOperator) {
			costFilterOperator((FilterPlanOperator) pop);
		}
		else if (pop instanceof MergeJoinPlanOperator) {
			costMergeJoinOperator((MergeJoinPlanOperator) pop);
		}
		else if (pop instanceof NestedLoopJoinPlanOperator) {
			costNestedLoopJoinOperator((NestedLoopJoinPlanOperator) pop);
		}
		else if (pop instanceof GroupByPlanOperator) {
			costGroupByOperator((GroupByPlanOperator) pop);
		}
		else if (pop instanceof AnalyzedSelectQuery) {
			costSubQuery((AnalyzedSelectQuery) pop);
		}
		else {
			throw new IllegalArgumentException("Unrecognized plan operator.");
		}
	}
	
	/**
	 * Gets the cumulative costs of the child operator. If those costs have not yet
	 * been computed, this function triggers the computation of those costs.
	 * 
	 * @param childPop The child operator to get the costs from.
	 * @return The cumulative costs of the child operator. 
	 */
	private long getChildCosts(OptimizerPlanOperator childPop)
	{
		long childCosts = childPop.getCumulativeCosts();
		if (childCosts < 0) {
			// not yet computed
			costGenericOperator(childPop);
			childCosts = childPop.getCumulativeCosts();
		}
		return childCosts;
	}
	
	/**
	 * Computes the costs and cumulative costs for the given TABLE SCAN operator.
	 * 
	 * @param tscan The scan operator to compute the costs for.
	 */
	private void costTableScanOperator(TableScanPlanOperator tscan)
	{
		long scanCosts = this.costEstimator.computeTableScanCosts(tscan.getTable());
		tscan.setOperatorCosts(scanCosts);
		tscan.setCumulativeCosts(scanCosts);
	}
	
	/**
	 * Computes the costs and cumulative costs for the given INDEX SCAN operator.
	 * 
	 * @param iscan The index scan operator to compute the costs for.
	 */
	private void costIndexLookupOperator(IndexLookupPlanOperator iscan)
	{
		long cardOfResult = iscan.getOutputCardinality();
		long costs = this.costEstimator.computeIndexLookupCosts(iscan.getIndex(),
				iscan.getTableAccess().getTable(), cardOfResult);
		
		// index scan is a leaf operator, so operator costs and cumulative costs are the same
		iscan.setOperatorCosts(costs);
		iscan.setCumulativeCosts(costs);
	}
	
	/**
	 * Computes the costs and cumulative costs for the given SORT operator.
	 * 
	 * @param sortPop The sort operator to compute the costs for.
	 */
	private void costSortOperator(SortPlanOperator sortPop)
	{
		OptimizerPlanOperator childPop = sortPop.getChild();
		// get the child's costs and compute them if necessary
		long childCosts = getChildCosts(childPop);
		
		long sortCosts = this.costEstimator.computeSortCosts(childPop.getReturnedColumns(), 
				childPop.getOutputCardinality());
	
		sortPop.setOperatorCosts(sortCosts);
		sortPop.setCumulativeCosts(childCosts + sortCosts);
	}
	
	/**
	 * Computes the costs and cumulative costs for the given FETCH operator.
	 * 
	 * @param fetchPop The FETCH operator to compute the costs for.
	 */
	private void costFetchOperator(FetchPlanOperator fetchPop)
	{
		OptimizerPlanOperator childPop = fetchPop.getChild();
		// get the child's costs and compute them if necessary
		long childCosts = getChildCosts(childPop);
		
		long fetchCosts = this.costEstimator.computeFetchCosts(fetchPop.getAccessedTable(), 
				childPop.getOutputCardinality(), fetchPop.isSequentialFetch());
	
		fetchPop.setOperatorCosts(fetchCosts);
		fetchPop.setCumulativeCosts(childCosts + fetchCosts);
	}
	
	/**
	 * Computes the costs and cumulative costs for the given FILTER operator.
	 * 
	 * @param filterPop The filter operator to compute the costs for.
	 */
	private void costFilterOperator(FilterPlanOperator filterPop)
	{
		OptimizerPlanOperator childPop = filterPop.getChild();
		// get the child's costs and compute them if necessary
		long childCosts = getChildCosts(childPop);
		long filterCosts = this.costEstimator.computeFilterCost(filterPop.getSimplePredicate(), childPop.getOutputCardinality());
	
		filterPop.setOperatorCosts(filterCosts);
		filterPop.setCumulativeCosts(childCosts + filterCosts);
	}
	
	/**
	 * Computes the costs and cumulative costs for the given MERGE JOIN operator.
	 * 
	 * @param mergeJoinPop The merge join operator to compute the costs for.
	 */
	private void costMergeJoinOperator(MergeJoinPlanOperator mergeJoinPop)
	{
		OptimizerPlanOperator leftChild = mergeJoinPop.getLeftChild();
		OptimizerPlanOperator rightChild = mergeJoinPop.getRightChild();
		
		// get the child's costs and compute them if necessary
		long leftChildCosts = getChildCosts(leftChild);
		long rightChildCosts = getChildCosts(rightChild);
		
		long joinCosts = this.costEstimator.computeMergeJoinCost();
	
		mergeJoinPop.setOperatorCosts(joinCosts);
		mergeJoinPop.setCumulativeCosts(joinCosts + leftChildCosts + rightChildCosts);
	}
	
	/**
	 * Computes the costs and cumulative costs for the given NESTED-LOOP JOIN operator.
	 * 
	 * @param nlJoinPop The nested loop join operator to compute the costs for.
	 */
	private void costNestedLoopJoinOperator(NestedLoopJoinPlanOperator nlJoinPop)
	{
		OptimizerPlanOperator outerChild = nlJoinPop.getOuterChild();
		OptimizerPlanOperator innerChild = nlJoinPop.getInnerChild();
		
		// get the child's costs and compute them if necessary
		long outerChildCosts = getChildCosts(outerChild);
		getChildCosts(innerChild);
		
		long joinCosts = this.costEstimator.computeNestedLoopJoinCost(outerChild.getOutputCardinality(), innerChild);
		
		nlJoinPop.setOperatorCosts(joinCosts);
		nlJoinPop.setCumulativeCosts(joinCosts + outerChildCosts);
	}
	
	/**
	 * Computes the costs and cumulative costs for the given GROUP BY operator.
	 * The operator is assumed to be free, as it only performs a one-pass over
	 * a pre-sorted stream of tuples.
	 * 
	 * @param groupOp The GROUP BY operator to compute the costs for.
	 */
	private void costGroupByOperator(GroupByPlanOperator groupOp)
	{
		OptimizerPlanOperator child = groupOp.getChild();
		
		// get the child's costs and compute them if necessary
		long childCosts = getChildCosts(child);
		
		groupOp.setOperatorCosts(0);
		groupOp.setCumulativeCosts(childCosts);
	}
	
	/**
	 * Computes the costs and cumulative costs for the given sub-query.
	 * The cost of the sub-query is the cost of its plan.
	 * 
	 * @param subQuery The sub-query to be costed.
	 */
	private void costSubQuery(AnalyzedSelectQuery subQuery)
	{
		OptimizerPlanOperator plan = subQuery.getQueryPlan();
		if (plan == null) {
			throw new IllegalStateException("Subquery cannot be costed, because it has no physical plan yet.");
		}
		
		// get the child's costs and compute them if necessary
		long childCosts = getChildCosts(plan);
		
		subQuery.setOperatorCosts(0);
		subQuery.setCumulativeCosts(childCosts);
	}
}
