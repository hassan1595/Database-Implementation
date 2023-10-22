package de.tuberlin.dima.minidb.optimizer.joins;


import de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator;
import de.tuberlin.dima.minidb.semantics.JoinGraphEdge;
import de.tuberlin.dima.minidb.semantics.Relation;


/**
 * Interface for the class implementing the algorithm to pick the best join order for the
 * query execution plan.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public interface JoinOrderOptimizer
{
	/**
	 * Finds the best join order for a query represented as the given graph.
	 * 
	 * @param relations The relations, forming the nodes in the join graph.
	 * @param joins The joins, forming the edges in the join graph. 
	 * @return The abstract best plan, restricted to the join operators.
	 */
	public OptimizerPlanOperator findBestJoinOrder(Relation[] relations, JoinGraphEdge[] joins);
	
}
