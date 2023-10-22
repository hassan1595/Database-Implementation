package de.tuberlin.dima.minidb.optimizer;

import de.tuberlin.dima.minidb.semantics.AnalyzedSelectQuery;


/**
 * This interface describes classes that can be used to verify join orders. The 
 * {@link #verifyJoinOrder(OptimizerPlanOperator)} method is called and either completes
 * successfully, or throws an exception, if the plan is considered correct.
 *
 */
public interface JoinOrderVerifier
{
	/**
	 * @param query The query for which the join plan has been created.
	 * @param pop The optimizer plan that is to be verified.
	 * @throws OptimizerException Thrown, if the plan is declared incorrect.
	 */
	public void verifyJoinOrder(AnalyzedSelectQuery query, OptimizerPlanOperator pop) throws OptimizerException;
	
}
