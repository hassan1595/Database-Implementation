package de.tuberlin.dima.minidb.qexec.predicate;

import org.apache.hadoop.io.Writable;

import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.qexec.QueryExecutionException;

/**
 * Interface describing a local predicate.
 * <p>
 * The predicate itself is not necessarily an atomic boolean condition, but may for example be a conjunction or
 * disjunction of other local predicates.
 * The local predicate can hence be understood as a tree where the inner nodes are conjunctions or disjunctions and the
 * leafs are the atomic conditions.
 * A call to {@link #evaluate} determines if the subtree below that local predicate is true.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public interface LocalPredicate extends Writable
{
	/**
	 * Evaluate this predicate on a tuple.
	 * 
	 * @param dataTuple The tuple to evaluate the predicate on.
	 * @return True, if the qualifies for this predicate, false otherwise. 
	 * @throws QueryExecutionException If the predicate could not be evaluated on
	 *                                 the given tuple.
	 */
	boolean evaluate(DataTuple dataTuple) throws QueryExecutionException;
}
