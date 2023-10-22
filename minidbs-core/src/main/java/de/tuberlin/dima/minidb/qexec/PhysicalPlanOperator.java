package de.tuberlin.dima.minidb.qexec;


import de.tuberlin.dima.minidb.core.DataTuple;


/**
 * The basic signature of a low level physical query plan operator, following the
 * OPEN/NEXT/CLOSE design.
 * 
 * @author Stephan Ewen (sewen@cs.tu-berlin.de)
 * @author Michael Saecker
 */
public interface PhysicalPlanOperator
{
	/**
	 * Opens the plan below below and including this operator. Sets the initial status
	 * necessary to start executing the plan.
	 * 
	 * In the case that the tuples produced by the plan rooted at this operator
	 * are correlated to another stream of tuples, the plan is opened and closed for
	 * every such. The OPEN call gets as parameter the tuple for which the correlated
	 * tuples are to be produced this time.
	 * The most common example for that is the inner side of a nested-loop-join,
	 * which produces tuples correlated to the current tuple from the outer side
	 * (outer loop).
	 * 
	 * Consider the following pseudo code, describing the next() function of a 
	 * nested loop join:
	 * 
	 * // S is the outer child of the Nested-Loop-Join.
	 * // R is the inner child of the Nested-Loop-Join.
	 * 
	 * next() {
	 *   do {
	 *     r := R.next();
	 *     if (r == null) { // not found, R is exhausted for the current s
	 *       R.close();
	 *       s := S.next();
	 *       if (s == null) { // not found, both R and S are exhausted
	 *         return null;
	 *       } 
	 *       R.open(s);    // open the plan correlated to the outer tuple
	 *       r := R.next();
	 *     }
	 *   } while ( r.keyColumn != s.keyColumn ) // until predicate is fulfilled
	 *   
	 *   return concatenation of r and s;
	 * }
	 * 
	 * @param epoch	The epoch the tuples to be returned have to belong to.
	 * @param correlatedTuple The tuple for which the correlated tuples in the sub-plan
	 *                        below and including this operator are to be fetched. Is
	 *                        null is the case that the plan produces no correlated tuples.
	 * @throws QueryExecutionException Thrown, if the operator could not be opened,
	 *                                 that means that the necessary actions failed. 
	 */
	public void open(DataTuple correlatedTuple) throws QueryExecutionException;
	
	
	/**
	 * This gets the next tuple from the operator.
	 * This method should return null, if no more tuples are available.
	 * 
	 * @return The next tuple, produced by this operator.
	 * @throws QueryExecutionException Thrown, if the query execution could not be completed
	 *                                 for whatever reason.
	 */
	public DataTuple next() throws QueryExecutionException;
	
	
	/**
	 * Closes the query plan by releasing all resources and runtime structures used
	 * in this operator and the plan below.
	 * 
	 * @throws QueryExecutionException If the cleanup operation failed.
	 */
	public void close() throws QueryExecutionException;

}
