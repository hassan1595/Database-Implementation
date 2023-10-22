package de.tuberlin.dima.minidb.qexec;

/**
 * Operator representing the access to an index in a correlated fashion.
 * <p>
 * The index returns the RIDs of the tuples that match an index condition. It evaluates equality predicates against one
 * column of the current correlated tuple.
 * <p>
 * This interface is empty and serves only as a marker.
 * All relevant methods are specified in the interface {@link PhysicalPlanOperator}.
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public interface IndexCorrelatedLookupOperator extends PhysicalPlanOperator {

}
