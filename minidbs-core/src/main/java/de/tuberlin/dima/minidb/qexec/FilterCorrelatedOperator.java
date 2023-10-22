package de.tuberlin.dima.minidb.qexec;

/**
 * Interface describing a FILTER operator that filters incoming tuples by comparing them against the current correlated
 * tuple.
 * <p>
 * This interface is empty and serves only as a marker.
 * All relevant methods are specified in the interface {@link PhysicalPlanOperator}.
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public interface FilterCorrelatedOperator extends PhysicalPlanOperator {

}