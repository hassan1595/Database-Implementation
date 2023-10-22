package de.tuberlin.dima.minidb.qexec;

/**
 * Interface describing a FILTER operator that applies a predicate on the incoming tuples.
 * <p>
 * This interface is empty and serves only as a marker.
 * All relevant methods are specified in the interface {@link PhysicalPlanOperator}.
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public interface FilterOperator extends PhysicalPlanOperator {

}