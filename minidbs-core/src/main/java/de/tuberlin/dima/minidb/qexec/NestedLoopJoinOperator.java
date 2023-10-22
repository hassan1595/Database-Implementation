package de.tuberlin.dima.minidb.qexec;

/**
 * Interface representing a nested loop join operator.
 * <p>
 * The operator iterates over the tuples of the outer side (outer tuples).
 * For each outer tuple, it opens the plan of the inner side using the outer tuple as the current correlated tuple.
 * It then iterates over all tuples of the inner side to produce join results.
 * <p>
 * The join optionally evaluates a join predicate.
 * Optionally means here that in some cases, no join predicate exists (Cartesian Join), or the join predicate is
 * already represented through the correlation (such as that the inner side in known to produce only tuples that
 * match the current outer tuple.)
 * <p>
 * This interface is empty and serves only as a marker.
 * All relevant methods are specified in the interface {@link PhysicalPlanOperator}.
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public interface NestedLoopJoinOperator extends PhysicalPlanOperator {
}
