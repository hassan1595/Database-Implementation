package de.tuberlin.dima.minidb.qexec;

/**
 * Operator representing the access to an index in an uncorrelated fashion.
 * <p>
 * The index returns the RIDs of the tuples that match an index condition.
 * The condition is either the equality to some value:
 * <code> key = "Value" </code>
 * or the fact of being within the range between two keys:
 * <code> "val1" &lt;(=) key &lt;(=) "val2" </code>.
 * <p>
 * This interface is empty and serves only as a marker.
 * All relevant methods are specified in the interface {@link PhysicalPlanOperator}.
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public interface IndexLookupOperator extends PhysicalPlanOperator
{

}
