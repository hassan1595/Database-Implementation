package de.tuberlin.dima.minidb.qexec;

/**
 * Interface describing a physical plan operator that performs the FETCH operation.
 * <p>
 * The operator receives tuples from its child which contain a single RID field.
 * It uses this RID to fetch tuples from a table and returns them.
 * <p>
 * This interface is empty and serves only as a marker. All relevant methods
 * are specified in the interface {@link PhysicalPlanOperator}.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public interface FetchOperator extends PhysicalPlanOperator
{
}
