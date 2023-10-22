package de.tuberlin.dima.minidb.semantics.predicate;

import java.util.Map;


/**
 * The root interface for all classes that describe internal representations of local predicates.
 * A local predicate applies to a single table only.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public abstract class LocalPredicate implements Comparable<LocalPredicate>
{	
	/**
	 * An indicator about the predicates truth.
	 */
	protected PredicateTruth truth;
	
	/**
	 * The selectivity of this predicate.
	 */
	protected float selectivity; 
	
	// ------------------------------------------------------------------------
	
	/**
	 * Initializes the abstract local predicate to an unknown truth. 
	 */
	protected LocalPredicate()
	{
		this.truth = PredicateTruth.UNKNOWN;
	}

	// ------------------------------------------------------------------------
	
	/**
	 * Gets the truth from this OptimizerLocalPredicate.
	 *
	 * @return The truth.
	 */
	public PredicateTruth getTruth()
	{
		return this.truth;
	}

	/**
	 * Sets the truth for this OptimizerLocalPredicate.
	 *
	 * @param truth The truth to set.
	 */
	public void setTruth(PredicateTruth truth)
	{
		this.truth = truth;
	}
	
	// ------------------------------------------------------------------------
	
	/**
	 * Gets the overall selectivity of this predicate. The selectivity is the
	 * probability that a tuple passes this predicate.
	 * 
	 * @return This predicate's probability.
	 */
	public float getSelectivity()
	{
		return this.selectivity;
	}
	
	/**
	 * Sets the selectivity for this predicate.
	 * 
	 * @see de.tuberlin.dima.minidb.optimizer.predicates.OptimizerLocalPredicate#getSelectivity()
	 * @param selectivity The new selectivity.
	 */
	public void setSelectivity(float selectivity)
	{
		this.selectivity = selectivity;
	}
	
	/**
	 * Compares this predicate to another predicate with respect to selectivity.
	 * 
	 * @param o The predicate to compare to.
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(LocalPredicate o)
	{
		float otherSelectivity = o.getSelectivity();
		return this.selectivity < otherSelectivity ? -1 : this.selectivity > otherSelectivity ? 1 : 0;
	}
	
	/**
	 * Creates an executable variant of this optimizer predicate for use
	 * during query runtime.
	 * 
	 * @return An executable variant of the predicate.
	 */
	public abstract de.tuberlin.dima.minidb.qexec.predicate.LocalPredicate createExecutablePredicate();
	
	/**
	 * Creates a copy of this predicate, where the column indexes are adjusted from the positions in the base
	 * table to the positions in a projected tuple. The given map describes which column position from the
	 * base table is at which position in the projected tuple. If the column that a predicate needs is not
	 * yet included, than the predicates adds this column mapping.
	 *  
	 * @param columnMap The map mapping position in the base table to position in the tuple.
	 * @return A copy of the predicate, with adjusted column positions.
	 */
	public abstract LocalPredicate createCopyAdjustedForProjectedTuple(Map<Integer, Integer> columnMap);
}
