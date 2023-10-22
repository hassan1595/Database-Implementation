package de.tuberlin.dima.minidb.semantics.predicate;


import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import de.tuberlin.dima.minidb.qexec.predicate.LocalPredicateConjunction;


/**
 * The optimizer representation of a conjunction of other predicates.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class LocalPredicateConjunct extends LocalPredicate
{
	/**
	 * The predicates in this conjunction.
	 */
	private List<LocalPredicate> predicates;
	
	
	/**
	 * Creates a new empty conjunction.
	 */
	public LocalPredicateConjunct()
	{
		super();
		this.predicates = new ArrayList<LocalPredicate>();
	}
	
	/**
	 * Adds a predicate to this conjunction.
	 * 
	 * @param predicate The predicate to add.
	 */
	public void addPredicate(LocalPredicate predicate)
	{
		this.predicates.add(predicate);
	}
	
	/**
	 * Removes all boolean factor from this conjunction. 
	 */
	public void clearPredicates()
	{
		this.predicates.clear();
	}	
	
	/**
	 * Gets all predicates (boolean factors) in this conjunction.
	 * 
	 * @return An array with all boolean factors in this conjunction.
	 */
	public LocalPredicate[] getPredicates()
	{
		return this.predicates.toArray(new LocalPredicate[this.predicates.size()]);
	}
	
	/**
	 * Gets the number of predicates (boolean factors) in this conjunction.
	 * 
	 * @return The number of predicates in this conjunction.
	 */
	public int getNumberOfPredicates()
	{
		return this.predicates.size();
	}
	
	/**
	 * Sets the boolean factor in this conjunction equal to the given collection predicates.
	 * 
	 * @param predicates The new boolean factors for this conjunction.
	 */
	public void setPredicates(Collection<LocalPredicate> predicates)
	{
		this.predicates.clear();
		this.predicates.addAll(predicates);
	}
	
	/**
	 * Sorts the predicates in this conjunction after selectivity, ascending.
	 */
	public void sortBySelectivity()
	{
		Collections.sort(this.predicates);
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		StringBuilder bld = new StringBuilder(100);
		bld.append('[');
		
		for (int i = 0; i < this.predicates.size(); i++) {
			if (i != 0) {
				bld.append(' ').append('&').append(' ');
			}
			bld.append(this.predicates.get(i));
		}
		
		bld.append(']');
		
		return bld.toString();
	}
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.predicates.OptimizerLocalPredicate#createExecutablePredicate()
	 */
	@Override
	public LocalPredicateConjunction createExecutablePredicate()
	{
		// sort the predicates in descending selectivity
		sortBySelectivity();
		
		de.tuberlin.dima.minidb.qexec.predicate.LocalPredicate[] preds = 
			new de.tuberlin.dima.minidb.qexec.predicate.LocalPredicate[this.predicates.size()];
		
		for (int i = 0; i < preds.length; i++) {
			preds[i] = this.predicates.get(i).createExecutablePredicate();
		}
		
		return new LocalPredicateConjunction(preds);
	}
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.predicates.OptimizerLocalPredicate#createCopyAdjustedForProjectedTuple(java.util.Map)
	 */
	@Override
	public LocalPredicateConjunct createCopyAdjustedForProjectedTuple(Map<Integer, Integer> columnMap)
	{
		List<LocalPredicate> newPreds = new ArrayList<LocalPredicate>(this.predicates.size());
		
		for (int i = 0; i < this.predicates.size(); i++) {
			newPreds.add(this.predicates.get(i).createCopyAdjustedForProjectedTuple(columnMap));
		}
		
		LocalPredicateConjunct p = new LocalPredicateConjunct();
		p.setPredicates(newPreds);
		p.selectivity = this.selectivity;
		p.truth = this.truth;
		return p;
	}
}
