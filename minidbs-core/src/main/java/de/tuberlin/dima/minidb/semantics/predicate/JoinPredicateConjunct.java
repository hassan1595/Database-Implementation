package de.tuberlin.dima.minidb.semantics.predicate;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import de.tuberlin.dima.minidb.qexec.predicate.JoinPredicateConjunction;


/**
 * The optimizer representation of a conjunction of join predicates, such as they occur with a
 * composite key.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class JoinPredicateConjunct implements JoinPredicate
{
	/**
	 * The boolean factors in this conjunction. 
	 */
	private List<JoinPredicateAtom> predicates;
	
	
	/**
	 * Creates an new plan join predicate conjunction.
	 */
	public JoinPredicateConjunct()
	{
		this.predicates = new ArrayList<JoinPredicateAtom>();
	}
	
	
	/**
	 * Adds a join predicate to this conjunction. If the predicate it itself a conjunction,
	 * then its factors are added. If not, the predicate becomes an additional factor in
	 * this conjunction.
	 * 
	 * @param predicate The predicate to add.
	 */
	public void addJoinPredicate(JoinPredicate predicate)
	{
		if (predicate instanceof JoinPredicateConjunct) {
			this.predicates.addAll(((JoinPredicateConjunct) predicate).predicates);
		}
		else {
			this.predicates.add((JoinPredicateAtom) predicate);
		}
	}
	
	
	/**
	 * Gets a reference to the list of join predicates in this join predicate conjunction.
	 * Because the returned list is a direct reference, all changes to that list are immediately
	 * effective on the conjunction.
	 * 
	 * @return The list of predicates in this conjunction.
	 */
	public List<JoinPredicateAtom> getConjunctiveFactors()
	{
		return this.predicates;
	}
	
	// ------------------------------------------------------------------------
	
	/**
	 * Creates a copy of this predicate conjunction where the sides are switched for all atoms.
	 * For all contained atoms, the left hand side becomes the right hand side and vice versa.
	 * 
	 * @return The side switched copy.
	 */
	@Override
	public JoinPredicateConjunct createSideSwitchedCopy()
	{
		JoinPredicateConjunct newConjunct = new JoinPredicateConjunct();
		for (JoinPredicateAtom p : this.getConjunctiveFactors()) {
			newConjunct.addJoinPredicate(p.createSideSwitchedCopy());
		}
		return newConjunct;
	}
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.predicates.OptimizerJoinPredicate#createExecutablepredicate()
	 */
	@Override
	public JoinPredicateConjunction createExecutablepredicate()
	{
		de.tuberlin.dima.minidb.qexec.predicate.JoinPredicate[] preds = 
			new de.tuberlin.dima.minidb.qexec.predicate.JoinPredicate[this.predicates.size()];
		for (int i = 0; i < preds.length; i++) {
			preds[i] = this.predicates.get(i).createExecutablepredicate();
		}
		return new JoinPredicateConjunction(preds);
	}
	
	// ------------------------------------------------------------------------
	// ------------------------------------------------------------------------

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
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((this.predicates == null) ? 0 : 
				Arrays.hashCode(this.predicates.toArray(new JoinPredicateAtom[this.predicates.size()])));
		return result;
	}


	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		JoinPredicateConjunct other = (JoinPredicateConjunct) obj;
		if (this.predicates == null && other.predicates != null || this.predicates != null && other.predicates == null) {
			return false;
		} 
		if(this.predicates.size() != other.predicates.size())
		{
			return false;
		} 
		else	
		{
			JoinPredicateAtom[] thisPreds = this.predicates.toArray(new JoinPredicateAtom[this.predicates.size()]);
			JoinPredicateAtom[] otherPreds = other.predicates.toArray(new JoinPredicateAtom[other.predicates.size()]);
			// check that all predicates from this conjunction are contained in the other conjunction
			for(int i = 0; i < thisPreds.length; i++)
			{
				boolean found = false;
				for(int j = 0; j < otherPreds.length && !found; j++)
				{
					if(thisPreds[i].equals(otherPreds[j]))
					{
						found = true;
					}
				}
				if(!found)
				{
					return false;
				}				
			}
			// check that all predicates from the other conjunction are contained in the this conjunction
			for(int i = 0; i < otherPreds.length; i++)
			{
				boolean found = false;
				for(int j = 0; j < thisPreds.length && !found; j++)
				{
					if(otherPreds[i].equals(thisPreds[j]))
					{
						found = true;
					}
				}
				if(!found)
				{
					return false;
				}				
			}
		}
		return true;
	}


	/**
	 * Checks whether a predicate is already contained in this conjunctive predicate.
	 * 
	 * @param predicate The predicate that may be contained in this conjunctive predicate.
	 * @return True, if the predicate is contained, false otherwise.
	 */
	public boolean contains(JoinPredicate predicate) 
	{
		return this.predicates.contains(predicate);
	}
}
