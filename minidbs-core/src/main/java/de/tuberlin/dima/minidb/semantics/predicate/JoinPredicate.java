package de.tuberlin.dima.minidb.semantics.predicate;


/**
 * The base interface marking objects as internal representations of join predicates.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public interface JoinPredicate
{
	
	/**
	 * Creates a side switched copy of this join predicate. In the copy,
	 * the left side becomes the right side and vice versa.
	 * 
	 * @return The side switched copy.
	 */
	public JoinPredicate createSideSwitchedCopy();
	
	/**
	 * Creates an executable version of this predicate, build from exactly the
	 * information that is currently held by this optimizer predicate.
	 * 
	 * @return An executable version of the predicate.
	 */
	public de.tuberlin.dima.minidb.qexec.predicate.JoinPredicate createExecutablepredicate();
}
