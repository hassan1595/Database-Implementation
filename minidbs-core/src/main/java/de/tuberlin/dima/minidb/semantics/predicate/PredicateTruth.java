package de.tuberlin.dima.minidb.semantics.predicate;


/**
 * Enumeration indicating the truth status of a predicate.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public enum PredicateTruth
{
	/**
	 * Indicating that it is unknown whether the predicate is true. 
	 */
	UNKNOWN,

	/**
	 * Indicating that it is known that the predicate is always true. 
	 */
	ALWAYS_TRUE,
	
	/**
	 * Indicating that it is known that the predicate is always false. 
	 */
	ALWAYS_FALSE
}
