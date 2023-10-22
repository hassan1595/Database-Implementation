package de.tuberlin.dima.minidb.semantics;


/**
 * This enumeration represents an order for the output columns.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public enum Order
{
	/**
	 * Constant indicating that there is no order defined on the column.
	 */
	NONE,
	
	/**
	 * Constant indicating that there is an ascending order defined on the column.
	 */
	ASCENDING,
	
	/**
	 * Constant indicating that there is a descending order defined on the column.
	 */
	DESCENDING
}
