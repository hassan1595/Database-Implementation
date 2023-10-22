package de.tuberlin.dima.minidb;


import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.semantics.ProducedColumn;


/**
 * Simple stub interface to be implemented by classes that represent result sets and
 * collect result tuples, or errors.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public interface ResultHandler
{
	/**
	 * Function to be called prior to the beginning of query execution.
	 * 
	 * @param columns The schema of the columns that will be in the result set.
	 */
	public void openResultSet(ProducedColumn[] columns);
	
	/**
	 * This method is called for every tuple in the query result.
	 * 
	 * @param tuple The tuple to be added to the result.
	 */
	public void addResultTuple(DataTuple tuple);
	
	/**
	 * This method is called after the query execution has finished, either regularly or
	 * due to an error. 
	 */
	public void closeResultSet();
	
	/**
	 * This method is called to notify the result set that an exception or error
	 * has occurred.
	 * 
	 * @param t The exception or error that occurred.
	 */
	public void handleException(Throwable t);
}
