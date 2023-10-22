package de.tuberlin.dima.minidb.qexec.heap;


import de.tuberlin.dima.minidb.qexec.QueryExecutionException;


/**
 * A version of the QueryExecutionException indicating that the query was not
 * completed because the memory was insufficient to perform some operation, typically a
 * sort of hash table building.
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class QueryExecutionOutOfHeapSpaceException extends QueryExecutionException
{
	/**
	 * The serial version UID for JDK1.1 serialization compatibility.
	 */
    private static final long serialVersionUID = -6380784352146630161L;

	/**
	 * Create a plain QueryExecutionOutOfHeapSpaceException with no message.
	 */
	public QueryExecutionOutOfHeapSpaceException()
	{
		super("The query execution failed because the available query heap space was too small.");
	}
	
	/**
	 * Create a plain QueryExecutionOutOfHeapSpaceException with the given message.
	 */
	public QueryExecutionOutOfHeapSpaceException(String message)
	{
		super(message);
	}
}
