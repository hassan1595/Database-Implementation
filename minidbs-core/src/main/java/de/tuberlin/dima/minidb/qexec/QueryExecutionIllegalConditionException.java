package de.tuberlin.dima.minidb.qexec;


/**
 * This exception should be thrown during query execution, when a condition is
 * encountered that violates the guarantees that are made for a point. This
 * exception indicates that the plan would generate incorrect results.
 * 
 * Examples are the following:
 * <ul>
 *   <li>At a certain point in the query plan, the tuple stream is guaranteed to
 *       be sorted after some columns (just above a SORT operator), but is
 *       actually found to be not completely sorted.</li>
 *   <li>The tuples should be unique on the group columns, but are not.</li>
 *   <li>A certain field in the tuple is expected to have a certain data type,
 *       but found to have a different one.</li>
 * </ul>
 *
 * @author Helko Glathe, Stephan Ewen (sewen@cs.tu-berlin.de)
 */
public class QueryExecutionIllegalConditionException extends QueryExecutionException
{
	/**
	 * The serial version UID for JDK1.1 serialization compatibility.
	 */
    private static final long serialVersionUID = -4727446908660175533L;


	/**
	 * Create a plain QueryExecutionIllegalConditionException with no message.
	 */
	public QueryExecutionIllegalConditionException()
	{
		super("An illegal condition has been encountered violating properties " +
				"described by the query execution plan.");
	}

	/**
	 * Creates a QueryExecutionIllegalConditionException with the given message.
	 * 
	 * @param message The message for the exception.
	 */
	public QueryExecutionIllegalConditionException(String message)
	{
		super(message);
	}


	/**
	 * Creates a QueryExecutionIllegalConditionException with the given cause.
	 * 
	 * @param cause The original cause of the exception.
	 */
	public QueryExecutionIllegalConditionException(Throwable cause)
    {
	    super("An illegal condition has been encountered violating properties " +
				"described by the query execution plan.", cause);
    }
	
	
	/**
	 * Creates a QueryExecutionIllegalConditionException with the given message and
	 * original problem cause.
	 * 
	 * @param message The message for the exception.
	 * @param cause The original cause of the exception.
	 */
	public QueryExecutionIllegalConditionException(String message, Throwable cause)
    {
	    super(message, cause);
    }
}
