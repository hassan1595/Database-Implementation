package de.tuberlin.dima.minidb.qexec;


/**
 * The basic exception that is thrown whenever the query execution thread encounters
 * an error or a similar condition where it must abort the execution. Depending
 * on which kind of condition occurred, a more specific subclass of the exception
 * may be used, such as for example the QueyExecutionAbortedException.
 * If the execution failed to to another root cause (for example an I/O error),
 * the exception representing that cause should be passed as the <code>cause</code>
 * parameter to enable tracking back the cause of the failure.
 *
 * @author Helko Glathe, Stephan Ewen (sewen@cs.tu-berlin.de)
 */
public class QueryExecutionException extends Exception
{
	/**
	 * The serial version UID for JDK1.1 serialization compatibility.
	 */
    private static final long serialVersionUID = 8537760543878242053L;


	/**
	 * Create a plain QueryExecutionException with no message.
	 */
	public QueryExecutionException()
	{
		super("Query could not be executed.");
	}

	/**
	 * Creates a QueryExecutionException with the given message.
	 * 
	 * @param message The message for the exception.
	 */
	public QueryExecutionException(String message)
	{
		super(message);
	}


	/**
	 * Creates a QueryExecutionException with the given cause.
	 * 
	 * @param cause The original cause of the exception.
	 */
	public QueryExecutionException(Throwable cause)
    {
	    super("Query could not be executed.", cause);
    }
	
	
	/**
	 * Creates a QueryExecutionException with the given message and
	 * original problem cause.
	 * 
	 * @param message The message for the exception.
	 * @param cause The original cause of the exception.
	 */
	public QueryExecutionException(String message, Throwable cause)
    {
	    super(message, cause);
    }
}
