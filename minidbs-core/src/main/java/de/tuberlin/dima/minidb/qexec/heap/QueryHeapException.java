package de.tuberlin.dima.minidb.qexec.heap;


/**
 * An exception indicating an illegal sort heap request.
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class QueryHeapException extends Exception
{
	/**
	 * The serial version UID for JDK1.1 serialization compatibility.
	 */
	private static final long serialVersionUID = -3306658242070075145L;

	/**
	 * Create a plain SortHeapException with no message.
	 */
	public QueryHeapException()
	{
		super();
	}
	
	/**
	 * Create a plain SortHeapException with the given message.
	 * 
	 * @param message The exception message.
	 */
	public QueryHeapException(String message)
	{
		super(message);
	}
	
	/**
	 * Create a plain SortHeapException with the given cause.
	 * 
	 * @param cause The cause of the exception.
	 */
	public QueryHeapException(Throwable cause)
	{
		super(cause);
	}
	
	/**
	 * Create a plain SortHeapException with the given message.
	 * 
	 * @param message The exception message.
	 * @param cause The cause of the exception.
	 */
	public QueryHeapException(String message, Throwable cause)
	{
		super(message, cause);
	}
}
