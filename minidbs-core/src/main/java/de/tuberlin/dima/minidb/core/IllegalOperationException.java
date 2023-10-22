package de.tuberlin.dima.minidb.core;


/**
 * An exception that is thrown during query processing when an operation is called
 * on a data type that does not support that operation.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class IllegalOperationException extends RuntimeException
{

	/**
     * Generated serial version UID for serialization interoperability.
     */
    private static final long serialVersionUID = 6797266983395688292L;

	/**
	 * Creates an empty exception without message.
	 */
	public IllegalOperationException()
	{
	}

	/**
	 * Creates a plain exception with the given message.
	 * 
	 * @param message The message for the exception.
	 */
	public IllegalOperationException(String message)
	{
		super(message);
	}

	/**
	 * Creates a IllegalOperationException with the given cause.
	 * 
	 * @param cause The exception that originally causes this exception.
	 */
	public IllegalOperationException(Throwable cause)
	{
		super(cause);
	}

	/**
	 * Creates a IllegalOperationException with the given message and cause.
	 * 
	 * @param message The message for the exception.
	 * @param cause The exception that originally causes this exception.
	 */
	public IllegalOperationException(String message, Throwable cause)
	{
		super(message, cause);
	}

}
