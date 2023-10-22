package de.tuberlin.dima.minidb.io.index;


/**
 * An exception that is thrown when the format of an index is discovered
 * to be corrupt. This happens for example, if the conditions of the
 * keys in interior nodes are found to be violated or the sorted order in the
 * leafs.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class IndexFormatCorruptException extends RuntimeException
{

	/**
     * Generated serial version UID to support object serialization.
     */
	private static final long serialVersionUID = -7911360298796959039L;

	/**
	 * Creates an empty exception without message.
	 */
	public IndexFormatCorruptException()
	{
	}

	/**
	 * Creates a plain exception with the given message.
	 * 
	 * @param message The message for the exception.
	 */
	public IndexFormatCorruptException(String message)
	{
		super(message);
	}

	/**
	 * Creates a IndexFormatCorruptException with the given cause.
	 * 
	 * @param cause The exception that originally causes this exception.
	 */
	public IndexFormatCorruptException(Throwable cause)
	{
		super(cause);
	}

	/**
	 * Creates a IndexFormatCorruptException with the given message and cause.
	 * 
	 * @param message The message for the exception.
	 * @param cause The exception that originally causes this exception.
	 */
	public IndexFormatCorruptException(String message, Throwable cause)
	{
		super(message, cause);
	}

}
