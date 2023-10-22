package de.tuberlin.dima.minidb.io.cache;


/**
 * An exception that is thrown when the operations are performed on a page that is
 * identified to be expired.
 *
 * @author Stephan Ewen (sewen@cs.tu-berlin.de)
 */
public class PageExpiredException extends RuntimeException
{
	/**
     * Serial version UID for backwards compatibility with jdk1.1 serialization.
     */
    private static final long serialVersionUID = 4160418276230980793L;

	/**
	 * Create a plain PageExpiredException with no message.
	 */
	public PageExpiredException()
	{
	}

	/**
	 * Creates a PageExpiredException with the given message.
	 * 
	 * @param message The message for the exception.
	 */
	public PageExpiredException(String message)
	{
		super(message);
	}
}
