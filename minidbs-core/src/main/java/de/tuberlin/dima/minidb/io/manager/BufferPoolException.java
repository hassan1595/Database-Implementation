package de.tuberlin.dima.minidb.io.manager;


/**
 * An exception indicating that the buffer pool could not fulfill a certain request.
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class BufferPoolException extends Exception
{
	/**
     * Serial version UID for backwards compatibility with jdk1.1 serialization.
     */
    private static final long serialVersionUID = 195844171239628355L;

    
	/**
	 * Create a plain BufferPoolException with no message.
	 */
	public BufferPoolException()
	{}

	/**
	 * Creates a BufferPoolException with the given message.
	 * 
	 * @param message The message for the exception.
	 */
	public BufferPoolException(String message)
	{
		super(message);
	}

	/**
	 * Creates a BufferPoolException with the given cause.
	 * 
	 * @param cause The direct cause of the exception.
	 */
	public BufferPoolException(Throwable cause)
	{
		super(cause);
	}

	/**
	 * Creates a BufferPoolException with the given message and cause.
	 * 
	 * @param message The message for the exception.
	 * @param cause The direct cause of the exception.
	 */
	public BufferPoolException(String message, Throwable cause)
	{
		super(message, cause);
	}
}
