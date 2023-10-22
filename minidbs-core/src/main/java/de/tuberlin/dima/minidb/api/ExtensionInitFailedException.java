package de.tuberlin.dima.minidb.api;


/**
 * Simple Exception subclass to indicate that the initialization of the
 * extension registry has failed.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class ExtensionInitFailedException extends Exception
{

	/**
	 * A default serialVersionUID for serialization support. 
	 */
	private static final long serialVersionUID = 2005752890664247047L;

	
	/**
	 * Default constructor with no message and reason.
	 */
	public ExtensionInitFailedException()
	{
		super();
	}

	/**
	 * Constructs an exception with given message and original reason.
	 *  
	 * @param message The error message.
	 * @param cause The original reason.
	 */
	public ExtensionInitFailedException(String message, Throwable cause)
	{
		super(message, cause);
	}

	/**
	 * Constructs an exception with given message.
	 *  
	 * @param message The error message.
	 */
	public ExtensionInitFailedException(String message)
	{
		super(message);
	}

	/**
	 * Constructs an exception with original reason.
	 *  
	 * @param cause The original reason.
	 */
	public ExtensionInitFailedException(Throwable cause)
	{
		super(cause);
	}
}
