package de.tuberlin.dima.minidb.core;


/**
 * An exception that is thrown when a duplicate is detected even though the
 * conditions at some point state that no duplicates are allowed.
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class DuplicateException extends RuntimeException
{

	/**
     * A serial version UID to support object serialization.
     */
	private static final long serialVersionUID = 3681937260877009268L;

	/**
	 * Create a plain DuplicateException with no message.
	 */
	public DuplicateException()
	{
	}

	/**
	 * Creates a DuplicateException with the given message.
	 * 
	 * @param message The message for the exception.
	 */
	public DuplicateException(String message)
	{
		super(message);
	}
}
