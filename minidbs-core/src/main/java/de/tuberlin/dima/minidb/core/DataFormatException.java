package de.tuberlin.dima.minidb.core;


/**
 * An exception that is thrown when the encoded format of a data field is invalid.
 * It occurs on the attempt to derive a DataField from encoded data (binary table,
 * binary index, string, ...) and the format of that encoded data is invalid.
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class DataFormatException extends Exception
{

	/**
     * A serial version UID to support object serialization.
     */
    private static final long serialVersionUID = -887141097358926147L;

	/**
	 * Create a plain DataFormatException with no message.
	 */
	public DataFormatException()
	{
	}

	/**
	 * Creates a DataFormatException with the given message.
	 * 
	 * @param message The message for the exception.
	 */
	public DataFormatException(String message)
	{
		super(message);
	}
}
