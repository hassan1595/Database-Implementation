package de.tuberlin.dima.minidb.catalogue;


/**
 * An exception to indicate that the catalogue file could not be read because it is in
 * an invalid format.
 *
 * Stephan Ewen (sewen@cs.tu-berlin.de)
 */
public class CatalogueFormatException extends Exception
{
	/**
	 * The serial version UID for JDK1.1 serialization compatibility.
	 */
    private static final long serialVersionUID = -6780659779536481012L;

	/**
	 * Create a plain CatalogueFormatException with no message.
	 */
	public CatalogueFormatException()
	{
		super("The catalogue format is invalid.");
	}
	
	/**
	 * Creates a CatalogueFormatException with the given message.
	 * 
	 * @param message The message for the exception.
	 */
	public CatalogueFormatException(String message)
	{
		super(message);
	}
	
	/**
	 * Creates a CatalogueFormatException with the given cause.
	 * 
	 * @param cause The cause of the exception.
	 */
	public CatalogueFormatException(Throwable cause)
	{
		super(cause);
	}
	
	/**
	 * Creates a CatalogueFormatException with the given message and cause.
	 * 
	 * @param message The message for the exception.
	 * @param cause The cause of the exception.
	 */
	public CatalogueFormatException(String message, Throwable cause)
	{
		super(message, cause);
	}
}
