package de.tuberlin.dima.minidb.io.cache;


/**
 * An exception that is thrown when the binary format of an I/O page is invalid.
 * An example for an I/O pages are {@link de.tuberlin.dima.minidb.io.tables.TablePage}.
 * The construction of that page would for example throw this exception, if the
 * header was found to be corrupt.
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class PageFormatException extends Exception
{

	/**
     * A serial version UID to support object serialization.
     */
    private static final long serialVersionUID = -887141097358926147L;

	/**
	 * Create a plain PageFormatException with no message.
	 */
	public PageFormatException()
	{
	}

	/**
	 * Creates a page format exception with the given message.
	 * 
	 * @param message The message for the exception.
	 */
	public PageFormatException(String message)
	{
		super(message);
	}
}
