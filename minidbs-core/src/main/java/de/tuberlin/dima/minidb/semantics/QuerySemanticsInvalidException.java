package de.tuberlin.dima.minidb.semantics;


/**
 * Exception used to indicate that the semantics of a parsed query are not valid.
 * An example would be a reference to a non-existent table or column.
 * 
 * Stephan Ewen (sewen@cs.tu-berlin.de)
 */
public class QuerySemanticsInvalidException extends Exception
{
	/**
	 * The serial version UID for JDK1.1 serialization compatibility.
	 */
	private static final long serialVersionUID = 2377370500497618810L;

	
	/**
	 * Create a plain QuerySemanticsInvalidException with no message.
	 */
	public QuerySemanticsInvalidException()
	{
		super("The semantics of the given query are invalid.");
	}

	/**
	 * Creates a QuerySemanticsInvalidException with the given message.
	 * 
	 * @param message The message for the exception.
	 */
	public QuerySemanticsInvalidException(String message)
	{
		super(message);
	}
}
