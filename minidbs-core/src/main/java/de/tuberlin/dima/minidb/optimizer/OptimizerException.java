package de.tuberlin.dima.minidb.optimizer;

/**
 * Exception that is thrown when the optimizer encountered an erroneous condition.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class OptimizerException extends Exception
{
	/**
	 * Generated serial version UID for backward compatibility.
	 */
	private static final long serialVersionUID = -2251034179280936390L;

	/**
	 * Creates a plain OptimizerException with no message.
	 */
	public OptimizerException()
	{
		super();
	}

	/**
	 * Creates a plain OptimizerException with the given message.
	 * 
	 * @param message The message for the exception.
	 */
	public OptimizerException(String message)
	{
		super(message);
	}

	/**
	 * Creates an OptimizerException with the given message and cause.
	 * 
	 * @param message The message for the exception.
	 * @param cause The exception that originally causes this exception.
	 */
	public OptimizerException(String message, Throwable cause)
	{
		super(message, cause);
	}

	/**
	 * Creates an OptimizerException with the given cause.
	 * 
	 * @param cause The exception that originally causes this exception.
	 */
	public OptimizerException(Throwable cause)
	{
		super(cause);
	}
}
