package de.tuberlin.dima.minidb.core;


/**
 * An error that is thrown if the internal operation failed due to an error in the system itself.
 * Those are errors that is not due to invalid data or any input, but due to incorrect behavior of
 * the DBMS.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class InternalOperationFailure extends Error
{
	/**
     * Generated serial version UID for serialization interoperability.
     */
    private static final long serialVersionUID = 4439260328207737794L;
    
	/**
	 * Flag indicating whether the error caused the entire system to be corrupted
	 * and requires a shutdown.
	 */
	private boolean fatal;


	/**
	 * Creates a new error with given message, cause and indicator if it is fatal.
	 * 
	 * @param message The message for this exception.
	 * @param fatal Flag indicating whether the exception caused the entire system to be corrupted
	 *              and requires a shutdown
	 * @param cause The exception/error that initially caused the internal operation failure.
	 */
	public InternalOperationFailure(String message, boolean fatal, Throwable cause)
	{
		super(message, cause);
		
		this.fatal = fatal;
	}
	
	/**
	 * Checks whether this exception was fatal for the entire system.
	 * 
	 * @return true, if the exception proved fatal, false otherwise.
	 */
	public boolean isFatal()
	{
		return this.fatal;
	}

}
