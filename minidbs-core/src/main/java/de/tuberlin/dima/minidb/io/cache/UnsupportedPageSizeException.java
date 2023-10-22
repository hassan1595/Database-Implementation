package de.tuberlin.dima.minidb.io.cache;


/**
 * Exception representing a problem that occurred when a page size was
 * requested but not supported.
 *   
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class UnsupportedPageSizeException extends Exception
{
	
	/**
     * Generated serial version UID for serialization interoperability.
     */
    private static final long serialVersionUID = -7331920446621052046L;
    
    
	/**
	 * The page size in bytes that was requested but not supported.
	 */
	private final int requestedPageSize;
	
	
	/**
	 * Creates a new exception describing that the requested page size was not
	 * supported.
	 * 
	 * @param requestedPageSize The size (in bytes) that was not supported.
	 */
	public UnsupportedPageSizeException(int requestedPageSize)
	{
		this(requestedPageSize, "The page size of " + requestedPageSize + " was not supported.");
	}
	
	/**
	 * Creates a new exception describing that the requested page size was not
	 * supported, with an additional describing message.
	 * 
	 * @param requestedPageSize The size (in bytes) that was not supported.
	 * @param message The exceptions detail message.
	 */
	public UnsupportedPageSizeException(int requestedPageSize, String message)
	{
		super(message);
		this.requestedPageSize = requestedPageSize;
	}
	
	/**
	 * Gets the page size that was requested but not supported.
	 * 
	 * @return The requested page size (in bytes).
	 */
	public int getRequestedPageSize()
	{
		return this.requestedPageSize;
	}
}
