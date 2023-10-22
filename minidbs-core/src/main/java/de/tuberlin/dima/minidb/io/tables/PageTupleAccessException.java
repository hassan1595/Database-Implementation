package de.tuberlin.dima.minidb.io.tables;


/**
 * An Exception that is thrown when access to a tuple on a page is attempted,
 * but failed. Reasons are for example using an invalid index or trying to
 * access a tuple that has been deleted.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class PageTupleAccessException extends Exception
{
	
	/**
     * A generated UID to allow object serialization.
     */
    private static final long serialVersionUID = 5292573604133434364L;
    
	/**
	 * The index of the tuple that was tried to be accessed.
	 */
	private int index;
	
	
	/**
	 * Creates a new exception for an invalid access to a tuple with
	 * the given index.
	 * 
	 * @param index The index of the tuple that could not be fetched.
	 */
	public PageTupleAccessException(int index)
	{
		super("Tuple could not be accessed.");
		this.index = index;
	}

	/**
	 * Creates a new exception for an invalid access to a tuple with
	 * the given index and uses the given message to further describe
	 * the problem.
	 * 
	 * @param index The index of the tuple that could not be fetched.
	 * @param message A message that further describes the problem.
	 */
	public PageTupleAccessException(int index, String message)
	{
		super(message);
		this.index = index;
	}

	/**
	 * The index of the tuple that could not be fetched.
	 * 
	 * @return The index of the tuple that could not be fetched.
	 */
	public int getIndex()
    {
    	return this.index;
    }
}
