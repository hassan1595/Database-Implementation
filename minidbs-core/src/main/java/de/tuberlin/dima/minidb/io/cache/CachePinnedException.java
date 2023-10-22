package de.tuberlin.dima.minidb.io.cache;


/**
 * An exception that is thrown when a page could not be added to the cache, because
 * all entries in the entire cache were pinned.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class CachePinnedException extends Exception
{
	/**
     * Serial version UID for interoperability with prior version object serialization.
     */
    private static final long serialVersionUID = -5572357324298990992L;

    
	/**
	 * Creates a new CachePinnedException.
	 */
	public CachePinnedException()
	{
		super("The cache is completely pinned.");
	}
}
