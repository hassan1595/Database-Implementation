package de.tuberlin.dima.minidb.io.cache;


/**
 * An exception that is thrown on the attempt to add a page to the cache, for which
 * already a cache entry exists.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class DuplicateCacheEntryException extends Exception
{
	/**
     * Serial version UID for interoperability with prior version object serialization.
     */
    private static final long serialVersionUID = 823019747005680146L;
	
	/**
	 * The duplicate's resource id.
	 */
	private final int resourceId;
	
	/**
	 * The duplicate's page number.
	 */
	private final int pageNumber;

	/**
	 * Creates a new DuplicateCacheEntryException specifying the page that occurred
	 * double.
	 * 
	 * @param resourceId The duplicate's resource id.
	 * @param pageNumber The duplicate's page number.
	 */
	public DuplicateCacheEntryException(int resourceId, int pageNumber)
    {
	    super();
	    this.resourceId = resourceId;
	    this.pageNumber = pageNumber;
    }

	/**
	 * Gets the resource id of the duplicate entry.
	 * 
	 * @return The duplicate's resource id.
	 */
	public int getResourceId()
    {
    	return this.resourceId;
    }

	/**
	 * Gets the page number of the duplicate entry.
	 * 
	 * @return The duplicate's page number.
	 */
	public int getPageNumber()
    {
    	return this.pageNumber;
    }
}
