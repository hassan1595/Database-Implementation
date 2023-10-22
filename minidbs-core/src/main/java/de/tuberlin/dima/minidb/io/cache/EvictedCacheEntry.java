package de.tuberlin.dima.minidb.io.cache;


/**
 * This class represents entries that are evicted from a page cache. The entry consists primarily
 * of the binary page (byte buffer) where the cached data is held. In addition, the cache
 * entry holds fields that allow to identify the specific resource and its page where the
 * data came from and/or will be written to.
 * 
 * The entry also holds the wrapping page object (TablePage or IndexPage) that is used
 * to access the elements contained on the page.
 * 
 * An evicted cache entry always holds the binary page byte array. The other fields may be null
 * or undefined, when the cache entry represents an entry that does not hold any resource data,
 * for example just after the initialization of the page cache.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class EvictedCacheEntry
{
	/**
	 * The buffer holding the binary page. The most important field in the
	 * cache entry.
	 */
	private final byte[] binaryPage;
	
	/**
	 * The page object wrapping the binary page (TablePage/IndexPage/...).
	 * May be null for entries without resource data.
	 */
	private final CacheableData wrappingPage;
	
	/**
	 * The id identifying the specific resource.
	 */
	private final int resourceID;
	
	
	// ------------------------------------------------------------------------
	
	
	/**
	 * Creates a new CacheEntry that holds the given byte buffer with the binary page, and in
	 * addition the type, id and page number to identify the resource and origin/destination
	 * of the cached data.
	 * 
	 * @param binaryPage The binary page that is cached through this entry.
	 * @param wrappingPage The object that is used to access the data in the binary page. May be null.
	 * @param resourceID The id identifying the specific resource.
	 */
	public EvictedCacheEntry(byte[] binaryPage, CacheableData wrappingPage, int resourceID)
	{
		if (binaryPage == null) {
			// fail fast
			throw new NullPointerException("Binary page buffer must not be null");
		}
		
		this.binaryPage = binaryPage;
		this.wrappingPage = wrappingPage;
		this.resourceID = resourceID;
	}
	
	/**
	 * Creates a new cache entry for a binary page buffer that does not
	 * hold any resource data. This constructor is called to create empty cache entries
	 * when creating a new cache or increasing the size.
	 * 
	 * @param binaryPage The buffer for this cache entry.
	 */
	public EvictedCacheEntry(byte[] binaryPage)
	{
		if (binaryPage == null) {
			// fail fast
			throw new NullPointerException("Binary page buffer must not be null");
		}
		
		this.binaryPage = binaryPage;
		this.wrappingPage = null;
		this.resourceID = -1; // make negative to fail in the end
	}
	
	
	// ------------------------------------------------------------------------
	
	
	/**
	 * Gets the binary page buffer that holds the data cached by this entry.
	 *  
	 * @return This entry's binary page buffer.
	 */
	public byte[] getBinaryPage()
	{
		return this.binaryPage;
	}

	/**
	 * Gets the page (TablePage/IndexPage/...) that wraps the data in the binary page
	 * buffer and is used to manipulate the binary page.
	 * 
	 * @return The page object wrapping this entry's binary buffer, or null if no page is associated.
	 */
	public CacheableData getWrappingPage()
	{
		return this.wrappingPage;
	}

	/**
	 * Gets the id of the resource where the cache's data originated from or will be
	 * written to. For cache entries that hold no resource data, this method returns
	 * -1. 
	 * 
	 * @return The resource's id, or -1, if this cache entry holds no resource data. 
	 */
	public int getResourceID()
	{
		return this.resourceID;
	}

	/**
	 * Gets the physical page number of the page in the resource where this entry's
	 * data originated or will be written to.
	 * 
	 * @return The physical page number, or -1, if the cache entry holds no resource data.
	 */
	public int getPageNumber()
	{
		return this.wrappingPage != null ? this.wrappingPage.getPageNumber() : -1;
	}
}
