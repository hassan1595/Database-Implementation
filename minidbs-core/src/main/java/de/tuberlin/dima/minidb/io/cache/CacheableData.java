package de.tuberlin.dima.minidb.io.cache;

/**
 * The interface has to be implemented by all classes representing a page
 * that can be held in the DBS page cache.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public interface CacheableData
{
	/**
	 * This method checks, if the object has been modified since it was read from 
	 * the persistent storage. Objects that have been changed must be written back
	 * before they can be evicted from the cache.
	 * 
	 * @return true, if the data object has been modified, false if it is unchanged.
	 * @throws PageExpiredException Thrown, if the operation is performed 
	 * 								on a page that is identified to be expired. 
	 */
	boolean hasBeenModified() throws PageExpiredException;
	
	/**
	 * Gets the number of the page that is represented by this object.
	 * 
	 * @return The page number.
	 * @throws PageExpiredException Thrown, if the operation is performed 
	 * 								on a page that is identified to be expired. 
	 */
	int getPageNumber() throws PageExpiredException;
	
	/**
	 * Marks this cached data object as invalid. This method should be called, when the
	 * contents of the buffer behind the cached object is no longer in the cache, such that
	 * this cacheable data object would now work on invalid data.
	 */
	void markExpired();
	
	/**
	 * Checks if this cacheable object is actually expired. An object is expired, when the
	 * contents in the buffer behind that object is no longer in the cache or has been
	 * overwritten.
	 *  
	 * @return True, if the object is expired (no longer valid), false otherwise.
	 */
	boolean isExpired();
	
	/**
	 * Gets the binary buffer that contains the data that is wrapped by this page. All operations on this
	 * page will affect the buffer that is returned by this method.
	 * 
	 * @return The buffer containing the data wrapped by this page object.
	 */
	byte[] getBuffer();
}
