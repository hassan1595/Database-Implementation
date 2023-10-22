package de.tuberlin.dima.minidb.io.cache;


/**
 * The specification of a page cache holding a fix number of entries and supporting
 * pinning of pages.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public interface PageCache
{
	
	/**
	 * Checks, if a page is in the cache and returns the cache entry for it. The
	 * page is identified by its resource id and the page number. If the
	 * requested page is not in the cache, this method returns null.
	 * The page experiences a cache hit through the request.
	 * 
	 * @param resourceId The id of the resource for which we seek to get a page.
	 * @param pageNumber The physical page number of the page we seek to retrieve.
	 * @return The cache entry containing the page data, or null, if the page is not
	 *         contained in the cache.
	 */
	CacheableData getPage(int resourceId, int pageNumber);
	
	/**
	 * Checks, if a page is in the cache and returns the cache entry for it. Upon success,
	 * it also increases the pinning counter of the entry for the page such that the entry 
	 * cannot be evicted from the cache until the pinning counter reaches zero. 
	 * <p>
	 * The page is identified by its resource id and the page number. If the
	 * requested page is not in the cache, this method returns null.
	 * The page experiences a cache hit through the request.
	 * 
	 * @param resourceId The id of the resource for which we seek to get a page.
	 * @param pageNumber The physical page number of the page we seek to retrieve.
	 * @return The cache entry containing the page data, or null, if the page is not
	 *         contained in the cache.
	 */
	CacheableData getPageAndPin(int resourceId, int pageNumber);
	
	/**
	 * This method adds a page to the cache by adding a cache entry for it. The entry must not be
	 * already contained in the cache. In order to add the new entry, one entry will always be
	 * evicted to keep the number of contained entries constant.
	 * <p>
	 * If the cache is still pretty cold (new) then the evicted entry may be an
	 * entry containing no resource data. However, he evicted entry always contain a binary page
	 * buffer.
	 * <p>
	 * For ARC, if the page was not in any of the bottom lists (B lists), the page enters the MRU end of the
	 * T1 list. The newly added page is not considered as hit yet. Therefore, the first getPage() request to that
	 * page produces the first hit and does not move the page out of the T1 list. This functionality is important
	 * to allow pre-fetched pages to be added to the cache without causing the first actual "GET" request to
	 * mark them as frequent.
	 * If the page was contained in any of the Bottom lists before, it will directly enter the T2 list (frequent
	 * items). Since it is directly considered frequent, it is considered to be hit.  
	 * 
	 * @param newPage The new page to be put into the cache.
	 * @param resourceId The id of the resource the page belongs to.
	 * 
	 * @return The evicted cache entry.
	 * @throws CachePinnedException Thrown, if no page could be evicted, because all pages
	 *                              are pinned.
	 * @throws DuplicateCacheEntryException Thrown, if an entry for that page is already contained.
	 *                                      The entry is considered to be already contained, if an
	 *                                      entry with the same resource-type, resource-id and page
	 *                                      number is in the cache. The contents of the binary page
	 *                                      buffer does not need to match for this exception to be
	 *                                      thrown.
	 */
	EvictedCacheEntry addPage(CacheableData newPage, int resourceId)
		throws CachePinnedException, DuplicateCacheEntryException;
	

	/**
	 * This method behaves similarly to the {@link #addPage} method, with the
	 * following distinctions:
	 *
	 * <ol>
	 * <li>The page is immediately pinned. (Increase pinning counter, see unpinPage for further information.)</li>
	 * <li>The page is immediately considered to be hit, even if it enters the T1 list.</li>
	 * </ol>
	 * @param newPage The new page to be put into the cache.
	 * @param resourceId The id of the resource the page belongs to.
	 * 
	 * @return The evicted cache entry.
	 * @throws CachePinnedException Thrown, if no page could be evicted, because all pages
	 *                              are pinned.
	 * @throws DuplicateCacheEntryException Thrown, if an entry for that page is already contained.
	 *                                      The entry is considered to be already contained, if an
	 *                                      entry with the same resource-type, resource-id and page
	 *                                      number is in the cache. The contents of the binary page
	 *                                      buffer does not need to match for this exception to be
	 *                                      thrown.
	 */
	EvictedCacheEntry addPageAndPin(CacheableData newPage, int resourceId)
		throws CachePinnedException, DuplicateCacheEntryException;
	
	/**
	 * Decreases the pinning counter of the entry for the page described by this resource ID and
	 * page number. If there is no entry for this page, this method does nothing. If
	 * the entry is not pinned, this method does nothing. If the pinning counter reaches zero, the
	 * page can be evicted from the cache.
	 * 
	 * @param resourceId The id of the resource of the page entry to be unpinned.
	 * @param pageNumber The physical page number of the page entry to be unpinned.
	 */
	void unpinPage(int resourceId, int pageNumber);
	
	/**
	 * Gets all pages/entries for the resource specified by this resource type and
	 * resource ID. A call to this method counts as a request for each individual pages
	 * entry in the sense of how it affects the replacement strategy.
	 * 
	 * @param resourceId The id of the resource for which we seek the pages.
	 * @return An array of all pages for this resource, or an empty array, if no page is
	 *         contained for this resource.
	 */
	CacheableData[] getAllPagesForResource(int resourceId);
	
	/**
	 * Removes all entries/pages belonging to a specific resource from the cache. This method
	 * does not obey pinning: Entries that are pinned are also expelled. The expelled pages
	 * may no longer be found by the {@link #getPage} or {@link #getPageAndPin}.
	 * The entries for the expelled pages will be the next to be evicted from the cache.
	 * If no pages from the given resource are contained in the cache, this method does
	 * nothing.
	 * 
	 * NOTE: This method must not change the size of the cache. It also is not required to
	 * physically destroy the entries for the expelled pages - they simply must no longer
	 * be retrievable from the cache and be the next to be replaced. The byte arrays
	 * behind the expelled pages must be kept in the cache and be returned as evicted
	 * entries once further pages are added to the cache.
	 *  
	 * @param resourceId The id of the resource whose pages are to be replaced.
	 */
	void expelAllPagesForResource(int resourceId);
	
	/**
	 * Gets the capacity of this cache in pages (entries).
	 * 
	 * @return The number of pages (cache entries) that this cache can hold.
	 */
	int getCapacity();
	
	/**
	 * Unpins all entries, such that they can now be evicted from the cache (pinning counter = 0). 
	 * This operation has no impact on the position of the entry in the structure that
	 * decides which entry to evict next.
	 */
	void unpinAllPages();

}
