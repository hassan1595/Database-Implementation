package de.tuberlin.dima.minidb.io.manager;

import de.tuberlin.dima.minidb.io.cache.CacheableData;
import de.tuberlin.dima.minidb.io.cache.PageCache;

/**
 * An entry for the load queue, representing a loading request of an existing page. Synchronous accesses use the
 * entries as monitors.
 *
 * @author Rudi Poepsel Lemaitre (r.poepsellemaitre@tu-berlin.de)
 */
public final class LoadQueueEntry extends QueueEntry {
    /**
     * The cache to place the page into after the read operation has completed.
     */
    private final PageCache targetCache;

    /**
     * The page created as a result of the read operation. Will be set when the read
     * operation has succeeded.
     */
    private CacheableData resultPage;

    /**
     * Counter indicating that the page should be pinned in the cache after it has been read.
     */
    private int shouldPin;

    /**
     * Flag indicating that the read operation has completed.
     */
    private boolean requestCompleted;

    /**
     * Creates a new load queue entry, representing a loading request of an existing page.
     *
     * @param resource
     *        The resource to load the page from.
     * @param pageNumber
     *        The page number of the page to load.
     * @param resourceHandle
     *        The resource manager to use to load the page.
     * @param targetCache
     *        The cache to which to add the page after loading.
     * @param pin
     *        Flag indicating whether to pin the page in the cache after loading it.
     */
    public LoadQueueEntry(Integer resource, int pageNumber, ResourceManager resourceHandle, PageCache targetCache, boolean pin) {
        super(resource, pageNumber, resourceHandle);

        this.targetCache = targetCache;
        this.requestCompleted = false;
        this.shouldPin = pin ? 1 : 0;
    }

    /**
     * Gets the target of this LoadQueueEntry.
     *
     * @return the target
     */
    public PageCache getTargetCache() {
        return this.targetCache;
    }

    /**
     * Sets the page that has resulted from the successful read operation.
     *
     * @param page
     *        The page created as a result of the read operation.
     */
    public void setResultPage(CacheableData page) {
        this.resultPage = page;
    }

    /**
     * Gets the wrapping page object that resulted from the load.
     *
     * @return The wrapping page object.
     */
    public CacheableData getResultPage() {
        return this.resultPage;
    }

    /**
     * Marks the page as loaded.
     */
    public void setCompleted() {
        this.requestCompleted = true;
    }

    /**
     * Checks whether the page has been loaded.
     *
     * @return true, if the page has already been loaded, false otherwise.
     */
    public boolean isCompleted() {
        return this.requestCompleted;
    }

    /**
     * Checks whether the page should be pinned in the cache after it has been loaded.
     *
     * @return true, if the page should be pinned in the cache after it has been loaded,
     *         false otherwise.
     */
    public boolean shouldPin() {
        return this.shouldPin > 0;
    }

    /**
     * Returns the number of pinning requests for this entry.
     *
     * @return The number of pinning requests.
     */
    public int getPinning() {
        return this.shouldPin;
    }

    /**
     * Increases the pinning counter by 1.
     */
    public void increasePinning() {
        this.shouldPin++;
    }
}