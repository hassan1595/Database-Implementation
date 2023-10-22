package de.tuberlin.dima.minidb.io.manager;

import de.tuberlin.dima.minidb.io.cache.CacheableData;

/**
 * An entry for the write queue, representing a writing request of an evicted page. Synchronous accesses use the
 * entries as monitors.
 *
 * @author Rudi Poepsel Lemaitre (r.poepsellemaitre@tu-berlin.de)
 */
public final class WriteQueueEntry extends QueueEntry {
    /**
     * The buffer with the binary data of the page.
     */
    private final byte[] bufferToWrite;

    /**
     * The wrapping page object.
     */
    private final CacheableData page;

    /**
     * Flag indicating that the page will reenter the cache after it has been cleaned.
     */
    private boolean immediateCacheRefetch;

    /**
     * Counter indicating that the page is pinned in the cache after it has reentered it.
     */
    private int refetchPinning;

    /**
     * Flag indicating that the page has been successfully written.
     */
    private boolean written;

    /**
     * Creates a new entry for the queue of pages to clean.
     *
     * @param resource
     *        The resource that the page will go to.
     * @param pageNumber
     *        The page number of the page.
     * @param resourceManager
     *        The manager to the resource to write the page to.
     * @param buffer
     *        The buffer with the binary data of the page.
     * @param page
     *        The page object.
     */
    public WriteQueueEntry(Integer resource, int pageNumber, ResourceManager resourceManager, byte[] buffer, CacheableData page) {
        super(resource, pageNumber, resourceManager);

        this.bufferToWrite = buffer;
        this.page = page;
        this.immediateCacheRefetch = false;
        this.refetchPinning = 0;
    }

    /**
     * Gets the page's binary buffer.
     *
     * @return The pages binary buffer.
     */
    public byte[] getBufferToWrite() {
        return this.bufferToWrite;
    }

    /**
     * Gets the wrapping page object of the page to be written.
     *
     * @return The wrapping page object of the page to be written.
     */
    public CacheableData getPage() {
        return this.page;
    }

    /**
     * Sets the entry to be immediately added to the cache again, after the writing
     * has completed.
     */
    public void markForImmediateRefetch(boolean pinning) {
        this.immediateCacheRefetch = true;
        if (pinning) {
            this.refetchPinning++;
        }
    }

    /**
     * Checks if the entry to be immediately added to the cache again, after the writing
     * has completed.
     *
     * @return True, if the page will reenter the cache after it has been written, false
     *         otherwise.
     */
    public boolean isSetForImmediateRefetch() {
        return this.immediateCacheRefetch;
    }

    /**
     * Checks if the page has been successfully written.
     *
     * @return True, if the page has been written, false otherwise.
     */
    public boolean isWritten() {
        return this.written;
    }

    /**
     * Marks the page as successfully written.
     */
    public void markWritten() {
        this.written = true;
    }

    /**
     * Marks the page to be pinned in the cache after it has been re-added.
     *
     * @return true, if the page should be pinned in the cache after it has been re-fetched, false otherwise.
     */
    public boolean isRefetchedPinning() {
        return this.refetchPinning > 0;
    }

    /**
     * Returns the number of pinning requests.
     *
     * @return The number of pinning requests.
     */
    public int getPinning() {
        return this.refetchPinning;
    }
}