package de.tuberlin.dima.minidb.io.manager;
import de.tuberlin.dima.minidb.Config;
import de.tuberlin.dima.minidb.io.cache.*;

import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

public class PoolManager implements BufferPoolManager {


    private HashMap<Integer, ResourceManager> resourceManagers;
    private HashMap<PageSize, PageCacheClass> pageCaches;
    private HashMap<PageSize, List<byte[]>> bufferCollection;
    private HashMap<PageSize, Queue<byte[]>> freeBufferCollection;

    private List<LinkedList<LoadQueueEntry>> loadQueue;
    private List<LinkedList<WriteQueueEntry>> writeQueue;

    private Config config;
    private Logger logger;

    public PoolManager(Config config, Logger logger) {
        this.resourceManagers = new HashMap<>();
        this.pageCaches = new HashMap<>();
        this.bufferCollection = new HashMap<>();
        // buffers which are currently not in use
        this.freeBufferCollection = new HashMap<>();
        this.loadQueue = new ArrayList<>();
        this.writeQueue = new ArrayList<>();

        this.config = config;
        this.logger = logger;
    }


    private byte[] getFreeBuffer(PageSize pageSize) throws BufferPoolException {
        Queue queue = this.freeBufferCollection.get(pageSize);
        if (queue.isEmpty()) {
            throw new BufferPoolException("No free buffers available");
        }
        return (byte[]) queue.poll();
    }


    @Override
    public void startIOThreads() throws BufferPoolException {
        ReadThread readThread = new ReadThread();
        WriteThread writeThread = new WriteThread();
        readThread.start();
        writeThread.start();

    }

    @Override
    public void closeBufferPool() {

    }

    @Override
    public void registerResource(int id, ResourceManager manager) throws BufferPoolException {
        PageSize resourcePageSize = manager.getPageSize();
        // bookkeeping work in case we haven't seen a particular page size before
        if (!this.bufferCollection.containsKey(resourcePageSize)) {
            // create new buffer collection for this page size
            this.bufferCollection.put(resourcePageSize, new ArrayList<>());
            this.freeBufferCollection.put(resourcePageSize, new LinkedList<>());
            // initialize the buffers in the collection
            for (int i = 0; i < this.config.getNumIOBuffers(); i++) {
                this.bufferCollection.get(resourcePageSize).add(new byte[resourcePageSize.getNumberOfBytes()]);
                // add the created byte[] pointer to the free buffer collection
                this.freeBufferCollection.get(resourcePageSize).add(this.bufferCollection.get(resourcePageSize).get(i));
            }
            // create new page cache with size given by the page size
            PageCacheClass pageCache = new PageCacheClass(resourcePageSize, this.config.getCacheSize(resourcePageSize));
            this.pageCaches.put(resourcePageSize, pageCache);
        }
        // register the resource manager
        this.resourceManagers.put(id, manager);
    }

    @Override
    public CacheableData getPageAndPin(int resourceId, int pageNumber) throws BufferPoolException, IOException {
        return null;
    }

    @Override
    public CacheableData unpinAndGetPageAndPin(int resourceId, int unpinPageNumber, int getPageNumber) throws BufferPoolException, IOException {
        return null;
    }

    @Override
    public void unpinPage(int resourceId, int pageNumber) {

    }

    @Override
    public void prefetchPage(int resourceId, int pageNumber) throws BufferPoolException {

    }

    @Override
    public void prefetchPages(int resourceId, int startPageNumber, int endPageNumber) throws BufferPoolException {

    }

    @Override
    public CacheableData createNewPageAndPin(int resourceId) throws BufferPoolException, IOException {
        // get the manager for the provided id
        ResourceManager manager = this.resourceManagers.get(resourceId);
        // get the managers page size
        PageSize pageSize = manager.getPageSize();
        // get a free buffer for the managers page size
        byte[] buffer = this.getFreeBuffer(pageSize);
        try {
            CacheableData page = manager.reserveNewPage(buffer);
            // get the page cache for this page size
            // TODO: are we supposed to add the page to page cache when creating it here?
            // TODO: does this method actually already leverage the write/read threads?
            PageCacheClass pageCache = this.pageCaches.get(pageSize);
            try {
                pageCache.addPageAndPin(page, resourceId);
            } catch (DuplicateCacheEntryException e) {
                throw new BufferPoolException("Page already in cache");
            } catch (CachePinnedException e) {
                throw new BufferPoolException("All entries in the cache are pinned");
            }
            return page;
        } catch (PageFormatException e) {
            throw new BufferPoolException("Manager could not reserve new page");
        }
    }

    @Override
    public CacheableData createNewPageAndPin(int resourceId, Enum<?> type) throws BufferPoolException, IOException {
        // get the manager for the provided id
        ResourceManager manager = this.resourceManagers.get(resourceId);
        // get the managers page size
        PageSize pageSize = manager.getPageSize();
        // get a free buffer for the managers page size
        byte[] buffer = this.getFreeBuffer(pageSize);
        try {
            CacheableData page = manager.reserveNewPage(buffer, type);
            // get the page cache for this page size
            // TODO: are we supposed to add the page to page cache when creating it here?
            PageCacheClass pageCache = this.pageCaches.get(pageSize);
            try {
                pageCache.addPageAndPin(page, resourceId);
            } catch (DuplicateCacheEntryException e) {
                throw new BufferPoolException("Page already in cache");
            } catch (CachePinnedException e) {
                throw new BufferPoolException("All entries in the cache are pinned");
            }
            return page;
        } catch (PageFormatException e) {
            throw new BufferPoolException("Manager could not reserve new page");
        }
    }

    private class ReadThread extends Thread {
        private volatile boolean isAlive = true;

        public void shutdown() {
            this.isAlive = false;
        }

        public void run() {
            while (this.isAlive) {
                // check if the load queue is not empty
                synchronized (loadQueue) {
                    if (loadQueue.isEmpty()) {
                        try {
                            loadQueue.wait();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    } else {
                        // get the request
                        LoadQueueEntry request = loadQueue.get(0).get(0);
                        // retrieve the amount of free buffers necessary to complete the request

                    }
                }
            }
        }


    }
    private class WriteThread extends Thread {
        private volatile boolean isAlive = true;
        public void shutdown() {
            this.isAlive = false;
        }

        public void run() {
            while (this.isAlive) {
                // process pages
            }
        }
    }
}
