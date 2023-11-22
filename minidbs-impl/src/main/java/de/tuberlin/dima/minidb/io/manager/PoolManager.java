package de.tuberlin.dima.minidb.io.manager;
import de.tuberlin.dima.minidb.Config;
import de.tuberlin.dima.minidb.io.cache.*;
import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;


public class PoolManager implements BufferPoolManager {


    private HashMap<Integer, ResourceManager> resourceManagers;
    private HashMap<PageSize, PageCacheClass> pageCaches;
    private HashMap<PageSize, LinkedList<byte[]>> freeBufferCollection;

    private List<LinkedList<LoadQueueEntry>> loadQueue;
    // simple queue without command stuff
    private LinkedList<LoadQueueEntry> simpleLoadQueue;
    private LinkedList<WriteQueueEntry> simpleWriteQueue;
    private List<LinkedList<WriteQueueEntry>> writeQueue;

    private Config config;
    private Logger logger;

    private ReadThread readThread;
    private WriteThread writeThread;

    private boolean isClosed;


    public PoolManager(Config config, Logger logger) {
        this.resourceManagers = new HashMap<>();
        this.pageCaches = new HashMap<>();
        // buffers which are currently not in use
        this.freeBufferCollection = new HashMap<>();
        this.simpleLoadQueue = new LinkedList<>();
        this.simpleWriteQueue = new LinkedList<>();

        this.config = config;
        this.logger = logger.getLogger("BufferPoolManager");
        this.isClosed = false;
    }


    private byte[] getFreeBuffer(PageSize pageSize) throws BufferPoolException {
        if (this.freeBufferCollection.get(pageSize).isEmpty()) {
            throw new BufferPoolException("No free buffers available");
        }
        Queue freeBuffers = this.freeBufferCollection.get(pageSize);
        return (byte[]) freeBuffers.poll();
    }


    @Override
    public void startIOThreads() throws BufferPoolException {
        this.readThread = new ReadThread();
        this.writeThread = new WriteThread();
        this.readThread.start();
        this.writeThread.start();
    }

    @Override
    public void closeBufferPool() {
        // we can shut down the read thread immediately and discard any read requests
        this.isClosed = true;
        this.readThread.shutdown();
        this.simpleLoadQueue.clear();
        for (ResourceManager manager : this.resourceManagers.values()) {
            try {
                manager.closeResource();
            } catch (IOException e) {
                logger.log(Level.SEVERE, e.toString());
            }
        }
        // wait for the write thread to finish
        this.writeThread.shutdown();
        while (this.writeThread.isProcessing()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                logger.log(Level.SEVERE, e.toString());
            }
        }
    }


    @Override
    public void registerResource(int id, ResourceManager manager) throws BufferPoolException {
        if (this.isClosed) {
            throw new BufferPoolException("The pool is closed");
        }
        PageSize resourcePageSize = manager.getPageSize();
        // bookkeeping work in case we haven't seen a particular page size before
        if (!this.freeBufferCollection.containsKey(resourcePageSize)) {
            // create new buffer collection for this page size
            this.freeBufferCollection.put(resourcePageSize, new LinkedList<>());
            // initialize the buffers in the collection
            for (int i = 0; i < this.config.getNumIOBuffers(); i++) {
                this.freeBufferCollection.get(resourcePageSize).add(new byte[resourcePageSize.getNumberOfBytes()]);
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
        if (this.isClosed) {
            throw new BufferPoolException("The pool is closed");
        }
        // fetch the resource manager
        ResourceManager resourceManager = this.resourceManagers.get(resourceId);
        // fetch the page size of the resource manager
        PageSize pageSize = resourceManager.getPageSize();
        // fetch the page cache for this page size
        PageCache pageCache = this.pageCaches.get(pageSize);
        // check if the page is in the cache
        CacheableData data = pageCache.getPageAndPin(resourceId, pageNumber);
        if (data != null) {
            // page is in cache and we do not involve the disk
            return data;
        } else {
            // we have a cache miss and need to load the page from disk
            // create a new load queue entry
            LoadQueueEntry entry = new LoadQueueEntry(
                    resourceId,
                    pageNumber,
                    resourceManager,
                    pageCache,
                    true
            );
            // push the entry to the queue
            // TODO: group page requests by page id as given in lecture slides
            this.simpleLoadQueue.push(entry);
            synchronized (entry) {
                while (!entry.isCompleted()) {
                    try {
                        entry.wait();
                    } catch (InterruptedException e) {
                        logger.log(Level.SEVERE, e.toString());
                    }
                }
            }
            return entry.getResultPage();
        }
    }

    @Override
    public CacheableData unpinAndGetPageAndPin(int resourceId, int unpinPageNumber, int getPageNumber) throws BufferPoolException, IOException {
        if (this.isClosed) {
            throw new BufferPoolException("The pool is closed");
        }
        // unpin the given page
        this.unpinPage(resourceId, unpinPageNumber);
        // fetch the resource manager
        ResourceManager resourceManager = this.resourceManagers.get(resourceId);
        // fetch the page size of the resource manager
        PageSize pageSize = resourceManager.getPageSize();
        // fetch the page cache for this page size
        PageCache pageCache = this.pageCaches.get(pageSize);
        // check if the page is in the cache
        CacheableData data = pageCache.getPageAndPin(resourceId, getPageNumber);
        if (data != null) {
            // page is in cache and we do not involve the disk
            return data;
        } else {
            // we have a cache miss and need to load the page from disk
            // create a new load queue entry
            LoadQueueEntry entry = new LoadQueueEntry(
                    resourceId,
                    getPageNumber,
                    resourceManager,
                    pageCache,
                    true
            );
            // push the entry to the queue
            // TODO: group page requests by page id as given in lecture slides
            this.simpleLoadQueue.push(entry);
            synchronized (entry) {
                while (!entry.isCompleted()) {
                    try {
                        entry.wait();
                    } catch (InterruptedException e) {
                        logger.log(Level.SEVERE, e.toString());
                    }
                }
            }
            return entry.getResultPage();
        }
    }


    @Override
    public void unpinPage(int resourceId, int pageNumber) {
        // get the resource manager
        ResourceManager manager = this.resourceManagers.get(resourceId);
        // get the page size of the resource manager
        PageSize pageSize = manager.getPageSize();
        // get the page cache
        PageCache pageCache = this.pageCaches.get(pageSize);
        // unpin the page
        pageCache.unpinPage(resourceId, pageNumber);
    }

    @Override
    public void prefetchPage(int resourceId, int pageNumber) throws BufferPoolException {
        if (this.isClosed) {
            throw new BufferPoolException("The pool is closed");
        }
    }

    @Override
    public void prefetchPages(int resourceId, int startPageNumber, int endPageNumber) throws BufferPoolException {
        if (this.isClosed) {
            throw new BufferPoolException("The pool is closed");
        }
    }


    @Override
    public CacheableData createNewPageAndPin(int resourceId) throws BufferPoolException, IOException {
        // TODO: are we correct in assuming that this function is not involving the disk?
        // TODO: if that is the case, why are we getting more pages in the corresponding test than we have buffers?
        // TODO: what is the lifecycle of a buffer in this case?
        if (this.isClosed) {
            throw new BufferPoolException("The pool is closed");
        }
        // get the manager for the provided id
        ResourceManager manager = this.resourceManagers.get(resourceId);
        if (manager == null) {
             throw new BufferPoolException("Manager has not been registered yet");
        }
        // get the managers page size
        PageSize pageSize = manager.getPageSize();
        // get a free buffer for the managers page size
        byte[] buffer = this.getFreeBuffer(pageSize);
        try {
            CacheableData page = manager.reserveNewPage(buffer);
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

    @Override
    public CacheableData createNewPageAndPin(int resourceId, Enum<?> type) throws BufferPoolException, IOException {
        if (this.isClosed) {
            throw new BufferPoolException("The pool is closed");
        }
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
                if (!simpleLoadQueue.isEmpty()) {
                    // TODO: how do determine which load queue to poll from?
                    // TODO: how to retrieve the amount of pages required ? it appears that each entry represents a request for one page
                    LoadQueueEntry request = simpleLoadQueue.poll();
                    int resourceId = request.getResourceId();
                    ResourceManager resourceManager = request.getResourceManager();
                    PageSize pageSize = resourceManager.getPageSize();
                    try {
                        byte[] buffer = getFreeBuffer(pageSize);
                        CacheableData data = resourceManager.readPageFromResource(buffer, request.getPageNumber());
                        PageCache pageCache = request.getTargetCache();
                        EvictedCacheEntry evictedEntry;
                        if (request.shouldPin()) {
                            evictedEntry = pageCache.addPageAndPin(data, resourceId);
                        } else {
                            evictedEntry = pageCache.addPage(data, resourceId);
                        }
                        if (evictedEntry != null && evictedEntry.getWrappingPage().hasBeenModified()) {
                            // add evicted page to the write queue because it has been modified
                            simpleWriteQueue.push(new WriteQueueEntry(
                                    evictedEntry.getResourceID(),
                                    evictedEntry.getPageNumber(),
                                    resourceManager,
                                    evictedEntry.getBinaryPage(),
                                    evictedEntry.getWrappingPage()
                            ));
                        }
                        freeBufferCollection.get(pageSize).add(buffer);
                        request.setResultPage(data);
                        request.setCompleted();
                        synchronized (request) {
                            request.notifyAll();
                        }
                    } catch (BufferPoolException e) {
                        logger.log(Level.SEVERE, e.toString());
                    } catch (IOException e) {
                        logger.log(Level.SEVERE, e.toString());
                    } catch (DuplicateCacheEntryException e) {
                        logger.log(Level.SEVERE, e.toString());
                    } catch (CachePinnedException e) {
                        logger.log(Level.SEVERE, e.toString());
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

        public boolean isProcessing() {
            return this.isAlive || !simpleWriteQueue.isEmpty();
        }

        public void run() {
            while (this.isAlive) {
                // check if the write queue is not empty
                if (!simpleWriteQueue.isEmpty()) {
                    // get the request data
                    WriteQueueEntry entry = simpleWriteQueue.poll();
                    ResourceManager resourceManager = entry.getResourceManager();
                    PageSize pageSize = resourceManager.getPageSize();
                    try {
                        resourceManager.writePageToResource(entry.getBufferToWrite(), entry.getPage());
                        // return the buffer to the free buffer collection
                        freeBufferCollection.get(pageSize).add(entry.getBufferToWrite());
                    } catch (IOException e) {
                        logger.log(Level.SEVERE, e.toString());
                    }

                }
            }
        }
    }
}
