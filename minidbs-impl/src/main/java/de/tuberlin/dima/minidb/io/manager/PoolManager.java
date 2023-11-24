package de.tuberlin.dima.minidb.io.manager;
import de.tuberlin.dima.minidb.Config;
import de.tuberlin.dima.minidb.io.cache.*;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;



public class PoolManager implements BufferPoolManager {
    private HashMap<Integer, ResourceManager> resourceManagers;
    private ConcurrentHashMap<PageSize, PageCache> pageCaches;
    private ConcurrentHashMap<PageSize, ConcurrentLinkedQueue<byte[]>> freeBufferCollection;
    private LinkedBlockingQueue<LoadQueueEntry> simpleLoadQueue;
    private LinkedBlockingQueue<WriteQueueEntry> simpleWriteQueue;
    private Config config;
    private Logger logger;
    private ReadThread readThread;
    private WriteThread writeThread;
    private boolean isClosed;


    public PoolManager(Config config, Logger logger) {
        this.resourceManagers = new HashMap<>();
        this.pageCaches = new ConcurrentHashMap<>();
        // buffers which are currently not in use
        this.freeBufferCollection = new ConcurrentHashMap<>();
        this.simpleLoadQueue = new LinkedBlockingQueue<>();
        this.simpleWriteQueue = new LinkedBlockingQueue<>();

        this.config = config;
        this.logger = logger.getLogger("BufferPoolManager");
        this.isClosed = false;
    }


    private byte[] getFreeBuffer(PageSize pageSize) throws BufferPoolException {
        Queue<byte[]> freeBuffers = this.freeBufferCollection.get(pageSize.getNumberOfBytes());
        byte[] freeBuffer = (byte[]) freeBuffers.poll();
        if (freeBuffer == null) {
            throw new BufferPoolException("No free buffers available");
        }
        return freeBuffer;
    }


    @Override
    public void startIOThreads() throws BufferPoolException {
        this.readThread = new ReadThread();
        this.readThread.setName("ReadThread");
        this.writeThread = new WriteThread();
        this.writeThread.setName("WriteThread");
        this.readThread.start();
        this.writeThread.start();
    }

    @Override
    public void closeBufferPool() {
        // we can shut down the read thread immediately and discard any read requests
        this.isClosed = true;
        this.readThread.shutdown();
        this.writeThread.shutdown();
        this.writeThread.interrupt();
    }


    @Override
    public void registerResource(int id, ResourceManager manager) throws BufferPoolException {
        if (this.isClosed) {
            throw new BufferPoolException("The pool is closed");
        }
        PageSize resourcePageSize = manager.getPageSize();
        // bookkeeping work in case we haven't seen a particular page size before
        if (!this.freeBufferCollection.containsKey(resourcePageSize.getNumberOfBytes())) {
            // create new buffer collection for this page size
            this.freeBufferCollection.put(resourcePageSize.getNumberOfBytes(), new ConcurrentLinkedQueue<>());
            // initialize the buffers in the collection
            for (int i = 0; i < this.config.getNumIOBuffers(); i++) {
                byte[] newByteArray = new byte[resourcePageSize.getNumberOfBytes()];
                this.freeBufferCollection.get(resourcePageSize.getNumberOfBytes()).add(newByteArray);
            }
            // create new page cache with size given by the page size
            PageCache pageCache = new PageCacheClass(resourcePageSize, this.config.getCacheSize(resourcePageSize));
            // todo: block caches, free buffers, write queue, load queue
            this.pageCaches.put(resourcePageSize.getNumberOfBytes(), pageCache);
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
        PageCache pageCache = this.pageCaches.get(pageSize.getNumberOfBytes());
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
            this.simpleLoadQueue.offer(entry);
            // wait for the read to complete
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
        PageCache pageCache = this.pageCaches.get(pageSize.getNumberOfBytes());
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
            this.simpleLoadQueue.offer(entry);
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
        PageCache pageCache = this.pageCaches.get(pageSize.getNumberOfBytes());
        // unpin the page
        pageCache.unpinPage(resourceId, pageNumber);
    }

    @Override
    public void prefetchPage(int resourceId, int pageNumber) throws BufferPoolException {
        if (this.isClosed) {
            throw new BufferPoolException("The pool is closed");
        }
        ResourceManager manager = this.resourceManagers.get(resourceId);
        PageSize pageSize = manager.getPageSize() ;
        PageCache cache = this.pageCaches.get(pageSize.getNumberOfBytes());
        CacheableData cacheData = cache.getPage(resourceId, pageNumber);
        if (cacheData == null) {
            // create a new load queue entry
            LoadQueueEntry entry = new LoadQueueEntry(
                    resourceId,
                    pageNumber,
                    manager,
                    cache,
                    false
            );
            // push the entry to the queue
            this.simpleLoadQueue.offer(entry);
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
            // TODO: check every read/write requests if it is already in the queue
            try {
                PageCache pageCache = this.pageCaches.get(pageSize.getNumberOfBytes());
                EvictedCacheEntry evictedEntry = pageCache.addPageAndPin(page, resourceId);
                if (evictedEntry.getWrappingPage() != null && evictedEntry.getWrappingPage().hasBeenModified()) {
                    WriteQueueEntry entry = new WriteQueueEntry(
                            evictedEntry.getResourceID(),
                            evictedEntry.getPageNumber(),
                            manager,
                            evictedEntry.getBinaryPage(),
                            evictedEntry.getWrappingPage()
                    );
                    simpleWriteQueue.offer(entry);
                } else {
                    byte[] rawPage = evictedEntry.getBinaryPage();
                    ConcurrentLinkedQueue<byte[]> bufferQueue = this.freeBufferCollection.get(pageSize.getNumberOfBytes());
                    bufferQueue.add(rawPage);
                }
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
            PageCache pageCache = this.pageCaches.get(pageSize.getNumberOfBytes());
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
            this.interrupt();

        }
        public void run() {
            while (this.isAlive) {
                // check if the load queue is not empty
                    // TODO: how do determine which load queue to poll from?
                    // => there is a value in the config which describes how big each subqueue can get
                    // => use n = 10 as the difference factor for page number to choose a subqueue depending on the page number
                    // start from the most recent page
                    // TODO: how to retrieve the amount of pages required ? it appears that each entry represents a request for one page
                    // TODO: always reuse buffers from the request queue entries
                LoadQueueEntry request = null;
                try {
                    request = simpleLoadQueue.take();
                    // check the write queue for the page in the request
                    synchronized (simpleWriteQueue) {
                        for (WriteQueueEntry entry : simpleWriteQueue) {
                            if (entry.getResourceId() == request.getResourceId() && entry.getPageNumber() == request.getPageNumber()) {
                                // page is in the write queue => we do not need to load it from disk
                                PageCache cache = request.getTargetCache();
                                try {
                                    if (request.shouldPin()) {
                                        cache.addPageAndPin(entry.getPage(), entry.getResourceId());
                                    } else {
                                        cache.addPage(entry.getPage(), entry.getResourceId());
                                    }
                                }
                                catch (DuplicateCacheEntryException e) {
                                    throw new RuntimeException(e);
                                } catch (CachePinnedException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    this.isAlive = false;
                    return;
                }
                int resourceId = request.getResourceId();
                ResourceManager resourceManager = request.getResourceManager();
                PageSize pageSize = resourceManager.getPageSize();
                try {
                    byte[] buffer = getFreeBuffer(pageSize);
                    CacheableData data = resourceManager.readPageFromResource(buffer, request.getPageNumber());

                    PageCache pageCache = request.getTargetCache();
                    EvictedCacheEntry evictedEntry;
                    synchronized (pageCache) {
                        if (request.shouldPin()) {
                            evictedEntry = pageCache.addPageAndPin(data, resourceId);
                        } else {
                            evictedEntry = pageCache.addPage(data, resourceId);
                        }
                    }
                    if (evictedEntry.getResourceID() != -1 && evictedEntry.getWrappingPage().hasBeenModified()) {
                        // add evicted page to the write queue because it has been modified and cache is hot
                        simpleWriteQueue.offer(new WriteQueueEntry(
                                evictedEntry.getResourceID(),
                                evictedEntry.getPageNumber(),
                                resourceManager,
                                evictedEntry.getBinaryPage(),
                                evictedEntry.getWrappingPage()
                        ));
                    } else {
                        ConcurrentLinkedQueue<byte[]> bufferQueue = freeBufferCollection.get(pageSize.getNumberOfBytes());
                        byte[] rawPage = evictedEntry.getBinaryPage();
                        bufferQueue.add(rawPage);
                        System.out.println("Added buffer back to the pool");
                    }
                    synchronized (request) {
                        request.setResultPage(data);
                        request.setCompleted();
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


    private class WriteThread extends Thread {
        private volatile boolean isAlive = true;

        public void shutdown() {
            this.isAlive = false;
        }
        public void run() {
            while (this.isAlive || !simpleWriteQueue.isEmpty()) {
                WriteQueueEntry entry;
                try {
                    entry = simpleWriteQueue.take();
                    ResourceManager resourceManager = entry.getResourceManager();
                    PageSize pageSize = resourceManager.getPageSize();
                    try {
                        resourceManager.writePageToResource(entry.getBufferToWrite(), entry.getPage());
                        // return the buffer to the free buffer collection
                        ConcurrentLinkedQueue<byte[]> bufferQueue = freeBufferCollection.get(pageSize.getNumberOfBytes());
                        bufferQueue.add(entry.getBufferToWrite());
                    } catch (IOException e) {
                        logger.log(Level.SEVERE, e.toString());
                    }
                } catch (InterruptedException e) {
                    return;
                }
            }
        }
    }
}
