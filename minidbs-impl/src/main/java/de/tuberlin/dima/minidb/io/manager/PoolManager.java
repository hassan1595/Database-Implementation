package de.tuberlin.dima.minidb.io.manager;
import de.tuberlin.dima.minidb.Config;
import de.tuberlin.dima.minidb.io.cache.*;

import javax.annotation.processing.SupportedSourceVersion;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
TODO: group page requests by page id as given in lecture slides
TODO: how do determine which load queue to poll from?
TODO: always reuse buffers from the request queue entries
 */




public class PoolManager implements BufferPoolManager {
    private HashMap<Integer, ResourceManager> resourceManagers;
    private ConcurrentHashMap<Integer, PageCache> pageCaches;
    private ConcurrentHashMap<Integer, LinkedBlockingQueue<byte[]>> freeBufferCollection;
    private LinkedBlockingQueue<LoadQueueEntry> simpleLoadQueue;
    private LinkedBlockingQueue<WriteQueueEntry> simpleWriteQueue;
    private Config config;
    private Logger logger;
    private ReadThread readThread;
    private WriteThread writeThread;
    private boolean isClosed;

    private final ReadWriteLock cacheLock = new ReentrantReadWriteLock();
    private final ReentrantLock readEntryLock = new ReentrantLock();
    private final Condition condition = readEntryLock.newCondition();



    public PoolManager(Config config, Logger logger) {
        this.resourceManagers = new HashMap<>();
        this.pageCaches = new ConcurrentHashMap<>();
        this.freeBufferCollection = new ConcurrentHashMap<>();
        this.simpleLoadQueue = new LinkedBlockingQueue<>();
        this.simpleWriteQueue = new LinkedBlockingQueue<>();

        this.config = config;
        // required for throwing exceptions after the closing method has been called
        this.isClosed = false;
        this.logger  = Logger.getLogger("BufferPoolManager");
//        this.logger .setLevel(Level.OFF);
        logger.info(getLogMessage("Created buffer pool manager with %d IO buffers", config.getNumIOBuffers()));
    }

    private String getLogMessage(String message, Object... args) {
        String formatted = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));
        return String.format("[%s] [%s]", Thread.currentThread().getName(), formatted) + " " + String.format(message, args);
    }


    private byte[] getFreeBuffer(PageSize pageSize) throws BufferPoolException {
        LinkedBlockingQueue<byte[]> freeBuffers = this.freeBufferCollection.get(pageSize.getNumberOfBytes());
        byte[] freeBuffer;
        try {
            this.logger.info(getLogMessage("Trying to pop buffer from free buffer collection with size %d", pageSize.getNumberOfBytes()));
            freeBuffer = freeBuffers.take();
            this.logger.info(this.getLogMessage(
                    "Popped buffer from free buffer collection with size %d. %d Buffers in the pool",
                    pageSize.getNumberOfBytes(),
                    freeBuffers.size()
            ));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
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
        this.logger.info("Started IO threads");
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
            this.freeBufferCollection.put(resourcePageSize.getNumberOfBytes(), new LinkedBlockingQueue<>());
            // initialize the buffers in the collection
            for (int i = 0; i < this.config.getNumIOBuffers(); i++) {
                byte[] newByteArray = new byte[resourcePageSize.getNumberOfBytes()];
                this.freeBufferCollection.get(resourcePageSize.getNumberOfBytes()).offer(newByteArray);
            }
            // create new page cache with size given by the page size
            PageCache pageCache = new PageCacheClass(resourcePageSize, this.config.getCacheSize(resourcePageSize));
            // todo: block caches, free buffers, write queue, load queue
            this.pageCaches.put(resourcePageSize.getNumberOfBytes(), pageCache);
        }
        // register the resource manager
        this.resourceManagers.put(id, manager);
        this.logger.info(this.getLogMessage("Registered resource manager for resource %d", id));
    }

    @Override
    public CacheableData getPageAndPin(int resourceId, int pageNumber) throws BufferPoolException, IOException {
        this.logger.info(this.getLogMessage("Trying to get page %d of resource %d", pageNumber, resourceId));
        if (this.isClosed) {
            throw new BufferPoolException("The pool is closed");
        }
        LoadQueueEntry entry = null;
        synchronized (simpleLoadQueue){
        // fetch the resource manager
        ResourceManager resourceManager = this.resourceManagers.get(resourceId);
        // fetch the page size of the resource manager
        PageSize pageSize = resourceManager.getPageSize();
        // fetch the page cache for this page size
        PageCache pageCache = this.pageCaches.get(pageSize.getNumberOfBytes());
        CacheableData data = null;
        synchronized (pageCache){
             data = pageCache.getPageAndPin(resourceId, pageNumber);
        }

        if (data != null) {
            // page is in cache and we do not involve the disk
            this.logger.info(this.getLogMessage("Returning cache hit for page %d of resource %d", pageNumber, resourceId));
            return data;
        } else {
                // we have a cache miss and need to load the page from disk
                // create a new load queue entry
                this.logger.info(this.getLogMessage("Cache miss for page %d of resource %d", pageNumber, resourceId));
                // check if the entry is already in the load queue



                    for (LoadQueueEntry queueEntry: simpleLoadQueue) {
                        if (queueEntry.getResourceId() == resourceId && queueEntry.getPageNumber() == pageNumber) {
                            entry = queueEntry;
                            this.logger.info(this.getLogMessage("Found existing load queue entry for page %d of resource %d", pageNumber, resourceId));
                            break;
                        }
                    }

                    if (entry == null) {
                        entry = new LoadQueueEntry(
                                resourceId,
                                pageNumber,
                                resourceManager,
                                pageCache,
                                true
                        );

                        boolean offerResult = this.simpleLoadQueue.offer(entry);
                        if(simpleLoadQueue.size() == 1){
                            simpleLoadQueue.notifyAll();
                        }
                        this.logger.info(this.getLogMessage("Offered load queue entry for page %d of resource %d to load queue with result %s", pageNumber, resourceId, offerResult));
                }



            }
        }
            synchronized (entry) {
                while (!entry.isCompleted()) {
                    try {
                        this.logger.info(this.getLogMessage("Waiting for load queue entry for page %d of resource %d to complete", pageNumber, resourceId));
                        entry.wait();
                    } catch (InterruptedException e) {
                        logger.log(Level.SEVERE, e.toString());
                        break;
                    }
                }
                logger.info(getLogMessage("Got notified that entry for page %d of resource %d is complete", pageNumber, resourceId));
                return entry.getResultPage();
            }

    }

    @Override
    public CacheableData unpinAndGetPageAndPin(int resourceId, int unpinPageNumber, int getPageNumber) throws BufferPoolException {
        if (this.isClosed) {
            throw new BufferPoolException("The pool is closed");
        }
        LoadQueueEntry entry = null;
        synchronized (simpleLoadQueue){
        // unpin the given page
        this.unpinPage(resourceId, unpinPageNumber);
        // fetch the resource manager
        ResourceManager resourceManager = this.resourceManagers.get(resourceId);
        // fetch the page size of the resource manager
        PageSize pageSize = resourceManager.getPageSize();
        // fetch the page cache for this page size
        PageCache pageCache = this.pageCaches.get(pageSize.getNumberOfBytes());
        // check if the page is in the cache
        CacheableData data = null;
        synchronized (pageCache){
            data = pageCache.getPageAndPin(resourceId, getPageNumber);
        }
        if (data != null) {
            // page is in cache and we do not involve the disk
            return data;
        } else {
            // we have a cache miss and need to load the page from disk
            // create a new load queue entry
            this.logger.info(this.getLogMessage("Cache miss for page %d of resource %d", getPageNumber, resourceId));
            // check if the entry is already in the load queue



                for (LoadQueueEntry queueEntry: simpleLoadQueue) {
                    if (queueEntry.getResourceId() == resourceId && queueEntry.getPageNumber() == getPageNumber) {
                        entry = queueEntry;
                        entry.increasePinning();
                        this.logger.info(this.getLogMessage("Found existing load queue entry for page %d of resource %d", getPageNumber, resourceId));
                        break;
                    }
                }

                if (entry == null) {
                    entry = new LoadQueueEntry(
                            resourceId,
                            getPageNumber,
                            resourceManager,
                            pageCache,
                            true
                    );
                    entry.increasePinning();

                    boolean offerResult = this.simpleLoadQueue.offer(entry);
                    if(simpleLoadQueue.size() == 1){
                        simpleLoadQueue.notifyAll();
                    }
                    this.logger.info(this.getLogMessage("Offered load queue entry for page %d of resource %d to load queue with result %s", getPageNumber, resourceId, offerResult));



            }


            }
        }
            synchronized (entry) {
                while (!entry.isCompleted()) {
                    try {
                        this.logger.info(this.getLogMessage("Waiting for load queue entry for page %d of resource %d to complete", getPageNumber, resourceId));
                        entry.wait();
                    } catch (InterruptedException e) {
                        logger.log(Level.SEVERE, e.toString());
                        break;
                    }
                }
                logger.info(getLogMessage("Got notified that entry for page %d of resource %d is complete", getPageNumber, resourceId));
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
        synchronized (pageCache){
            pageCache.unpinPage(resourceId, pageNumber);
        }

    }

    @Override
    public void prefetchPage(int resourceId, int pageNumber) throws BufferPoolException {
        if (this.isClosed) {
            throw new BufferPoolException("The pool is closed");
        }
        synchronized (simpleLoadQueue){
        ResourceManager resourceManager = this.resourceManagers.get(resourceId);
        PageSize pageSize = resourceManager.getPageSize() ;
        PageCache pageCache = this.pageCaches.get(pageSize.getNumberOfBytes());
        CacheableData cacheData = null;
        synchronized (pageCache){
            cacheData = pageCache.getPage(resourceId, pageNumber);
        }

        if (cacheData == null) {
            // we have a cache miss and need to load the page from disk
            // create a new load queue entry
            this.logger.info(this.getLogMessage("Cache miss for page %d of resource %d", pageNumber, resourceId));
            // check if the entry is already in the load queue
            LoadQueueEntry entry = null;


                for (LoadQueueEntry queueEntry: simpleLoadQueue) {
                    if (queueEntry.getResourceId() == resourceId && queueEntry.getPageNumber() == pageNumber) {
                        entry = queueEntry;
                        this.logger.info(this.getLogMessage("Found existing load queue entry for page %d of resource %d", pageNumber, resourceId));
                        break;
                    }


                }

                if (entry == null) {
                    entry = new LoadQueueEntry(
                            resourceId,
                            pageNumber,
                            resourceManager,
                            pageCache,
                            true
                    );
                    boolean offerResult = this.simpleLoadQueue.offer(entry);
                    if(simpleLoadQueue.size() == 1){
                        simpleLoadQueue.notifyAll();
                    }
                    this.logger.info(this.getLogMessage("Offered load queue entry for page %d of resource %d to load queue with result %s", pageNumber, resourceId, offerResult));
                }
            }





        }
    }

    @Override
    public void prefetchPages(int resourceId, int startPageNumber, int endPageNumber) throws BufferPoolException {
        if (this.isClosed) {
            throw new BufferPoolException("The pool is closed");
        }

        for(int pageNumber = startPageNumber; pageNumber <= endPageNumber; pageNumber ++){
            this.prefetchPage(resourceId, pageNumber);
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
            logger.info(getLogMessage("Reserved new page %d of resource %d", page.getPageNumber(), resourceId));
            // get the page cache for this page size
            try {
                PageCache pageCache = this.pageCaches.get(pageSize.getNumberOfBytes());
                EvictedCacheEntry evictedEntry = null;
                synchronized (pageCache) {
                    evictedEntry = pageCache.addPageAndPin(page, resourceId);
                    logger.info(getLogMessage("Got evicted entry for page %d of resource %d", evictedEntry.getPageNumber(), evictedEntry.getResourceID()));
                }
                if (evictedEntry.getWrappingPage() != null && evictedEntry.getWrappingPage().hasBeenModified()) {
                    WriteQueueEntry entry = new WriteQueueEntry(
                            evictedEntry.getResourceID(),
                            evictedEntry.getPageNumber(),
//                            heereee
                            this.resourceManagers.get(evictedEntry.getResourceID()),
                            evictedEntry.getBinaryPage(),
                            evictedEntry.getWrappingPage()
                    );
                    boolean offerResult = simpleWriteQueue.offer(entry);
                    logger.info(getLogMessage("Offered entry for page %d of resource %d to write queue with result %s because page has been modified",
                            evictedEntry.getPageNumber(), evictedEntry.getResourceID(), offerResult));
                } else {
                    byte[] rawPage = evictedEntry.getBinaryPage();
                    LinkedBlockingQueue<byte[]> bufferQueue = this.freeBufferCollection.get(pageSize.getNumberOfBytes());
                    boolean offerResult = bufferQueue.offer(rawPage);
                    logger.info(getLogMessage("Offered buffer from evicted entry for page %d of resource %d back to the pool. %d buffers in the pool",
                            evictedEntry.getPageNumber(), evictedEntry.getResourceID(), bufferQueue.size()));
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
        if (manager == null) {
            throw new BufferPoolException("Manager has not been registered yet");
        }
        // get the managers page size
        PageSize pageSize = manager.getPageSize();
        // get a free buffer for the managers page size
        byte[] buffer = this.getFreeBuffer(pageSize);
        try {
            CacheableData page = manager.reserveNewPage(buffer, type);
            logger.info(getLogMessage("Reserved new page %d of resource %d", page.getPageNumber(), resourceId));
            // get the page cache for this page size
            try {
                PageCache pageCache = this.pageCaches.get(pageSize.getNumberOfBytes());
                EvictedCacheEntry evictedEntry = null;
                synchronized (pageCache) {
                    evictedEntry = pageCache.addPageAndPin(page, resourceId);
                    logger.info(getLogMessage("Got evicted entry for page %d of resource %d", evictedEntry.getPageNumber(), evictedEntry.getResourceID()));
                }
                if (evictedEntry.getWrappingPage() != null && evictedEntry.getWrappingPage().hasBeenModified()) {
                    WriteQueueEntry entry = new WriteQueueEntry(
                            evictedEntry.getResourceID(),
                            evictedEntry.getPageNumber(),
                            this.resourceManagers.get(evictedEntry.getResourceID()),
                            evictedEntry.getBinaryPage(),
                            evictedEntry.getWrappingPage()
                    );
                    boolean offerResult = simpleWriteQueue.offer(entry);
                    logger.info(getLogMessage("Offered entry for page %d of resource %d to write queue with result %s because page has been modified",
                            evictedEntry.getPageNumber(), evictedEntry.getResourceID(), offerResult));
                } else {
                    byte[] rawPage = evictedEntry.getBinaryPage();
                    LinkedBlockingQueue<byte[]> bufferQueue = this.freeBufferCollection.get(pageSize.getNumberOfBytes());
                    boolean offerResult = bufferQueue.offer(rawPage);
                    logger.info(getLogMessage("Offered buffer from evicted entry for page %d of resource %d back to the pool. %d buffers in the pool",
                            evictedEntry.getPageNumber(), evictedEntry.getResourceID(), bufferQueue.size()));
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
    private class ReadThread extends Thread {
        private volatile boolean isAlive = true;

        public void shutdown() {
            this.isAlive = false;
            this.interrupt();

        }
        public void run() {
            while (this.isAlive) {
                // check if the load queue is not empty
                // => there is a value in the config which describes how big each subqueue can get
                // => use n = 10 as the difference factor for page number to choose a subqueue depending on the page number
                // start from the most recent page

                LoadQueueEntry request = null;
                int resourceId = -1;
                ResourceManager resourceManager = null;
                CacheableData requestedData = null;

                synchronized (simpleLoadQueue){
                    while(simpleLoadQueue.peek() == null)
                    {
                            try {
                                simpleLoadQueue.wait();
                            } catch (InterruptedException e) {
                                this.isAlive = false;
                                break;
                            }

                    }
                    request = simpleLoadQueue.peek();
                }
                if(!this.isAlive){
                    break;
                }




//                deadlock because eec thread thinks it is getting the required resource but it was already in cache

                resourceId = request.getResourceId();
                resourceManager = request.getResourceManager();
                logger.info(getLogMessage("Took load queue entry for page %d of resource %d", request.getPageNumber(), request.getResourceId()));
                // check the write queue for the page in the request
                synchronized (simpleWriteQueue) {
                        for (WriteQueueEntry entry : simpleWriteQueue) {
                            if (entry.getResourceId() == request.getResourceId() && entry.getPageNumber() == request.getPageNumber()) {
                                // page is in the write queue => we do not need to load it from disk
                                logger.info(getLogMessage("Page %d of resource %d is in the write queue", request.getPageNumber(), request.getResourceId()));
                                PageCache pagecache = request.getTargetCache();
                                requestedData = entry.getPage();
                                if(request.shouldPin()){
                                    for(int i =0; i < request.getPinning(); i ++){
                                        entry.markForImmediateRefetch(request.shouldPin());
                                    }
                                }
                                else{
                                    entry.markForImmediateRefetch(false);
                                }

                            }
                        }
                    }

                try {

                    if( requestedData == null) {
                        byte[] buffer;
                        buffer = getFreeBuffer(resourceManager.getPageSize());
                        requestedData = resourceManager.readPageFromResource(buffer, request.getPageNumber());

                    }
                    logger.info(getLogMessage("Read page %d of resource %d from disk", request.getPageNumber(), resourceId));

                    PageCache pageCache = request.getTargetCache();
                    EvictedCacheEntry evictedEntry;
                    if (request.shouldPin()) {
                        synchronized (pageCache){




                            evictedEntry = pageCache.addPageAndPin(requestedData, resourceId);
                            for(int i = 0; i < request.getPinning() -1 ; i++){
                                pageCache.getPageAndPin(resourceId, requestedData.getPageNumber());
                            }
                            logger.info(getLogMessage("Got evicted entry for page %d of resource %d", evictedEntry.getPageNumber(), evictedEntry.getResourceID()));
                            logger.info(getLogMessage("Added page %d of resource %d to cache and pinned it", request.getPageNumber(), resourceId));
                        }

                    } else {
                        synchronized (pageCache)
                        {
                            evictedEntry = pageCache.addPage(requestedData, resourceId);
                            logger.info(getLogMessage("Got evicted entry for page %d of resource %d", evictedEntry.getPageNumber(), evictedEntry.getResourceID()));
                            logger.info(getLogMessage("Added page %d of resource %d to cache", request.getPageNumber(), resourceId));
                        }

                    }
                    if (evictedEntry.getResourceID() != -1 && evictedEntry.getWrappingPage().hasBeenModified()) {
                        // add evicted page to the write queue because it has been modified and cache is hot
                        boolean offerResult = simpleWriteQueue.offer(new WriteQueueEntry(
                                evictedEntry.getResourceID(),
                                evictedEntry.getPageNumber(),
                                resourceManagers.get(evictedEntry.getResourceID()),
                                evictedEntry.getBinaryPage(),
                                evictedEntry.getWrappingPage()
                        ));

                        logger.info(getLogMessage("Offered write queue entry for page %d of resource %d to write queue with result %s",
                                evictedEntry.getPageNumber(), evictedEntry.getResourceID(), offerResult));
                    } else {
                        LinkedBlockingQueue<byte[]> bufferQueue = freeBufferCollection.get(resourceManager.getPageSize().getNumberOfBytes());
                        byte[] rawPage = evictedEntry.getBinaryPage();
                        bufferQueue.offer(rawPage);
                        logger.info(getLogMessage("Added buffer back to the pool. %d buffers in the pool", bufferQueue.size()));
                    }

                    synchronized (simpleLoadQueue) {
                        LoadQueueEntry e = simpleLoadQueue.poll();
                    }

                    synchronized (request) {
                        request.setResultPage(requestedData);
                        request.setCompleted();
                        request.notifyAll();
                        logger.info(getLogMessage("Notifying execution threads that request for page %d of resource %d is completed", request.getResourceId(), request.getPageNumber()));
                    }


//                    lock cache as well in the queue sync check

                }
                catch (BufferPoolException e) {
                    throw new RuntimeException(e);
                } catch (IOException e) {
                    logger.severe(getLogMessage("Could not read page %d of resource %d from disk", request.getPageNumber(), resourceId));
                } catch (DuplicateCacheEntryException e) {
                    synchronized (simpleLoadQueue) {
                        simpleLoadQueue.poll();
                    }
                    logger.severe(getLogMessage("Could not add page %d of resource %d to cache because it is already in the cache", request.getPageNumber(), resourceId));
                } catch (CachePinnedException e) {
                    synchronized (simpleLoadQueue) {
                        simpleLoadQueue.poll();
                    }
                    logger.severe(getLogMessage("Could not add page %d of resource %d to cache because all entries are pinned", request.getPageNumber(), resourceId));
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
                    logger.info(getLogMessage("Took write queue entry for page %d of resource %d", entry.getPageNumber(), entry.getResourceId()));
                    ResourceManager resourceManager = entry.getResourceManager();
                    PageSize pageSize = resourceManager.getPageSize();
                    try {


                        resourceManager.writePageToResource(entry.getBufferToWrite(), entry.getPage());
                        logger.info(getLogMessage("Wrote page %d of resource %d to disk", entry.getPageNumber(), entry.getResourceId()));
                    } catch (IOException e) {
                        logger.severe(getLogMessage("Could not write page %d of resource %d to disk: %s", entry.getPageNumber(), entry.getResourceId(), e.getMessage()));
                    }

                    if(!entry.isSetForImmediateRefetch()){
                        LinkedBlockingQueue<byte[]> bufferQueue = freeBufferCollection.get(pageSize.getNumberOfBytes());
                        bufferQueue.offer(entry.getBufferToWrite());
                        logger.info(getLogMessage("Added buffer back to the pool. %d buffers in the pool", bufferQueue.size()));
                    }

                } catch (InterruptedException e) {
                    this.isAlive = false;
                }
            }
        }
    }
}
