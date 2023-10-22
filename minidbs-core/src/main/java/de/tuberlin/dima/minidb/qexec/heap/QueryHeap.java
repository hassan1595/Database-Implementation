package de.tuberlin.dima.minidb.qexec.heap;


import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import de.tuberlin.dima.minidb.Config;
import de.tuberlin.dima.minidb.Constants;
import de.tuberlin.dima.minidb.catalogue.ColumnSchema;
import de.tuberlin.dima.minidb.catalogue.TableSchema;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.core.DataType;
import de.tuberlin.dima.minidb.core.InternalOperationFailure;
import de.tuberlin.dima.minidb.io.cache.PageFormatException;
import de.tuberlin.dima.minidb.io.cache.PageSize;
import de.tuberlin.dima.minidb.io.tables.PageTupleAccessException;
import de.tuberlin.dima.minidb.io.tables.TablePage;
import de.tuberlin.dima.minidb.io.tables.TableResourceManager;
import de.tuberlin.dima.minidb.io.tables.TupleIterator;


/**
 * STUB Implementation of the Query Heap.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class QueryHeap
{
	// --------------------------------------------------------------------------------------------
	//                                    Constants
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Describes the fraction of the heap that is used for buffers for
	 * external lists;
	 */
	private static final float FRACTION_RESERVED_FOR_BLOCK_BUFFERS = 0.5f;
	
	/**
	 * Describes how much of the heap may be assigned to a single operator at a time.
	 */
	private static final float MAX_INTERNAL_SPACE_FRACTION_PER_ASSIGNMENT = 0.33f;
	
	/**
	 * Describes how many buffers a single operator may occupy at a time.
	 */
	private static final float MAX_BUFFER_FRACTION_PER_ASSIGNMENT = 0.5f;
	
	/**
	 * The page size of the blocks written to temp-space.
	 */
	private static final PageSize BLOCK_PAGE_SIZE = PageSize.SIZE_8192;
	
	/**
	 * Describes the minimal number of tuples that must fit into the internally sorted lists
	 * for the sort to be applicable.
	 */
	private static final int MIN_INTERNAL_SORT_TUPLES = 1000;
	

	// --------------------------------------------------------------------------------------------
	//                                          Pool General
	// --------------------------------------------------------------------------------------------
	
	/**
	 * The logger.
	 */
	private final Logger logger;
	
	/**
	 * The random number generator used to assign IDs for reserved heap space.
	 */
	private final Random idGenerator = new Random();
	
	/**
	 * Flag indicating that the heap was closed.
	 */
	private boolean closed;

	
	// --------------------------------------------------------------------------------------------
	//                           Block Buffer Section (for temp-space I/O)
	// --------------------------------------------------------------------------------------------
	
	/**
	 * The list of block buffers for external sorts and partitioned hashing currently available. 
	 */
	private List<byte[]> blockBuffers;
	
	/**
	 * The path to the directory for temp files.
	 */
	private File tempFileDirectory;
	
	/**
	 * The total number of block buffers provided by the heap.
	 */
	private int numTotalBlockBuffers;
	
	/**
	 * The maximal number of buffers that a single operator can use. 
	 */
	private int maxBuffersPerAssignment;
	
	
	// --------------------------------------------------------------------------------------------
	//                         Assignable Main Memory Section (for temp-space I/O)
	// --------------------------------------------------------------------------------------------
	
	/**
	 * The monitor on which queries waiting for heap space wait.
	 */
	private Object assignableHeapMonitor;
	
	/**
	 * Queue of heap requests.
	 */
	private List<HeapRequest> requestedHeapSizes;
	
	/**
	 * Exponent for the exponential function used to assign space depending on the
	 * space that is still free.
	 */
	private double assignmentExponent;
	
	/**
	 * Total number of bytes that are free for assignment.
	 */
	private long totalAssignableSize;
	
	/**
	 * Current number of bytes that are free to be assigned.
	 */
	private long bytesFree;
	
	/**
	 * Maximal number of bytes that a single query can get assigned.
	 */
	private long maxBytesPerAssignment;
	
	// --------------------------------------------------------------------------------------------
	//                                          Sort Heap Specifics
	// --------------------------------------------------------------------------------------------
	
	/**
	 * The map of assignments of space for sorting.
	 */
	private Map<Integer, AssignedSortHeapSpace> assignedSortSpace;

	
	
	// --------------------------------------------------------------------------------------------
	//                                          Setup and Tear Down
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Creates a new Query Heap that occupies at most the given number of bytes and used the
	 * given buffer pool for its I/O operations.
	 * 
	 * @param owner The DBMS instance that owns this query heap.
	 */
	public QueryHeap(Logger logger, Config config) throws QueryHeapException
	{
		this.logger = logger;

		// sanity checks
		long sizeInBytes = config.getQueryHeapSize();
		if (sizeInBytes < 1024 * 1024) {
			throw new QueryHeapException("Query heap size must be at least " + (1024 * 1024) +
					" bytes");
		}
		
		// ----------------- set up block buffers -------------------
		
		long bytesForBlocks = (long) (sizeInBytes * FRACTION_RESERVED_FOR_BLOCK_BUFFERS);
		this.numTotalBlockBuffers = (int) (bytesForBlocks / BLOCK_PAGE_SIZE.getNumberOfBytes());
		bytesForBlocks = ((long) this.numTotalBlockBuffers) * BLOCK_PAGE_SIZE.getNumberOfBytes();
		
		this.blockBuffers = new ArrayList<byte[]>(this.numTotalBlockBuffers);
		for (int i = 0; i < this.numTotalBlockBuffers; i++) {
			this.blockBuffers.add(new byte[BLOCK_PAGE_SIZE.getNumberOfBytes()]);
		}
		this.maxBuffersPerAssignment = (int) (this.numTotalBlockBuffers * MAX_BUFFER_FRACTION_PER_ASSIGNMENT);
		//adapt path in case of jar source
		if(this.getClass().getResource(config.getTempspaceDirectory()) == null)
			this.tempFileDirectory = new File(config.getTempspaceDirectory());
		else
			this.tempFileDirectory = new File(this.getClass().getResource(config.getTempspaceDirectory()).getPath());
		
		// ----------------- set up assignable part -------------------
		this.totalAssignableSize = sizeInBytes - bytesForBlocks;
		this.bytesFree = this.totalAssignableSize;
		this.maxBytesPerAssignment = (long) (this.totalAssignableSize * MAX_INTERNAL_SPACE_FRACTION_PER_ASSIGNMENT);
		this.assignmentExponent = Math.log(this.maxBytesPerAssignment) / Math.log(this.totalAssignableSize);
		
		// set up the lists, maps and monitors
		this.assignedSortSpace = new HashMap<Integer, AssignedSortHeapSpace>();
		this.requestedHeapSizes = new ArrayList<HeapRequest>();
		this.assignableHeapMonitor = new Object();
	}
	
	
	/**
	 * Shuts down the query heap and releases all resources.
	 */
	public void closeQueryHeap() throws QueryHeapException
	{	
		synchronized (this.assignableHeapMonitor) {
			
			if (this.closed) {
				throw new QueryHeapException("Query heap has already been closed.");
			}
	
			// mark it closed
			this.closed = true;
			
			// clear the ones waiting for assignments
			for (int i = 0; i < this.requestedHeapSizes.size(); i++) {
				HeapRequest req = this.requestedHeapSizes.get(i);
				synchronized (req) {
					req.markDone();
					req.notifyAll();
				}
			}
			
			// clear the assignments
			Iterator<AssignedSortHeapSpace> iter = this.assignedSortSpace.values().iterator();
			while (iter.hasNext())
			{
				AssignedSortHeapSpace space = iter.next();
				internalReleaseSortArray(space);
				internalReleaseSortLists(space);
			}
			
			this.assignedSortSpace = null;
			
			// clear the block buffers
			synchronized (this.blockBuffers) {
				this.blockBuffers.clear();
				this.blockBuffers.notifyAll();
			}
		}
	}
	
	/**
	 * Reserves a portion of the heap for sorting and assigns an ID to that to address it.
	 * 
	 * @param tupleSchema The schema of the tuples, used to estimate the memory consumption.
	 * @param estimatedCardinality The estimated number of tuples to sort.
	 * @return The ID under which the assigned portion of the heap can be addressed.
	 * @throws QueryExecutionOutOfHeapSpaceException Thrown, if the heap is in total to small
	 *                                               to provide enough space to sort tuples of
	 *                                               the given schema.
	 */
	public int reserveSortHeap(DataType[] columnTypes, int estimatedCardinality)
	throws QueryExecutionOutOfHeapSpaceException, QueryHeapException
	{		
		// the id we will get assigned
		Integer id = null;
		AssignedSortHeapSpace space = null;
		
		// calculate space
		int tupleWidth = getTupleBytes(columnTypes);
		long minimalBytes = MIN_INTERNAL_SORT_TUPLES * tupleWidth;
		long share = 0;
		
		// check if it is at all possible to get our minimal tuple count
		if (minimalBytes > this.maxBytesPerAssignment) {
			throw new QueryExecutionOutOfHeapSpaceException(
					"Query Heap is too small to assign the minimal bytes for sorting to the query."
					+ " Required bytes: " + minimalBytes + ", Maximal bytes for one sort: " + 
					this.maxBytesPerAssignment + " (" +
					((1-FRACTION_RESERVED_FOR_BLOCK_BUFFERS) * MAX_INTERNAL_SPACE_FRACTION_PER_ASSIGNMENT)
					+ " of the total heap space).");
		}
		
		// request object that we need to wait on if we can not immediately get the space
		HeapRequest request = null;
		
		// ---------------------------------------------------------------------
		// BEGIN: Critical section on shared structures
		// ---------------------------------------------------------------------
		synchronized (this.assignableHeapMonitor)
		{
			if (this.closed) {
				throw new QueryHeapException("The query heap has been closed.");
			}
			
			// check if other requests are pending or there is not enough
			// space to serve us
			if (!this.requestedHeapSizes.isEmpty() || this.bytesFree < minimalBytes)
			{
				request = new HeapRequest(minimalBytes);
				this.requestedHeapSizes.add(request);
			}
		}
		// ---------------------------------------------------------------------
		// END: Critical section on shared structures
		// ---------------------------------------------------------------------
		
		// if we need to wait for space to become available, wait on the request object.
		if (request != null) {
			synchronized (request) {
				while (!request.isDone() && !this.closed)
				{
					try {
						request.wait(1000);
					}
					catch (InterruptedException iex) {}
				}
			}
		}
		
		// ---------------------------------------------------------------------
		// BEGIN: Critical section on shared structures
		// ---------------------------------------------------------------------
		synchronized (this.assignableHeapMonitor)
		{
			// check if we were closed in the meantime
			if (this.closed) {
				throw new QueryHeapException("The query heap has been closed while the thread " +
						"was waiting for heap space to become available.");
			}
			
			// sanity check
			if (this.bytesFree < minimalBytes) {
				throw new InternalOperationFailure(
						"Query Heap encountered strategy bug and could not assign heap space.",
						false, null);
			}
			
			// remove our queue entry, if necessary
			if (request != null) {
				this.requestedHeapSizes.remove(request);
			}
			
			// compute our share
			if (this.requestedHeapSizes.isEmpty()) {
				// no unserved request pending, assign as by the exponential function
				share = (long) Math.pow(this.bytesFree, this.assignmentExponent);
				long cardinalityRequirement = 2L * estimatedCardinality * tupleWidth;
				share = cardinalityRequirement < 0 ? share : Math.min(share, cardinalityRequirement);
				share = Math.max(share, minimalBytes);
			}
			else {
				share = minimalBytes;
			}
			
			// reserve space
			this.bytesFree -= share;
			do {
				id = new Integer(this.idGenerator.nextInt(Integer.MAX_VALUE) + 1);
			}
			while (this.assignedSortSpace.containsKey(id));
			
			space = new AssignedSortHeapSpace(columnTypes, id.intValue(), share);
			this.assignedSortSpace.put(id, space);
		}
		// ---------------------------------------------------------------------
		// END: Critical section on shared structures
		// ---------------------------------------------------------------------
		
		int numInternalTuples = (int) (share / tupleWidth);
		space.setNumInternallySortedTuples(numInternalTuples);
		space.setInternalSortArray(getPooledSortArray(numInternalTuples));
		
		return id.intValue();
	}
	
	
	
	/**
	 * Releases all resources on the part of the heap assigned under the given ID.
	 * 
	 * @param heapId The ID under which the part of the head was assigned.
	 * @return True, if the heap was released, false if nothing was reserved under this ID. 
	 */
	public boolean releaseSortHeap(int heapId)
	{
		AssignedSortHeapSpace space = null;
		synchronized (this.assignableHeapMonitor) {
			space = this.assignedSortSpace.remove(new Integer(heapId));
			if (space != null) {
				internalReleaseSortArray(space); 
			}
		}

		internalReleaseSortLists(space);
		
		return space != null;
	}
	
	/**
	 * Gets the array for internal sorts for the assigned heap with the given ID.
	 * 
	 * @param heapId The id of the assigned part of the heap.
	 * @return The array for internal sorts for this assigned part of the heap.
	 * @throws QueryHeapException Thrown if no heap space has been assigned under the given ID or
	 *                           if the sort array has previously been released.
	 */
	public DataTuple[] getSortArray(int heapId) throws QueryHeapException
	{		
		// get the assignment
		AssignedSortHeapSpace space = null;
		synchronized (this.assignableHeapMonitor) {
			if (this.closed) {
				throw new QueryHeapException("The query heap has been closed.");
			}
			
			space = this.assignedSortSpace.get(new Integer(heapId));
		}
		
		// check
		if (space == null) {
			throw new QueryHeapException("No heap space assigned under the id " + heapId);
		}
		DataTuple[] array = space.getInternalSortArray();
		if (array == null) {
			throw new QueryHeapException(
					"Internal sort array has already been released for heap " + heapId);
		}
		
		return array;
	}
	
	/**
	 * Releases the resources for internal sorting from this heap. Resources for merging
	 * sub-lists that were written will still be kept.
	 * 
	 * @param heapId The ID of the part of the heap.
	 * @throws QueryHeapException Thrown if no heap space has been assigned under the given ID.
	 */
	public void releaseSortArray(int heapId) throws QueryHeapException
	{		
		AssignedSortHeapSpace space = null;
		synchronized (this.assignableHeapMonitor) {
			if (this.closed) {
				throw new QueryHeapException("The query heap has been closed.");
			}
			
			space = this.assignedSortSpace.get(new Integer(heapId));
			if (space == null) {
				throw new QueryHeapException("No heap space assigned under the id " + heapId);
			}
			internalReleaseSortArray(space);
		}
	}
	
	/**
	 * Gets the number of tuples that may be sorted internally under this reserved portion of the sort heap.
	 * 
	 * @param heapId The ID of the reserved portion of the sort heap.
	 * @return The number of tuples that may be sorted internally.
	 * @throws QueryHeapException Thrown if no heap space has been assigned under the given ID.
	 */
	public int getMaximalTuplesForInternalSort(int heapId) throws QueryHeapException
	{		
		AssignedSortHeapSpace space = null;
		synchronized (this.assignableHeapMonitor) {
			if (this.closed) {
				throw new QueryHeapException("The query heap has been closed.");
			}
			
			space = this.assignedSortSpace.get(new Integer(heapId));
		}
		
		if (space == null) {
			throw new QueryHeapException("No heap space assigned under the id " + heapId);
		}
		return space.getNumInternallySortedTuples();
	}
	
	
	
	/**
	 * Writes a sequence of tuples to secondary storage. For the written sequence, an iterator
	 * will be available later.
	 * 
	 * @param heapId The ID of the reserved portion of the sort heap.
	 * @param tuples The list of tuples to write.
	 * @param numTuples The number of tuples in the list to be written.
	 * @throws QueryHeapException Thrown if no heap space has been assigned under the given ID.
	 * @throws IOException Thrown, if an I/O problem prevented the lists from being
	 *                     written properly.
	 */
	public void writeTupleSequencetoTemp(int heapId, DataTuple[] tuples, int numTuples)
	throws QueryHeapException, IOException
	{		
		// get handle
		AssignedSortHeapSpace space = null;
		synchronized (this.assignableHeapMonitor) {
			if (this.closed) {
				throw new QueryHeapException("The query heap has been closed.");
			}
			
			space = this.assignedSortSpace.get(new Integer(heapId)); 
		}
		
		// check
		if (space == null) {
			throw new QueryHeapException("No heap space assigned under the id " + heapId);
		}
		
		// check if we spilled before
		TableResourceManager tempFileHandle = space.getTempFileManager();
		if (tempFileHandle == null)
		{
			// create the temp table schema
			TableSchema schema = new TableSchema(BLOCK_PAGE_SIZE);
			DataType[] cols = space.getTupleSchema();
			for (int i = 0; i < cols.length; i++) {
				ColumnSchema scheme = ColumnSchema.createColumnSchema("TempCol-" + i, cols[i]);
				schema.addColumn(scheme);
			}
			
			// create a new temp file, checking whether we are in jar or not
			String resource = "/config.xml";
		    URL res = getClass().getResource(resource);
			File tempFile = null;
			if (res.toString().startsWith("jar:")){
				tempFile = File.createTempFile(Constants.QUERY_HEAP_TEMP_FILE_PREFIX, String.valueOf(space.getHeapId()));
				tempFile.deleteOnExit();
			}
			else{
				tempFile = new File(this.tempFileDirectory, 
					Constants.QUERY_HEAP_TEMP_FILE_PREFIX + space.getHeapId());
			}
			tempFileHandle = TableResourceManager.createTable(tempFile, schema);
			space.setTempFile(tempFile, tempFileHandle);
		}
		
		// begin writing the list to the temporary file
		byte[] buffer = getBuffer();
		
		try {
			int tuplesWritten = 0;
			TablePage currentPage = tempFileHandle.reserveNewPage(buffer);
			int beginningOfList = currentPage.getPageNumber();
			
			// write all tuples in blocks
			while (tuplesWritten < numTuples)
			{
				try {
					if (currentPage.insertTuple(tuples[tuplesWritten])) {
						tuplesWritten++;
					}
					else {
						// full, write and create a new one
						tempFileHandle.writePageToResource(buffer, currentPage);
						currentPage = tempFileHandle.reserveNewPage(buffer);
					}
				}
				catch (PageFormatException e) {
					throw new QueryHeapException("Sorted sublist could not be written to temp space.", e);
				}
			}
			
			// write the last block
			tempFileHandle.writePageToResource(buffer, currentPage);
			
			// store the blocks for that sublist
			int listLength = currentPage.getPageNumber() - beginningOfList + 1;
			space.addWrittenList(new WrittenListDescriptor(beginningOfList, listLength));
		}
		catch (Exception e) {
			throw new QueryHeapException("An error occurred writing the sorted sublist: " + e.getMessage());
		}
		finally {
			returnBuffer(buffer);
		}
	}
	
	/**
	 * Gets an array of iterators that can be used to obtain the tuples from the sorted sublist.
	 * The sublists are read into memory as needed.
	 * 
	 * @param heapId The ID of the reserved portion of the sort heap.
	 * @return An array of iterators, one per sub-list that has been written.
	 * @throws QueryHeapException Thrown if no heap space has been assigned under the given ID 
	 *                            or if no external sorted lists were produced for this
	 *                            reserved portion of the heap.
	 */
	public ExternalTupleSequenceIterator[] getExternalSortedLists(int heapId)
	throws QueryHeapException, IOException
	{		
		// get handle
		AssignedSortHeapSpace space = null;
		synchronized (this.assignableHeapMonitor) {
			if (this.closed) {
				throw new QueryHeapException("The query heap has been closed.");
			}
			
			space = this.assignedSortSpace.get(new Integer(heapId)); 
		}
		
		// check if that ID exists
		if (space == null) {
			throw new QueryHeapException("No heap space assigned under the id " + heapId);
		}
		
		// check if the iterators already exist
		if (space.getExternalListIterators() != null) {
			return space.getExternalListIterators();
		}
		
		// check that external lists were created
		TableResourceManager manager = space.getTempFileManager();
		List<WrittenListDescriptor> externalLists = space.getWrittenLists();
		if (manager == null || externalLists == null || externalLists.isEmpty()) {
			throw new QueryHeapException("No external sub-lists were produced for this reserved " +
					"portion of the query heap.");
		}
		
		// check if we have at all enough buffers, or if we cannot perform this sort with the
		// given heap size
		if (externalLists.size() > this.maxBuffersPerAssignment) {
			throw new QueryHeapException(
					"Cannot process the sort, too many sub-lists were created. " +
					"A larger query-heap size is required.");
		}
		
		// create the iterators
		ExternalListIterator[] iters = new ExternalListIterator[externalLists.size()];
		try {
			for (int i = 0; i < iters.length; i++) {
				WrittenListDescriptor descr = externalLists.get(i);
				iters[i] = new ExternalListIterator(manager, getBuffer(), descr.getFirstBlock(), descr.numBlocks);
			}
		}
		catch (Exception e) {
			internalReleaseSortListIterators(iters);
		}
		
		space.setExternalListIterators(iters);
		space.clearWrittenLists();
		
		return iters;
	}
	
	
	/**
	 * Internal function to synchronize access to grab a buffer.
	 * 
	 * @return An acquired buffer.
	 */
	private final byte[] getBuffer()
	{
		synchronized (this.blockBuffers) {
			while (this.blockBuffers.isEmpty() && !this.closed) {
				try {
					this.blockBuffers.wait(1000);
				}
				catch (InterruptedException iex) {}
			}
			
			return this.closed ? null : this.blockBuffers.remove(this.blockBuffers.size() - 1);
		}
	}
	
	/**
	 * Internal function to synchronize access and release a buffer.
	 * 
	 * @param buffer The buffer to release.
	 */
	private final void returnBuffer(byte[] buffer)
	{
		synchronized (this.blockBuffers) {
			if (!this.closed) {
				this.blockBuffers.add(buffer);
				this.blockBuffers.notify();
			}
		}
	}
	
	/**
	 * Function that gets an array for internal sorting that provides at least the given number of entries.
	 * If an array of at least that size is pooled, the array is returned, otherwise a new one
	 * is instantiated with the given capacity.
	 * 
	 * @param minCapacity The minimal capacity for the array.
	 * @return An array of at least the given size.
	 */
	private final DataTuple[] getPooledSortArray(int minCapacity)
	{
		return new DataTuple[minCapacity];
	}


	/**
	 * Releases the heap assignment's reserved memory for internal sorting.
	 * 
	 * WARNING: This method assumes the caller already holds the lock for the assignable space.
	 * 
	 * @param space The object describing the sort heap assignment where the internal array is to
	 *              be released.
	 */
	private final void internalReleaseSortArray(AssignedSortHeapSpace space)
	{
		if (space != null) {
			// first pool the sort array 
			DataTuple[] array = space.getInternalSortArray();
			if (array != null) {
				space.setInternalSortArray(null);
			}
			
			// now register the bytes as available
			releaseAssignableHeapSpace(space.getHeapSize());
			
			// reset all internal fields
			space.setHeapSize(0);
			space.setNumInternallySortedTuples(0);
			
		}
	}
	
	/**
	 * Internal function to mark heap space as available and wake up any waiting
	 * requesters. 
	 * 
	 * @param bytestoRelease The number of bytes to release.
	 */
	private final void releaseAssignableHeapSpace(long bytestoRelease)
	{
		// add available bytes
		this.bytesFree += bytestoRelease;
		long bytes = this.bytesFree;
		
		// wake up as many requests as could now be served
		for (int i = 0; i < this.requestedHeapSizes.size(); i++)
		{
			HeapRequest req = this.requestedHeapSizes.get(i);
			if (req.getBytes() < bytes) {
				bytes -= req.getBytes();
				synchronized (req) {
					req.notifyAll();
				}
			}
		}
	}



	/**
	 * Internal implementation of the function to release the temp-space  resources of the
	 * given heap.
	 * 
	 *  @param space The heap space assignment to release.
	 */
	private void internalReleaseSortLists(AssignedSortHeapSpace space)
	{
		if (space != null) {
			// release the iterators
			ExternalListIterator[] iters = space.getExternalListIterators();
			if (iters != null) {
				internalReleaseSortListIterators(iters);
				space.setExternalListIterators(null);
			}
			space.clearWrittenLists();
			// release the temp file
			if (space.getTempFileManager() != null) {
				try {
					space.getTempFileManager().closeResource();
					TableResourceManager.deleteTable(space.getTempFile());
				}
				catch (Exception ex) {
					this.logger.log(Level.WARNING, "Temp file from query heap could not be released.", ex);
				}
				space.setTempFile(null, null);
			}
		}
	}


	/**
	 * Utility function to release the iterators over the sorted sublists.
	 * 
	 * @param iters The array of iterators to release.
	 */
	private final void internalReleaseSortListIterators(ExternalListIterator[] iters)
	{
		for (int i = 0; i < iters.length; i++) {
			if (iters[i] != null) {
				byte[] buffer = iters[i].abort();
				returnBuffer(buffer);
			}
		}
	}


	/**
	 * Utility function to compute the memory needed by a tuple.
	 * 
	 * @param columns The schema of the columns.
	 * @return A relative value estimating the size of a tuple.
	 */
	public static final int getTupleBytes(DataType[] columns)
	{
		int size = 8;	// ad-hoc tuple overhead estimation
		for (int i = 0; i < columns.length; i++) {
			DataType type = columns[i];
			int colWidth = type.getNumberOfBytes();
			colWidth += type.isArrayType() ? 20 : 0;
			size += colWidth + 4;  // ad-hoc field overhead
		}
		return size;
	}
	
	/**
	 * Gets the size of the pages used to write external tuple sequences.
	 * 
	 * @return The page sized used for tables in tempspace.
	 */
	public static final PageSize getPageSize()
	{
		return BLOCK_PAGE_SIZE;
	}

	
	/**
	 * Utility class describing a request for space on the sort heap.
	 */
	private static final class HeapRequest
	{
		/**
		 * The number of bytes requested.
		 */
		private long bytes;
		
		/**
		 * Flag indicating that the request was fulfilled.
		 */
		private boolean done;
		
		/**
		 * Creates a new request for the given number of bytes.
		 * 
		 * @param bytes The number of bytes requested.
		 */
		public HeapRequest(long bytes)
		{
			this.bytes = bytes;
		}

		/**
		 * Gets the bytes from this QueryHeap.HeapRequest.
		 *
		 * @return The bytes.
		 */
		public long getBytes()
		{
			return this.bytes;
		}

		/**
		 * Gets the done from this QueryHeap.HeapRequest.
		 *
		 * @return The done.
		 */
		public boolean isDone()
		{
			return this.done;
		}

		/**
		 * Sets the HeapRequest done.
		 *
		 * @param done The done to set.
		 */
		public void markDone()
		{
			this.done = true;
		}
	}
	
	/**
	 * Utility class describing a list of blocks as part of a temp file.
	 */
	private static final class WrittenListDescriptor
	{
		/**
		 * First block of the written list.
		 */
		private int firstBlock;
		
		/**
		 * Number of block in the written list.
		 */
		private int numBlocks;

		/**
		 * Creates a new WrittenListDescriptor.
		 * 
		 * @param firstBlock The first block of the written list.
		 * @param numBlocks The number of block in the written list.
		 */
		public WrittenListDescriptor(int firstBlock, int numBlocks)
		{
			this.firstBlock = firstBlock;
			this.numBlocks = numBlocks;
		}

		/**
		 * Gets the firstBlock from this QueryHeap.WrittenListDescriptor.
		 *
		 * @return The firstBlock.
		 */
		public int getFirstBlock()
		{
			return this.firstBlock;
		}

		/**
		 * Gets the numBlocks from this QueryHeap.WrittenListDescriptor.
		 *
		 * @return The numBlocks.
		 */
		@SuppressWarnings("unused")
		public int getNumBlocks()
		{
			return this.numBlocks;
		}
	}
	
	
	
	private static final class AssignedSortHeapSpace
	{
		/**
		 * The schema of the tuples that are sorted here.
		 */
		private DataType[] tupleSchema;
		
		/**
		 * The id used with this assignment.
		 */
		private int heapId;
		
		/**
		 * The internal sort/hash bytes currently reserved with this assignment. 
		 */
		private long heapSize;
		
		/**
		 * The array used for internal sorting.
		 */
		private DataTuple[] internalSortArray;
		
		/**
		 * The maximal number of tuples to be sorted internally.
		 */
		private int numInternallySortedTuples;
		
		/**
		 * Handle to the temp file created by this sort.
		 */
		private TableResourceManager tempFileManager;
		
		/**
		 * The temp file created by this sort.
		 */
		private File tempFile;
		
		/**
		 * The lists written out for this sort.
		 */
		private List<WrittenListDescriptor> writtenLists;
		
		/**
		 * The iterators over the external lists. May be null, if no external
		 * lists were produced during the sort.
		 */
		private ExternalListIterator[] externalListIterators;
		
		
		/**
		 * 
		 * @param tupleSchema
		 * @param heapId
		 * @param heapSize
		 */
		public AssignedSortHeapSpace(DataType[] columnTypes, int heapId, long heapSize)
		{
			this.tupleSchema = columnTypes;
			this.heapSize = heapSize;
			this.heapId = heapId;
			this.internalSortArray = null;
			this.numInternallySortedTuples = 0;
		}

		
		
		/**
		 * Gets the tupleSchema from this QueryHeap.AssignedSortHeapSpace.
		 *
		 * @return The tupleSchema.
		 */
		public DataType[] getTupleSchema()
		{
			return this.tupleSchema;
		}

		/**
		 * Gets the heapId from this QueryHeap.AssignedHeapSpace.
		 *
		 * @return The heapId.
		 */
		public int getHeapId()
		{
			return this.heapId;
		}

		/**
		 * Gets the heapSize from this QueryHeap.AssignedHeapSpace.
		 *
		 * @return The heapSize.
		 */
		public long getHeapSize()
		{
			return this.heapSize;
		}

		/**
		 * Sets the heapSize for this QueryHeap.AssignedHeapSpace.
		 *
		 * @param heapSize The heapSize to set.
		 */
		public void setHeapSize(long heapSize)
		{
			this.heapSize = heapSize;
		}

		/**
		 * Gets the internalSortArray from this QueryHeap.AssignedSortHeapSpace.
		 *
		 * @return The internalSortArray.
		 */
		public DataTuple[] getInternalSortArray()
		{
			return this.internalSortArray;
		}

		/**
		 * Sets the internalSortArray for this QueryHeap.AssignedSortHeapSpace.
		 *
		 * @param internalSortArray The internalSortArray to set.
		 */
		public void setInternalSortArray(DataTuple[] internalSortArray)
		{
			this.internalSortArray = internalSortArray;
		}

		/**
		 * Gets the numInternallySortedTuples from this QueryHeap.AssignedSortHeapSpace.
		 *
		 * @return The numInternallySortedTuples.
		 */
		public int getNumInternallySortedTuples()
		{
			return this.numInternallySortedTuples;
		}

		/**
		 * Sets the numInternallySortedTuples for this QueryHeap.AssignedSortHeapSpace.
		 *
		 * @param numInternallySortedTuples The numInternallySortedTuples to set.
		 */
		public void setNumInternallySortedTuples(int numInternallySortedTuples)
		{
			this.numInternallySortedTuples = numInternallySortedTuples;
		}

		/**
		 * Gets the tempFileManager from this QueryHeap.AssignedSortHeapSpace.
		 *
		 * @return The tempFileManager.
		 */
		public TableResourceManager getTempFileManager()
		{
			return this.tempFileManager;
		}

		/**
		 * Gets the tempFile from this QueryHeap.AssignedSortHeapSpace.
		 *
		 * @return The tempFile.
		 */
		public File getTempFile()
		{
			return this.tempFile;
		}

		/**
		 * Sets the tempFile and its manager for this AssignedSortHeapSpace.
		 *
		 * @param tempFile The tempFile to set.
		 * @param tempFileManager The tempFileManager to set.
		 */
		public void setTempFile(File tempFile, TableResourceManager tempFileManager)
		{
			this.tempFile = tempFile;
			this.tempFileManager = tempFileManager;
		}
		
		/**
		 * Adds another written list descriptor.
		 * 
		 * @param descr The written list descriptor to add.
		 */
		public void addWrittenList(WrittenListDescriptor descr)
		{
			if (this.writtenLists == null) {
				this.writtenLists = new ArrayList<WrittenListDescriptor>(100);
			}
			this.writtenLists.add(descr);
		}
		
		/**
		 * Gets the list of written list descriptors.
		 * 
		 * @return The list of written list descriptors.
		 */
		public List<WrittenListDescriptor> getWrittenLists()
		{
			return this.writtenLists;
		}
		
		/**
		 * Clears the list of written list descriptors.
		 */
		public void clearWrittenLists()
		{
			this.writtenLists = null;
		}

		/**
		 * Gets the externalListIterators from this QueryHeap.AssignedSortHeapSpace.
		 *
		 * @return The externalListIterators.
		 */
		public ExternalListIterator[] getExternalListIterators()
		{
			return this.externalListIterators;
		}

		/**
		 * Sets the externalListIterators for this QueryHeap.AssignedSortHeapSpace.
		 *
		 * @param externalListIterators The externalListIterators to set.
		 */
		public void setExternalListIterators(ExternalListIterator[] externalListIterators)
		{
			this.externalListIterators = externalListIterators;
		}
	}
	
	/**
	 * Implementation of the <tt>ExternalTupleSequenceIterator</tt> that lazily loads blocks for tuples
	 * from secondary storage.
	 */
	private final class ExternalListIterator implements ExternalTupleSequenceIterator
	{
		private TableResourceManager tempFileManager;

		private TupleIterator currentIterator;
		
		private byte[] buffer;
		
		private int currentBlock;
		
		private int numBlocksLeft;
		

		public ExternalListIterator(TableResourceManager tempFileManager, byte[] buffer,
				int firstBlock, int numBlocks)
		throws IOException
		{
			this.tempFileManager = tempFileManager;
			this.buffer = buffer;
			this.currentBlock = firstBlock;
			this.numBlocksLeft = numBlocks;
			
			this.currentIterator = getNextNoneEmptyPage();
		}
		
		
		/**
		 * Aborts this iterator and returns its buffer.
		 * 
		 * @return The buffer.
		 */
		public byte[] abort()
		{
			this.tempFileManager = null;
			this.currentIterator = null;

			byte[] b = this.buffer;
			this.buffer = null;
			return b;
		}

		/* (non-Javadoc)
		 * @see de.tuberlin.dima.minidb.qexec.heap.ExternalTupleSequenceIterator#hasNext()
		 */
		@Override
		public boolean hasNext() throws QueryHeapException
		{
			if (this.currentIterator != null) {
				return true;
			}
			else if (this.tempFileManager == null) {
				throw new QueryHeapException("The sort heap assignment supporting this iterator has been released.");
			}
			else {
				return false;
			}
		}

		/* (non-Javadoc)
		 * @see de.tuberlin.dima.minidb.qexec.heap.ExternalTupleSequenceIterator#next()
		 */
		@Override
		public DataTuple next() throws QueryHeapException, IOException
		{
			if (this.currentIterator != null) {
				try {
					DataTuple tuple = this.currentIterator.next();
					
					// check if we need to load the next page
					if (!this.currentIterator.hasNext()) {
						this.currentIterator = getNextNoneEmptyPage();
					}
					
					return tuple;
				}
				catch (PageTupleAccessException ptae) {
					throw new IOException("Page from external sub-list was corrupted during I/O.", ptae);
				}
			}
			else {
				throw new NoSuchElementException();
			}
		}
		
		/**
		 * Gets the iterator over the next not empty page.
		 * 
		 * @return An iterator over the next not empty page.
		 * @throws IOException Thrown, if the next page could not be read.
		 */
		private TupleIterator getNextNoneEmptyPage() throws IOException
		{
			while (this.numBlocksLeft > 0) {
				TablePage page = this.tempFileManager.readPageFromResource(this.buffer, this.currentBlock);
				this.currentBlock++;
				this.numBlocksLeft--;
				try {
					int numCols = this.tempFileManager.getSchema().getNumberOfColumns();
					TupleIterator iter = page.getIterator(numCols, (0x1L << numCols) - 1);
					if (iter.hasNext()) {
						return iter;
					}
				}
				catch (PageTupleAccessException ptae) {
					throw new IOException("Page from external sub-list was corrupted during I/O.", ptae);
				}
			}
			
			return null;
		}
	}
}
