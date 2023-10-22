package de.tuberlin.dima.minidb.io.index;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;

import de.tuberlin.dima.minidb.Constants;
import de.tuberlin.dima.minidb.catalogue.IndexSchema;
import de.tuberlin.dima.minidb.catalogue.TableSchema;
import de.tuberlin.dima.minidb.core.InternalOperationFailure;
import de.tuberlin.dima.minidb.io.cache.CacheableData;
import de.tuberlin.dima.minidb.io.cache.PageFormatException;
import de.tuberlin.dima.minidb.io.cache.PageSize;
import de.tuberlin.dima.minidb.io.cache.UnsupportedPageSizeException;
import de.tuberlin.dima.minidb.io.manager.ResourceManager;

/**
 * This class implements the access to a file that contains index data. It provides
 * methods to create, open, close and delete an index. Once a IndexManager is
 * created for an index, it exclusively locks the index's file. The index resource manager
 * allows to read and write binary pages from and to an index.
 * NOTE: When empty pages are written to the resource manager, it marks their space in the file
 * eligible for overwriting. The index implementation must make sure not to leave empty pages that
 * are still linked in index.
 * The index resource that is maintained in the file has a header on the first page containing the
 * following information:
 * <ul>
 * <li>Bytes 0 - 3 = INT (little endian): Magic number</li>
 * <li>Bytes 4 - 7 = INT (little endian): Version. Currently only version 0 is supported.</li>
 * <li>Bytes 8 - 11 = INT (little endian): Page size in bytes.</li>
 * <li>Bytes 12 - 15 = INT (little endian): Column number of the indexed column.</li>
 * <li>Bytes 16 - 19 = INT (little endian): Page number of the root node page.</li>
 * <li>Bytes 20 - 23 = INT (little endian): Page number of the first leaf page.</li>
 * <li>Bytes 24 - 27 = INT (little endian): Attribute flags.</li>
 * </ul>
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class IndexResourceManager extends ResourceManager {
	/**
	 * The page on which the data initially starts.
	 */
	private static final int FIRST_DATA_PAGE = 1;

	/**
	 * The magic number that identifies a file as an index file.
	 */
	private static final int INDEX_HEADER_MAGIC_NUMBER = 0xBADC0FFE;

	/**
	 * The mask to access the 'unique' bit in the attributes.
	 */
	private static final int INDEX_HEADER_ATTRIBUTE_UNIQUE_MASK = 0x1;

	/**
	 * The I/O channel through which the index file is accessed.
	 */
	private final FileChannel ioChannel;

	/**
	 * The lock we hold on the index file.
	 */
	private final FileLock theLock;

	/**
	 * The schema of the index in the file.
	 */
	private final IndexSchema schema;

	/**
	 * Buffer to perform small header updates.
	 */
	private final ByteBuffer miniBuffer;

	/**
	 * The size of a page in bytes.
	 */
	private final int pageSize;

	/**
	 * The number of the last page in the index.
	 */
	private int lastPageNumber;

	// ------------------------------------------------------------------------
	//                        Constructor & Life-Cycle
	// ------------------------------------------------------------------------

	/**
	 * Creates a new index manager to work on an existing index. The index is represented
	 * by the given handle to the file.
	 * 
	 * @param fileHandle
	 *        The handle to the index's file.
	 * @param indexedTable
	 *        The schema of the table that is indexed by this index.
	 * @throws IOException
	 *         If the index file could not be accessed due to an I/O error.
	 * @throws PageFormatException
	 *         If the index file did not contain a valid header.
	 */
	protected IndexResourceManager(RandomAccessFile fileHandle, TableSchema indexedTable) throws IOException, PageFormatException {
		// Open the channel. If anything fails, make sure we close it again
		try {
			this.ioChannel = fileHandle.getChannel();
			try {
				this.theLock = this.ioChannel.tryLock();
			} catch (OverlappingFileLockException oflex) {
				throw new IOException("Index file locked by other consumer.");
			}

			if (this.theLock == null) {
				throw new IOException("Could acquire index file handle for exclusive usage. File locked otherwise.");
			}
		} catch (Throwable t) {
			// something failed.
			makeBestEffortToClose();

			// propagate the exception
			if (t instanceof IOException) {
				throw (IOException) t;
			} else {
				throw new IOException("An error occured while opening the index: " + t.getMessage());
			}
		}

		this.miniBuffer = ByteBuffer.allocate(4);
		this.miniBuffer.order(ByteOrder.LITTLE_ENDIAN);

		// rewind to the beginning of the file
		this.ioChannel.position(0);

		// read the schema
		this.schema = readIndexHeader(this.ioChannel, indexedTable);
		this.schema.setResourceManagerForPersistence(this);
		this.pageSize = this.schema.getPageSize().getNumberOfBytes();
		this.lastPageNumber = (int) (this.ioChannel.size() / this.pageSize) - 1;
	}

	/**
	 * Creates a index manager that newly creates an index for a given (existing) file.
	 * Thereby, the index resource manager opens the file and writes the schema into the file,
	 * representing this index's header.
	 * 
	 * @param fileHandle
	 *        The handle to the file which is used for the index.
	 * @param schema
	 *        The schema for the new index.
	 * @throws IOException
	 *         If the index could not be created due to an I/O error.
	 */
	protected IndexResourceManager(RandomAccessFile fileHandle, IndexSchema schema) throws IOException {
		// Open the channel. If anything fails, make sure we close it again
		try {
			this.ioChannel = fileHandle.getChannel();
			try {
				this.theLock = this.ioChannel.tryLock();
			} catch (OverlappingFileLockException oflex) {
				throw new IOException("Index file locked by other consumer.");
			}

			if (this.theLock == null) {
				throw new IOException("Could acquire index file handle for exclusive usage. File locked otherwise.");
			}
		} catch (Throwable t) {
			// something failed.
			makeBestEffortToClose();

			// propagate the exception
			if (t instanceof IOException) {
				throw (IOException) t;
			} else {
				throw new IOException("An error occured while opening the index: " + t.getMessage());
			}
		}

		this.miniBuffer = ByteBuffer.allocate(4);
		this.miniBuffer.order(ByteOrder.LITTLE_ENDIAN);

		// set member variables
		this.schema = schema;
		this.pageSize = schema.getPageSize().getNumberOfBytes();

		// set the correct root page and first leaf page number to 2. Make sure no one updates yet.
		schema.setResourceManagerForPersistence(null);
		schema.setFirstLeafNumber(FIRST_DATA_PAGE);
		schema.setRootPageNumber(FIRST_DATA_PAGE);

		// rewind and write the header
		this.ioChannel.position(0);
		writeIndexHeader(schema, this.ioChannel);

		// initialize to an empty page
		truncate();

		// make sure we are informed about updates for root or first leaf node
		schema.setResourceManagerForPersistence(this);
	}

	/*
	 * (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.io.ResourceManager#closeResource()
	 */
	@Override
	public synchronized void closeResource() throws IOException {
		try {
			this.theLock.release();
			this.ioChannel.close();
		} catch (Throwable t) {
			// something failed.
			makeBestEffortToClose();

			// propagate the exception
			if (t instanceof IOException) {
				throw (IOException) t;
			} else {
				throw new IOException("An error occured while opening the table: " + t.getMessage());
			}
		}
	}

	/**
	 * Tries to release all resources from this table, but does not
	 * complain if anything fails.
	 */
	private void makeBestEffortToClose() {
		// close the channel
		if (this.ioChannel != null) {
			try {
				this.ioChannel.close();
			} catch (Throwable ignored) {
				// ignore everything, just try to close
			}
		}

		// try to release the lock
		if (this.theLock != null) {
			try {
				this.theLock.release();
			} catch (Throwable ignored) {
				// ignore everything, just try to close
			}
		}
	}

	// ------------------------------------------------------------------------
	//                                Accessors
	// ------------------------------------------------------------------------

	/**
	 * Gets the schema of the index represented by this index manager.
	 * 
	 * @return The index schema.
	 */
	public IndexSchema getSchema() {
		return this.schema;
	}

	/*
	 * (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.io.ResourceManager#getPageSize()
	 */
	@Override
	public PageSize getPageSize() {
		return this.schema.getPageSize();
	}

	// ------------------------------------------------------------------------

	/**
	 * Updates the page number of the root page directly in the header. This
	 * operation directly causes an I/O operation.
	 * NOTE: For a good B-Tree implementation, this value should
	 * rarely change, so the function would be invoked on few occasions.
	 * 
	 * @param newNumber
	 *        The new page number of the first leaf page.
	 */
	public synchronized void updateRootPageNumber(int newNumber) throws IOException {
		// check consistency with the schema
		if (this.schema.getRootPageNumber() != newNumber) {
			this.schema.setRootPageNumber(newNumber);
		}

		// write the update to the header
		this.miniBuffer.clear();
		this.miniBuffer.putInt(newNumber);
		this.miniBuffer.flip();
		this.ioChannel.position(16); // offset of root page number in the header
		writeBuffer(this.ioChannel, this.miniBuffer);
	}

	/**
	 * Updates the page number of the first leaf page directly in the header. This
	 * operation directly causes an I/O operation.
	 * NOTE: For a regular B-Tree implementation, this value should never change.
	 * 
	 * @param newNumber
	 *        The new page number of the first leaf page.
	 */
	public synchronized void updateFirstLeafPageNumber(int newNumber) throws IOException {
		// check consistency with the schema
		if (this.schema.getFirstLeafNumber() != newNumber) {
			this.schema.setFirstLeafNumber(newNumber);
		}

		// write the update to the header
		this.miniBuffer.clear();
		this.miniBuffer.putInt(newNumber);
		this.miniBuffer.flip();
		this.ioChannel.position(20); // offset of root page number in the header
		writeBuffer(this.ioChannel, this.miniBuffer);
	}

	// ------------------------------------------------------------------------
	//                          I/O Methods
	// ------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.io.ResourceManager#truncate()
	 */
	@Override
	public synchronized void truncate() throws IOException {
		// write a new empty leaf page
		try {
			byte[] temporaryBuffer = new byte[this.pageSize];
			IndexPageFactory.initIndexPage(this.schema, temporaryBuffer, FIRST_DATA_PAGE, true);
			writePage(temporaryBuffer, FIRST_DATA_PAGE, this.pageSize);
		} catch (PageFormatException e) {
			throw new InternalOperationFailure("Reset/initialization of index: Mismatch in  between page and buffer size.", true, e);
		}

		// set the I/O channel to the right size.
		this.lastPageNumber = FIRST_DATA_PAGE;
		this.ioChannel.truncate((this.lastPageNumber + 1) * this.pageSize);

		this.schema.setFirstLeafNumber(FIRST_DATA_PAGE);
		this.schema.setRootPageNumber(FIRST_DATA_PAGE);
	}

	// ------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.io.ResourceManager#reserveNewPage(byte[])
	 */
	@Override
	public CacheableData reserveNewPage(byte[] ioBuffer) throws IOException, PageFormatException {
		throw new UnsupportedOperationException("An index page cannot be instantiated without specifying the type.");
	}

	/*
	 * (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.io.ResourceManager#reserveNewPage(byte[], java.lang.Enum)
	 */
	@Override
	public CacheableData reserveNewPage(byte[] buffer, Enum<?> type) throws IOException, PageFormatException {
		// sanity checks
		if (buffer.length != this.pageSize) {
			throw new IllegalArgumentException("The buffer to initialize the page to is too small.");
		}
		if (!(type instanceof BTreeIndexPageType)) {
			throw new IllegalArgumentException("Parameters must be a specifictaion of the page type through " + BTreeIndexPageType.class.getCanonicalName());
		}
		BTreeIndexPageType pageType = (BTreeIndexPageType) type;
		int newPageNumber = this.lastPageNumber + 1;

		try {
			BTreeIndexPage newPage = null;
			if (pageType == BTreeIndexPageType.LEAF_PAGE) {
				newPage = IndexPageFactory.initIndexPage(this.schema, buffer, newPageNumber, true);
			} else {
				newPage = IndexPageFactory.initIndexPage(this.schema, buffer, newPageNumber, false);
			}

			// increment the counter
			this.lastPageNumber++;

			return newPage;
		} catch (PageFormatException pfex) {
			throw new IOException("Could not initialize the new page: " + pfex.getMessage());
		}
	}

	/*
	 * (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.io.ResourceManager#readPageFromResource(byte[], int)
	 */
	@Override
	public BTreeIndexPage readPageFromResource(byte[] buffer, int pageNumber) throws IOException {
		// check that the page number is within range
		if (pageNumber < FIRST_DATA_PAGE || pageNumber > this.lastPageNumber) {
			throw new IOException("Page number " + pageNumber + " is not in valid range: [" + FIRST_DATA_PAGE + "," + this.lastPageNumber + "].");
		}

		// check that we have enough space
		if (buffer.length != this.pageSize) {
			throw new IOException("Buffer is not big enough to hold a page.");
		}

		// seek and read the buffer
		ByteBuffer b = ByteBuffer.wrap(buffer, 0, this.pageSize);
		long position = ((long) this.pageSize) * ((long) pageNumber);
		try {
			readIntoBuffer(this.ioChannel, b, position, this.pageSize);
		} catch (IOException ioex) {
			throw new IOException("Page " + pageNumber + " could not be read from index file.", ioex);
		}

		// create a table page for the loaded data
		try {
			return IndexPageFactory.createPage(this.schema, buffer);
		} catch (PageFormatException pfex) {
			throw new IOException("Page could not be fetched because it is corrupted.", pfex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.io.ResourceManager#readPageFromResource(byte[], int)
	 */
	@Override
	public BTreeIndexPage[] readPagesFromResource(byte[][] buffers, int firstPageNumber) throws IOException {
		if (Constants.DEBUG_CHECK) {
			// check that at least one buffer is provided
			if (buffers.length <= 0) {
				throw new IllegalArgumentException("At least one buffer should be provided.");
			}
			// check that the page number is within range
			if (firstPageNumber < FIRST_DATA_PAGE || firstPageNumber > this.lastPageNumber) {
				throw new IOException("Page number " + firstPageNumber + " is not in valid range: [" + FIRST_DATA_PAGE + "," + this.lastPageNumber + "].");
			}

			for (int i = 0; i < buffers.length; i++) {
				// check that we have enough space
				if (buffers[i].length != this.pageSize) {
					throw new IOException("Buffer is not big enough to hold a page.");
				}
			}
		}

		// seek and read the buffer
		ByteBuffer[] b = new ByteBuffer[buffers.length];
		for (int i = 0; i < buffers.length; i++) {
			b[i] = ByteBuffer.wrap(buffers[i], 0, this.pageSize);
		}

		try {
			this.ioChannel.position(this.pageSize * (long) firstPageNumber);
			long totalSize = buffers.length * this.pageSize;
			long bytesRemaining = buffers.length * this.pageSize;
			int currFirstBuffer = 0;
			do {
				bytesRemaining -= this.ioChannel.read(b, currFirstBuffer, buffers.length - currFirstBuffer);
				currFirstBuffer = (int) ((totalSize - bytesRemaining) / this.pageSize);
			} while (bytesRemaining > 0);
		} catch (IOException ioex) {
			throw new IOException("Page sequence [" + firstPageNumber + ", " + (firstPageNumber + buffers.length - 1) + "] could not be read from index file.",
				ioex);
		}

		// wrap the loaded buffers in CacheableData objects 
		BTreeIndexPage[] pages = new BTreeIndexPage[buffers.length];
		for (int i = 0; i < buffers.length; i++) {
			try {
				pages[i] = IndexPageFactory.createPage(this.schema, buffers[i]);
			} catch (PageFormatException pfex) {
				throw new IOException("Page could not be fetched because it is corrupted.", pfex);
			}
		}

		return pages;
	}

	/*
	 * (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.io.ResourceManager#writePageToResource(byte[],
	 * de.tuberlin.dima.minidb.io.cache.CacheableData, int)
	 */
	@Override
	public void writePageToResource(byte[] buffer, CacheableData wrapper) throws IOException {
		int pageNumber = wrapper.getPageNumber();

		// check that the page number is within range
		if (pageNumber < FIRST_DATA_PAGE) {
			throw new IOException("Page number " + pageNumber + " is not valid. First data page is " + FIRST_DATA_PAGE + ".");
		}

		// check that we have enough space
		if (buffer.length != this.pageSize) {
			throw new IOException("Buffer does not hold a full page (" + this.pageSize + " bytes).");
		}

		// now write the page
		try {
			writePage(buffer, pageNumber, this.pageSize);
		} catch (IOException ioex) {
			throw new IOException("Page (" + pageNumber + ") could not be written to the index file.");
		}
	}

	/*
	 * (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.io.ResourceManager#readPageFromResource(byte[], int)
	 */
	@Override
	public void writePagesToResource(byte[][] buffers, CacheableData[] wrappers) throws IOException {
		if (Constants.DEBUG_CHECK) {
			// check that buffers and wrappers array lengths match 
			if (buffers.length != wrappers.length) {
				throw new IllegalArgumentException("Unequal number of buffers and wrappers provided.");
			}

			// check that at least one buffer is provided
			if (buffers.length <= 0) {
				throw new IllegalArgumentException("At least one buffer should be provided.");
			}
		}

		int pageNumber = wrappers[0].getPageNumber();

		if (Constants.DEBUG_CHECK) {
			// check that the page number is within range
			if (pageNumber < FIRST_DATA_PAGE) {
				throw new IOException("Page number " + pageNumber + " is not valid. First data page is " + FIRST_DATA_PAGE + ".");
			}

			for (int i = 0; i < buffers.length; i++) {
				// we can ignore the wrapper, no need to use it
				// check that the page number is within range
				if (wrappers[i].getPageNumber() != pageNumber + i) {
					throw new IOException("Page number " + wrappers[i].getPageNumber() + " of page at position " + i + " is not sequential.");
				}

				// check that we have enough space
				if (buffers[i].length != this.pageSize) {
					throw new IOException("Buffer does not hold a full page (" + this.pageSize + " bytes).");
				}
			}
		}

		ByteBuffer[] b = new ByteBuffer[buffers.length];
		for (int i = 0; i < buffers.length; i++) {
			b[i] = ByteBuffer.wrap(buffers[i], 0, this.pageSize);
		}

		// seek and write the buffer. If the position is beyond the file size,
		// the channel will automatically increase the file length
		try {
			this.ioChannel.position(this.pageSize * (long) pageNumber);
			long totalSize = buffers.length * this.pageSize;
			long bytesRemaining = buffers.length * this.pageSize;
			int currFirstBuffer = 0;
			do {
				bytesRemaining -= this.ioChannel.write(b, currFirstBuffer, buffers.length - currFirstBuffer);
				currFirstBuffer = (int) ((totalSize - bytesRemaining) / this.pageSize);
			} while (bytesRemaining > 0);
		} catch (IOException ioex) {
			throw new IOException("Page sequence [" + pageNumber + ", " + (pageNumber + buffers.length - 1) + "] could not be written to the index file.", ioex);
		}
	}

	// ------------------------------------------------------------------------
	//                         Factory Methods
	// ------------------------------------------------------------------------

	/**
	 * Opens the index contained in the given file and returns a index resource manager to
	 * modify that index on a block bases.
	 * 
	 * @param indexFile
	 *        The file containing the index to be opened.
	 * @param indexedTable
	 *        The schema of the table that is indexed by this index.
	 * @return The IndexResourceManager to operate on the index file.
	 * @throws IOException
	 *         Thrown, if an I/O error occurred.
	 * @throws PageFormatException
	 *         Thrown, if the header of the index contained invalid data.
	 */
	public static IndexResourceManager openIndex(File indexFile, TableSchema indexedTable) throws IOException, PageFormatException {
		if (indexFile == null) {
			throw new NullPointerException("Index file must not be null.");
		}
		if (indexedTable == null) {
			throw new NullPointerException("Indexed table schema must not be null.");
		}

		try {
			// check if the file exists
			if (!indexFile.exists()) {
				throw new IOException("Table file '" + indexFile.getCanonicalPath() + "' does not exist.");
			}

			RandomAccessFile raf = new RandomAccessFile(indexFile, "rw");
			return new IndexResourceManager(raf, indexedTable);
		} catch (SecurityException sex) {
			throw new IOException("The user running the system has insufficient privileges for file manipulation.");
		}
	}

	/**
	 * Creates a new index with the given schema, indexing the a table whose schema is given
	 * by the index schema's {@link de.tuberlin.dima.minidb.catalogue.IndexSchema#getIndexTableSchema()} method.
	 * The index data will be stored in the file with the given name.* The new index will
	 * initially hold a description of the schema in the header and will otherwise contain
	 * an empty root node, which is a leaf page.
	 * 
	 * @param indexFile
	 *        The file to store the new index in.
	 * @param schema
	 *        The schema of the index to create.
	 * @throws IOException
	 *         If an I/O problem occurred.
	 */
	public static IndexResourceManager createIndex(File indexFile, IndexSchema schema) throws IOException {
		if (indexFile == null) {
			throw new NullPointerException("Index file must not be null.");
		}
		if (schema == null) {
			throw new NullPointerException("Index schema must not be null.");
		}

		try {
			// check if the file exists
			if (!indexFile.exists()) {
				// create the file to represent the table
				indexFile.createNewFile();
			}

			// create the random access file and the table manager
			RandomAccessFile raf = new RandomAccessFile(indexFile, "rw");
			return new IndexResourceManager(raf, schema);
		} catch (SecurityException sex) {
			throw new IOException("The user running the system has insufficient privileges for file manipulation.");
		}
	}

	/**
	 * Deletes the index represented by the given file. The index is deleted
	 * by deleting the file physically.
	 * 
	 * @param indexFile
	 *        The file for the index to be deleted.
	 * @throws IOException
	 *         If an I/O problem occurred.
	 */
	public static void deleteIndex(File indexFile) throws IOException {
		try {
			// check if the file exists
			if (!indexFile.exists()) {
				throw new IOException("Index file '" + indexFile.getCanonicalPath() + "' does not exist.");
			}

			// delete the file
			indexFile.delete();
		} catch (SecurityException sex) {
			throw new IOException("The user running the system has insufficient privileges for file manipulation.");
		}
	}

	// ------------------------------------------------------------------------

	// ------------------------------------------------------------------------
	//                        Miscellaneous
	// ------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object o) {
		// there never is another instance that is equal but not identical,
		// because we exclusively lock the files.
		return this == o;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return System.identityHashCode(this);
	}

	// ------------------------------------------------------------------------
	//                         Utility Functions
	// ------------------------------------------------------------------------

	/**
	 * Reads an index schema from a given I/O channel. The schema is read starting at the
	 * channel's current position.
	 * 
	 * @param channel
	 *        The channel from which to read the index schema.
	 * @param tableSchema
	 *        The schema of the table for which the index is created.
	 * @return The index schema read from the channel.
	 * @throws IOException
	 *         Thrown if an error occurred during reading from the channel.
	 * @throws PageFormatException
	 *         Thrown if the data read from the channel did not describe
	 *         a valid index schema.
	 */
	private static IndexSchema readIndexHeader(FileChannel channel, TableSchema tableSchema) throws IOException, PageFormatException {
		ByteBuffer buffer = ByteBuffer.allocate(28);
		buffer.order(ByteOrder.LITTLE_ENDIAN);

		// read one chunk
		readIntoBuffer(channel, buffer, 28);

		// check the magic number
		if (buffer.getInt() != INDEX_HEADER_MAGIC_NUMBER) {
			throw new PageFormatException("Index header invalid. Wrong magic number was found.");
		}
		// check the version number
		if (buffer.getInt() != 0) {
			throw new PageFormatException("Unknown index format version.");
		}

		// read page size and create empty schema
		int pageSize = buffer.getInt();
		int columnNumber = buffer.getInt();
		int rootNode = buffer.getInt();
		int firstLeafNode = buffer.getInt();
		int flags = buffer.getInt();
		boolean unique = (flags & INDEX_HEADER_ATTRIBUTE_UNIQUE_MASK) != 0;
		int highestPage = (int) (channel.size() / pageSize) - 1;

		// sanity checks
		if (columnNumber < 0 || columnNumber >= tableSchema.getNumberOfColumns()) {
			throw new PageFormatException("Index header specified an invalid column to be indexed.");
		}
		if (firstLeafNode < FIRST_DATA_PAGE || firstLeafNode > highestPage) {
			throw new PageFormatException("Index header specified a first leaf page number that is out of range.");
		}
		if (rootNode < FIRST_DATA_PAGE || rootNode > highestPage) {
			throw new PageFormatException("Index header specified a root page number that is out of range.");
		}

		try {
			// instantiate
			PageSize ps = PageSize.getPageSize(pageSize);
			return new IndexSchema(tableSchema, columnNumber, ps, unique, rootNode, firstLeafNode);
		} catch (UnsupportedPageSizeException uspsex) {
			throw new PageFormatException("The index header stated an unsupported page size.");
		}
	}

	/**
	 * Writes an index schema to the given channel. The schema is written to the
	 * channel starting at the channels current position.
	 * 
	 * @param schema
	 *        The schema of the index that will be written to the header.
	 * @param channel
	 *        The channel to write the data to.
	 * @throws IOException
	 *         Thrown, if an error occurred during writing to the channel.
	 */
	private static void writeIndexHeader(IndexSchema schema, FileChannel channel) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(schema.getPageSize().getNumberOfBytes());
		buffer.order(ByteOrder.LITTLE_ENDIAN);

		// first write magic number
		buffer.putInt(INDEX_HEADER_MAGIC_NUMBER);
		// second field write table format version. set to zero, because there is
		// nothing to decide, yet
		buffer.putInt(0);
		// write the page size
		buffer.putInt(schema.getPageSize().getNumberOfBytes());
		// write the number of columns
		buffer.putInt(schema.getColumnNumber());
		// write the root page number
		buffer.putInt(schema.getRootPageNumber());
		// write the first leaf page number
		buffer.putInt(schema.getFirstLeafNumber());

		// write the attribute flags
		int flags = 0;
		flags |= schema.isUnique() ? INDEX_HEADER_ATTRIBUTE_UNIQUE_MASK : 0;
		buffer.putInt(flags);

		// write the buffer
		buffer.flip();
		writeBuffer(channel, buffer);
		buffer.clear();

		// done with the index header
	}

	// ------------------------------------------------------------------------

	/**
	 * Reads the given number of bytes from the given file channel into the given buffer. The read
	 * will start at the file channel's current position. This* method blocks until all bytes are read.
	 * The buffer will contain the new bytes from position 0 to position num.
	 * 
	 * @param channel
	 *        The file channel to read from.
	 * @param buffer
	 *        The buffer into which to read the bytes.
	 * @param num
	 *        The number of bytes to read.
	 * @throws IOException
	 *         Thrown, when any I/O error occurred during reading.
	 */
	private static final void readIntoBuffer(FileChannel channel, ByteBuffer buffer, int num) throws IOException {
		// make space first
		buffer.clear();
		buffer.limit(num);

		// remember the channel position
		long pos = channel.position();
		int read = 0;

		// read as long as we need to
		while (read < num) {
			int count = channel.read(buffer);
			if (count == -1) {
				throw new EOFException();
			}
			read += count;
		}

		// ensure the output state is correct
		buffer.flip();
		if (read > num) {
			buffer.limit(num);
			channel.position(pos + num);
		}
	}

	/**
	 * Reads the given number of bytes from the given file channel into the given buffer. The bytes are read from the
	 * specified
	 * position. This method blocks until all bytes are read. The buffer will contain the new bytes from position
	 * 0 to position num.
	 * 
	 * @param channel
	 *        The file channel to read from.
	 * @param buffer
	 *        The buffer into which to read the bytes.
	 * @param position
	 *        The position in the file channel from where to read the data.
	 * @param num
	 *        The number of bytes to read.
	 * @throws IOException
	 *         Thrown, when any I/O error occurred during reading.
	 */
	private static final void readIntoBuffer(FileChannel channel, ByteBuffer buffer, long position, int num) throws IOException {
		// make space first
		buffer.clear();
		buffer.limit(num);

		int read = 0;

		// read as long as we need to
		while (read < num) {
			int count = channel.read(buffer, position);
			if (count == -1) {
				throw new EOFException();
			}
			read += count;
			position += count;
		}
	}

	/**
	 * Writes the given number of bytes from the given buffer into the given channel.
	 * The method writes from the buffers current position to the buffers current limit.
	 * This method blocks until all bytes are written.
	 * 
	 * @param channel
	 *        The file channel to write to.
	 * @param buffer
	 *        The buffer containing the data to be written..
	 * @throws IOException
	 *         Thrown, when any I/O error occurred during reading.
	 */
	private static void writeBuffer(FileChannel channel, ByteBuffer buffer) throws IOException {
		// write as long as we need to
		long bytes = buffer.remaining();
		while (bytes > 0) {
			bytes -= channel.write(buffer);
		}
	}

	/**
	 * Writes the given number of bytes from the given buffer into the given channel.
	 * The method writes from the buffers current position to the buffers current limit.
	 * This method blocks until all bytes are written.
	 * 
	 * @param channel
	 *        The file channel to write to.
	 * @param buffer
	 *        The buffer containing the data to be written.
	 * @param position
	 *        The position in the file channel to write the data to.
	 * @throws IOException
	 *         Thrown, when any I/O error occurred during the writing.
	 */
	private static final void writeBuffer(FileChannel channel, ByteBuffer buffer, long position) throws IOException {
		// write as long as we need to
		long bytes = buffer.remaining();
		while (bytes > 0) {
			int written = channel.write(buffer, position);
			bytes -= written;
			position += written;
		}
	}

	/**
	 * Writes the given buffer to the page indicated by the given page number.
	 * 
	 * @param buffer
	 *        The buffer of bytes to be written.
	 * @param pageNumber
	 *        The page number of the page where the buffer will be written to.
	 * @param pageSize
	 *        The size of the page that will be written.
	 * @throws IOException
	 *         Thrown, if an I/O problem occurred during the write operation.
	 */
	private final void writePage(byte[] buffer, int pageNumber, int pageSize) throws IOException {
		// create a buffer around the array
		ByteBuffer b = ByteBuffer.wrap(buffer, 0, pageSize);

		// seek and write the buffer. If the position is beyond the file size,
		// the channel will automatically increase the file length
		long position = ((long) pageSize) * ((long) pageNumber);
		writeBuffer(this.ioChannel, b, position);
	}
}
