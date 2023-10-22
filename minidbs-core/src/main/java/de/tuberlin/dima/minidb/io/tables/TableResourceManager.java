package de.tuberlin.dima.minidb.io.tables;

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
import de.tuberlin.dima.minidb.api.AbstractExtensionFactory;
import de.tuberlin.dima.minidb.catalogue.ColumnSchema;
import de.tuberlin.dima.minidb.catalogue.TableSchema;
import de.tuberlin.dima.minidb.core.BasicType;
import de.tuberlin.dima.minidb.core.DataType;
import de.tuberlin.dima.minidb.io.cache.CacheableData;
import de.tuberlin.dima.minidb.io.cache.PageFormatException;
import de.tuberlin.dima.minidb.io.cache.PageSize;
import de.tuberlin.dima.minidb.io.manager.ResourceManager;

/**
 * This class implements the access to a file that contains table data. It provides
 * methods to create, open, close and delete a table. Once a TableManager is
 * created for a table, it exclusively locks the table's file. The table manager
 * allows to read and write binary pages from and to a table.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class TableResourceManager extends ResourceManager {
	/**
	 * The magic number that identifies a file as a table file.
	 */
	private static final int TABLE_HEADER_MAGIC_NUMBER = 0xDEAFD00D;

	/**
	 * The mask to access the 'nullable' bit in the column attributes.
	 */
	private static final int TABLE_HEADER_COLUMN_ATTRIBUTE_NULLABLE_MASK = 0x1;

	/**
	 * The mask to access the 'unique' bit in the column attributes.
	 */
	private static final int TABLE_HEADER_COLUMN_ATTRIBUTE_UNIQUE_MASK = 0x2;

	/**
	 * The factory used to create new pages.
	 */
	private static final AbstractExtensionFactory pageFactory = AbstractExtensionFactory.getExtensionFactory();

	/**
	 * The I/O channel through which the table file is accessed.
	 */
	private final FileChannel ioChannel;

	/**
	 * The lock we hold on the table file.
	 */
	private final FileLock theLock;

	/**
	 * The schema of the table in the file.
	 */
	private final TableSchema schema;

	/**
	 * The size of a page in bytes.
	 */
	private final int pageSize;

	/**
	 * The first page that contains actual data (rather than metadata, like schema)
	 */
	private final int firstDataPageNumber;

	/**
	 * The last page that contains actual data (rather than metadata, like schema)
	 */
	private int lastDataPageNumber;

	// ------------------------------------------------------------------------
	//                        Constructor & Life-Cycle
	// ------------------------------------------------------------------------

	/**
	 * Creates a new table manager to work on an existing table. The table is represented
	 * by the given handle to the file.
	 * 
	 * @param fileHandle
	 *        The handle to the table's file.
	 * @throws IOException
	 *         If the table file could not be accessed due to an I/O error.
	 * @throws PageFormatException
	 *         If the table file did not contain a valid header.
	 */
	protected TableResourceManager(RandomAccessFile fileHandle) throws IOException, PageFormatException {
		// Open the channel. If anything fails, make sure we close it again
		try {
			this.ioChannel = fileHandle.getChannel();
			try {
				this.theLock = this.ioChannel.tryLock();
			} catch (OverlappingFileLockException oflex) {
				throw new IOException("Table file locked by other consumer.");
			}

			if (this.theLock == null) {
				throw new IOException("Could acquire table file handle for exclusive usage. File locked otherwise.");
			}
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

		// rewind to the beginning of the file
		this.ioChannel.position(0);

		// read the schema
		this.schema = readTableHeader(this.ioChannel);
		this.pageSize = this.schema.getPageSize().getNumberOfBytes();

		// find page numbers
		this.firstDataPageNumber = (int) (this.ioChannel.position() / this.schema.getPageSize().getNumberOfBytes()) + 1;
		this.lastDataPageNumber = (int) ((this.ioChannel.size() - 1) / this.schema.getPageSize().getNumberOfBytes());
	}

	/**
	 * Creates a table manager that newly creates a table for a given (existing) file.
	 * For that, the table manager opens the file and writes the schema into the file,
	 * representing this table's header.
	 * 
	 * @param fileHandle
	 *        The handle to the file which is used for the table.
	 * @param schema
	 *        The schema for the new table.
	 * @throws IOException
	 *         If the table could not be created due to an I/O error.
	 */
	protected TableResourceManager(RandomAccessFile fileHandle, TableSchema schema) throws IOException {
		// Open the channel. If anything fails, make sure we close it again
		try {
			this.ioChannel = fileHandle.getChannel();
			try {
				this.theLock = this.ioChannel.tryLock();
			} catch (OverlappingFileLockException oflex) {
				throw new IOException("Table file locked by other consumer.");
			}

			if (this.theLock == null) {
				throw new IOException("Could acquire table file handle for exclusive usage. File locked otherwise.");
			}
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

		// set member variables
		this.schema = schema;
		this.pageSize = schema.getPageSize().getNumberOfBytes();

		// rewind and write the header
		this.ioChannel.position(0);
		writeTableHeader(schema, this.ioChannel);

		// set first empty page and first data page both to the first page after what is
		// consumed by the header.
		this.firstDataPageNumber = (int) (this.ioChannel.position() / schema.getPageSize().getNumberOfBytes()) + 1;
		this.lastDataPageNumber = this.firstDataPageNumber - 1;
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
		// try to release the lock
		if (this.theLock != null) {
			try {
				this.theLock.release();
			} catch (Throwable ignored) {
				// ignore everything, just try to close
			}
		}

		// close the channel before propagating the exception
		if (this.ioChannel != null) {
			try {
				this.ioChannel.close();
			} catch (Throwable ignored) {
				// ignore everything, just try to close
			}
		}
	}

	// ------------------------------------------------------------------------
	//                                Accessors
	// ------------------------------------------------------------------------

	/**
	 * Gets the schema of the table in the resource handles through this resource manager.
	 * 
	 * @return The schema of the table.
	 */
	public TableSchema getSchema() {
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

	/**
	 * Gets the number of the first page that actually contains user data, i.e.
	 * the first page after the header is complete. That page may not yet
	 * exist, for example immediately after the creation of a resource. It will
	 * always exist if it is smaller or equal to the last data page number.
	 * 
	 * @return The number of the first page..
	 */
	public int getFirstDataPageNumber() {
		return this.firstDataPageNumber;
	}

	/**
	 * Gets the number of the last page number that actually contains data. It may not
	 * necessarily exist yet, for example directly after the creation of a table. It does exist,
	 * if it is larger or equal to the first data page number.
	 * 
	 * @return The number of the last page.
	 */
	public int getLastDataPageNumber() {
		return this.lastDataPageNumber;
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
		this.ioChannel.truncate(this.firstDataPageNumber * this.schema.getPageSize().getNumberOfBytes());
		this.lastDataPageNumber = this.firstDataPageNumber - 1;
	}

	/*
	 * (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.io.ResourceManager#reserveNewPage(byte[])
	 */
	@Override
	public final synchronized TablePage reserveNewPage(byte[] buffer) throws PageFormatException {
		// we can ignore the parameters object, because there is nothing to configure here
		if (buffer.length < this.schema.getPageSize().getNumberOfBytes()) {
			throw new IllegalArgumentException("The buffer to initialize the page to is too small.");
		}

		// determine the next empty page number
		int nextEmptyPageNumber = this.lastDataPageNumber >= this.firstDataPageNumber ? this.lastDataPageNumber + 1 : this.firstDataPageNumber;

		TablePage newPage = pageFactory.initTablePage(this.schema, buffer, nextEmptyPageNumber);

		// increment the counter
		this.lastDataPageNumber = nextEmptyPageNumber;

		return newPage;
	}

	/*
	 * (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.io.ResourceManager#reserveNewPage(byte[], java.lang.Enum)
	 */
	@Override
	public final synchronized TablePage reserveNewPage(byte[] buffer, Enum<?> type) throws PageFormatException {
		return reserveNewPage(buffer);
	}

	/*
	 * (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.io.ResourceManager#writePageToResource(byte[], int)
	 */
	@Override
	public void writePageToResource(byte[] buffer, CacheableData wrapper) throws IOException {
		int pageNumber = wrapper.getPageNumber();

		if (Constants.DEBUG_CHECK) {
			// we can ignore the wrapper, no need to use it
			// check that the page number is within range
			if (pageNumber < this.firstDataPageNumber) {
				throw new IOException("Page number " + pageNumber + " is not valid. " + "First data page is " + this.firstDataPageNumber + ".");
			}

			// check that we have enough space
			if (buffer.length != this.pageSize) {
				throw new IllegalArgumentException("The given buffer does not match for the specified page size (" + this.pageSize + " bytes).");
			}
		}

		ByteBuffer b = ByteBuffer.wrap(buffer, 0, this.pageSize);

		// seek and write the buffer. If the position is beyond the file size,
		// the channel will automatically increase the file length
		try {
			long position = (this.pageSize * (long) pageNumber);
			writeBuffer(this.ioChannel, b, position);
		} catch (IOException ioex) {
			throw new IOException("Page " + pageNumber + " could not be written to the table file.", ioex);
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
			// we can ignore the wrapper, no need to use it
			// check that the page number is within range
			if (pageNumber < this.firstDataPageNumber) {
				throw new IOException("Page number " + pageNumber + " is not valid. " + "First data page is " + this.firstDataPageNumber + ".");
			}

			for (int i = 0; i < buffers.length; i++) {
				// we can ignore the wrapper, no need to use it
				// check that the page number is within range
				if (wrappers[i].getPageNumber() != pageNumber + i) {
					throw new IOException("Page number " + wrappers[i].getPageNumber() + " of page at position " + i + " is not sequential.");
				}

				// check that we have enough space
				if (buffers[i].length != this.pageSize) {
					throw new IllegalArgumentException("The given buffer does not match for the specified page size (" + this.pageSize + " bytes).");
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
			long position = this.pageSize * (long) pageNumber;
			this.ioChannel.position(position);
			long totalSize = buffers.length * this.pageSize;
			long bytesRemaining = totalSize;
			int currFirstBuffer = 0;
			do {
				bytesRemaining -= this.ioChannel.write(b, currFirstBuffer, buffers.length - currFirstBuffer);
				currFirstBuffer = (int) ((totalSize - bytesRemaining) / this.pageSize);
			} while (bytesRemaining > 0);
		} catch (IOException ioex) {
			throw new IOException("Page sequence [" + pageNumber + ", " + (pageNumber + buffers.length - 1) + "] could not be written to the table file.", ioex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.io.ResourceManager#readPageFromResource(byte[], int)
	 */
	@Override
	public TablePage readPageFromResource(byte[] buffer, int pageNumber) throws IOException {
		if (Constants.DEBUG_CHECK) {
			// check that the page number is within range
			if (pageNumber < this.firstDataPageNumber) {
				throw new IOException("Page number " + pageNumber + " is not valid. " + "First data page is " + this.firstDataPageNumber + ".");
			}

			// check that we have enough space
			if (buffer.length != this.pageSize) {
				throw new IOException("Buffer is not big enough to hold a page.");
			}
		}

		// seek and read the buffer
		ByteBuffer b = ByteBuffer.wrap(buffer, 0, this.pageSize);

		try {
			long position = (this.pageSize * (long) pageNumber);
			readIntoBuffer(this.ioChannel, b, position, this.pageSize);

		} catch (IOException ioex) {
			throw new IOException("Page " + pageNumber + " could not be read from table file.", ioex);
		}

		// create a table page for the loaded data
		try {
			return pageFactory.createTablePage(this.schema, buffer);
		} catch (PageFormatException pfex) {
			throw new IOException("Page could not be fetched because it is corrupted.", pfex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.io.ResourceManager#readPageFromResource(byte[], int)
	 */
	@Override
	public TablePage[] readPagesFromResource(byte[][] buffers, int firstPageNumber) throws IOException {
		// check that at least one buffer is provided
		if (buffers.length <= 0) {
			throw new IllegalArgumentException("At least one buffer should be provided.");
		}

		if (Constants.DEBUG_CHECK) {
			// check that the page number is within range
			if (firstPageNumber < this.firstDataPageNumber) {
				throw new IOException("Page number " + firstPageNumber + " is not valid. " + "First data page is " + this.firstDataPageNumber + ".");
			}

			for (int i = 0; i < buffers.length; i++) {
				// check that we have enough space
				if (buffers[i].length != this.pageSize) {
					throw new IllegalArgumentException("Buffer is not big enough to hold a page.");
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
			throw new IOException("Page sequence [" + firstPageNumber + ", " + (firstPageNumber + buffers.length - 1) + "] could not be read from table file.",
				ioex);
		}

		// wrap the loaded buffers in CacheableData objects 
		TablePage[] pages = new TablePage[buffers.length];
		for (int i = 0; i < buffers.length; i++) {
			try {
				pages[i] = pageFactory.createTablePage(this.schema, buffers[i]);

			} catch (PageFormatException pfex) {
				throw new IOException("Page could not be fetched because it is corrupted.", pfex);
			}
		}

		return pages;
	}

	// ------------------------------------------------------------------------
	//                           Miscellaneous
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
	//                         Factory Methods
	// ------------------------------------------------------------------------

	/**
	 * Opens the table contained in the given file and returns a table manager to
	 * modify that table in blocks.
	 * 
	 * @param tableFile
	 *        The file containing the table to be opened.
	 * @return The TableManager to operate on the table file.
	 * @throws IOException
	 *         Thrown, if an I/O error occurred.
	 * @throws PageFormatException
	 *         Thrown, if the header of the table contained invalid data.
	 */
	public static TableResourceManager openTable(File tableFile) throws IOException, PageFormatException {
		if (tableFile == null) {
			throw new NullPointerException("Table file must not be null.");
		}

		try {
			// check if the file exists
			if (!tableFile.exists()) {
				throw new IOException("Table file '" + tableFile.getCanonicalPath() + "' does not exist.");
			}

			RandomAccessFile raf = new RandomAccessFile(tableFile, "rwd");
			return new TableResourceManager(raf);
		} catch (SecurityException sex) {
			throw new IOException("The user running the system has insufficient privileges for file manipulation.");
		}
	}

	/**
	 * Creates a new table with the given schema. The tables data will be stored in the given file.
	 * The new table will initially hold a description of the schema in the header and will
	 * otherwise be empty.
	 * 
	 * @param tableFile
	 *        The file to store the new table in.
	 * @throws IOException
	 *         If an I/O problem occurred.
	 */
	public static TableResourceManager createTable(File tableFile, TableSchema schema) throws IOException {
		if (tableFile == null) {
			throw new NullPointerException("Table file must not be null.");
		}
		if (schema == null) {
			throw new NullPointerException("Table schema must not be null.");
		}

		try {
			// check if the file exists
			if (!tableFile.exists()) {
				// create the file to represent the table
				tableFile.createNewFile();
			}

			// create the random access file and the table manager
			RandomAccessFile raf = new RandomAccessFile(tableFile, "rwd");
			return new TableResourceManager(raf, schema);
		} catch (SecurityException sex) {
			throw new IOException("The user running the system has insufficient privileges for file manipulation.");
		}
	}

	/**
	 * Deletes the table represented by the given file. The tables is deleted
	 * by deleting the file physically.
	 * 
	 * @param tableFile
	 *        The file for the table to be deleted.
	 * @throws IOException
	 *         If an I/O problem occurred.
	 */
	public static void deleteTable(File tableFile) throws IOException {
		try {
			// check if the file exists
			if (!tableFile.exists()) {
				throw new IOException("Table file '" + tableFile.getCanonicalPath() + "' does not exist exist.");
			}

			// delete the file
			tableFile.delete();
		} catch (SecurityException sex) {
			throw new IOException("The user running the system has insufficient privileges for file manipulation.");
		}
	}

	// ------------------------------------------------------------------------
	//                         Utility Functions
	// ------------------------------------------------------------------------

	/**
	 * Reads a table schema from a given I/O channel. The schema is read starting at the
	 * channel's current position.
	 * 
	 * @param channel
	 *        The channel from which to read the table schema.
	 * @return The table schema read from the channel.
	 * @throws IOException
	 *         Thrown if an error occurred during reading from the channel.
	 * @throws PageFormatException
	 *         Thrown if the data read from the channel did not describe
	 *         a valid table schema.
	 */
	private static TableSchema readTableHeader(FileChannel channel) throws IOException, PageFormatException {
		ByteBuffer buffer = ByteBuffer.allocate(4096);
		buffer.order(ByteOrder.LITTLE_ENDIAN);

		// read one chunk
		readIntoBuffer(channel, buffer, 16);

		// check the magic number
		if (buffer.getInt() != TABLE_HEADER_MAGIC_NUMBER) {
			throw new PageFormatException("Table header invalid. Magic number not found.");
		}
		// check the version number
		if (buffer.getInt() != 0) {
			throw new PageFormatException("Unknown table format version.");
		}

		// read page size and create empty schema
		TableSchema schema = null;
		int pageSize = buffer.getInt();
		try {
			schema = new TableSchema(pageSize);
		} catch (IllegalArgumentException iaex) {
			throw new PageFormatException("Table header specified an unsupported page size: " + pageSize);
		}

		// get column count
		int numCols = buffer.getInt();
		if (numCols <= 0 || numCols > Constants.MAX_COLUMNS_IN_TABLE) {
			throw new PageFormatException("Number of columns out of range: " + numCols);
		}

		// read all columns
		for (int i = 0; i < numCols; i++) {
			// get the column fix length part: type, array length, attributes (nullable)
			// and length of the name.
			readIntoBuffer(channel, buffer, 16);

			// get type
			int typeNum = buffer.getInt();
			if (typeNum < 0 || typeNum >= BasicType.values().length) {
				throw new PageFormatException("Invalid data type given for column " + i + ": " + typeNum);
			}
			BasicType basicType = BasicType.values()[typeNum];

			// get array length
			int length = buffer.getInt();
			if (basicType.isArrayType() && (length <= 0 || length > Constants.MAXIMAL_CHAR_ARRAY_LENGTH)) {
				throw new PageFormatException("Length for column " + i + "is out of bounds: " + length);
			}

			DataType type = DataType.get(basicType, length);

			// get the attribute flags
			int attributes = buffer.getInt();
			boolean nullable = (attributes & TABLE_HEADER_COLUMN_ATTRIBUTE_NULLABLE_MASK) != 0;
			boolean unique = (attributes & TABLE_HEADER_COLUMN_ATTRIBUTE_UNIQUE_MASK) != 0;

			// get the name
			int nameLength = buffer.getInt();
			if (nameLength < 0 || nameLength >= Constants.MAX_COLUMN_NAME_LENGTH) {
				throw new PageFormatException("Length for column " + i + "'s name is out of bounds: " + nameLength);
			}
			readIntoBuffer(channel, buffer, 2 * nameLength);
			StringBuilder bld = new StringBuilder(nameLength);
			for (int c = 0; c < nameLength; c++) {
				bld.append(buffer.getChar());
			}

			schema.addColumn(ColumnSchema.createColumnSchema(bld.toString(), type, nullable, unique));
		}

		// done
		return schema;
	}

	/**
	 * Writes a table schema to the given channel. The schema is written to the
	 * channel starting at the channels current position.
	 * 
	 * @param schema
	 *        The schema to be written.
	 * @param channel
	 *        The channel to write the schema to.
	 * @throws IOException
	 *         Thrown, if an error occurred during writing to the channel.
	 */
	private static void writeTableHeader(TableSchema schema, FileChannel channel) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(schema.getPageSize().getNumberOfBytes());
		buffer.order(ByteOrder.LITTLE_ENDIAN);

		// first write magic number
		buffer.putInt(TABLE_HEADER_MAGIC_NUMBER);
		// second field write table format version. set to zero, because there is
		// nothing to decide, yet
		buffer.putInt(0);
		// write the page size
		buffer.putInt(schema.getPageSize().getNumberOfBytes());
		// write the number of columns
		buffer.putInt(schema.getNumberOfColumns());

		// write the buffer
		buffer.flip();
		writeBuffer(channel, buffer);
		buffer.clear();

		// write all the columns
		for (int i = 0; i < schema.getNumberOfColumns(); i++) {
			ColumnSchema cs = schema.getColumn(i);

			// write the data type first
			buffer.putInt(cs.getDataType().getBasicType().ordinal());
			// write the length
			buffer.putInt(cs.getDataType().getLength());
			// write the attribute field (include the nullable flag)
			int attribs = 0;
			if (cs.isNullable()) {
				attribs |= TABLE_HEADER_COLUMN_ATTRIBUTE_NULLABLE_MASK;
			}
			if (cs.isUnique()) {
				attribs |= TABLE_HEADER_COLUMN_ATTRIBUTE_UNIQUE_MASK;
			}
			buffer.putInt(attribs);

			// get the name and check it
			String name = cs.getColumnName();
			if (name.length() == 0 || name.length() > Constants.MAX_COLUMN_NAME_LENGTH) {
				throw new IllegalArgumentException("Column name length has invalid length.");
			}
			// add name length
			buffer.putInt(name.length());

			// add the name
			for (int s = 0; s < name.length(); s++) {
				buffer.putChar(name.charAt(s));
			}

			// write the buffer
			buffer.flip();
			writeBuffer(channel, buffer);
			buffer.clear();
		}
		// done with the table header
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
	private static void readIntoBuffer(FileChannel channel, ByteBuffer buffer, int num) throws IOException {
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

}
