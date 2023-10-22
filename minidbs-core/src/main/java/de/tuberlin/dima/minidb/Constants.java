package de.tuberlin.dima.minidb;


import java.util.Locale;


/**
 * A collection of global constants that are directly relevant for the external
 * behavior of the system or relevant across different components.
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public abstract class Constants
{
	/*
	 * ********************************************************************************************
	 *                         File/Path and Class Constants
	 * ********************************************************************************************
	 */
	
	/**
	 * The cross-platform directory separator.
	 */
	public static final String DIR_SEPARATOR = "/";
	
	/**
	 * The path to the file holding the configuration data.
	 */
	public static final String CONFIG_FILE_PATH = "config.xml";
	
	/**
	 * The path to the file holding the persisted catalogue.
	 */
	public static final String CATALOGUE_FILE_PATH = "catalogue.xml";
	
	/**
	 * The prefix of the name for temp files created by the query heap.
	 */
	public static final String QUERY_HEAP_TEMP_FILE_PREFIX = "qheap.";
	
	
	/*
	 * ********************************************************************************************
	 *                                 Schema Constants
	 * ********************************************************************************************
	 */
	
	/**
	 * The maximal length that char and varchar fields may have.
	 */
	public static final int MAXIMAL_CHAR_ARRAY_LENGTH = 1024;
	
	/**
	 * The maximal number of columns in a table.
	 */
	public static final int MAX_COLUMNS_IN_TABLE = 1024;
	
	/**
	 * The maximal length of a column name in characters.
	 */
	public static final int MAX_COLUMN_NAME_LENGTH = 256;

	
	/*
	 * ********************************************************************************************
	 *                          Configuration Default Constants
	 * ********************************************************************************************
	 */
	
	/**
	 * The default directory for files containing tables and indexes.
	 */
	static final String DEFAULT_DATA_DIRECTORY = "." + DIR_SEPARATOR + "data" + DIR_SEPARATOR;
	
	/**
	 * The default directory for files containing temporary user data, like sorted sublists
	 * from external sorting or materialized partitions from partitioned hash joins.
	 */
	static final String DEFAULT_TEMPSPACE_DIRECTORY = "." + DIR_SEPARATOR + "tempspace" +
																				DIR_SEPARATOR;
	
	/**
	 * The default size of the query heap in bytes.
	 */
	static final long DEFAULT_QUERY_HEAP_SIZE = 20*1024*1024;
	
	/**
	 * The number of pages with which a page cache is initialized if no other
	 * specific value is given.
	 */
	static final int DEFAULT_INITIAL_CACHE_SIZE = 1000;
	
	/**
	 * The number of I/O buffers used by the buffer pool. The I/O buffers are needed to
	 * sequentialize reads and writes.
	 */
	static final int DEFAULT_NUM_IO_BUFFERS = 128;
	
	/**
	 * The default number of concurrent queries.
	 */
	static final int MAX_CONCURRENT_QUERIES = 10;
	
	/**
	 * The default time (microseconds) that is needed to transfer a block of the
	 * default block size from secondary storage to main memory.
	 */
	static final int DEFAULT_BLOCK_TRANSFER_TIME_READ = 40;
	
	/**
	 * The default time (microseconds) that is needed to transfer a block of the
	 * default block size from main memory to secondary storage.
	 */
	static final int DEFAULT_BLOCK_TRANSFER_TIME_WRITE = 40;
	
	/**
	 * The overhead (in microseconds) for a single block read operation if the block is not part of a sequence that
	 * is read. For magnetic disks, that would correspond to seek time + rotational
	 * latency.
	 */
	static final int DEFAULT_RANDOM_READ_OVERHEAD = 1500;
	
	/**
	 * The overhead (microseconds) for a single block write if the block is not part of a sequence that
	 * is written. For magnetic disks, that would correspond to seek time + rotational
	 * latency.
	 */
	static final int DEFAULT_RANDOM_WRITE_OVERHEAD = 1500;
	
	
	/*
	 * ********************************************************************************************
	 *                          Internal Constants
	 * ********************************************************************************************
	 */
	
	/**
	 * The locale used to bring case insensitive identifiers to a common format.
	 */
	public static final Locale CASE_LOCALE = Locale.ENGLISH;
	
	/**
	 * The default cardinality assumed for tables with unknown size. This value
	 * is set quite high to favor more robust plans. 
	 */
	public static final long DEFAULT_TABLE_CARDINALITY = 50000;
	
	/**
	 * The default number of pages in a resource.
	 */
	public static final int DEFAULT_TABLE_PAGES = 2500;
	
	/**
	 * The default cardinality for columns for unknown cardinality.
	 * The value is set low so that predicates are not assumed to be very selective
	 * and favor robust plans.
	 */
	public static final long DEFAULT_COLUMN_CARDINALITY = 10;
	
	/**
	 * The default length of the prefetching window used by table scans.
	 */
	public static final int DEFAULT_PREFETCHING_LENGTH = 32;
	
	/**
	 * A flag that indicates whether to perform debug checks.
	 */
	public static final boolean DEBUG_CHECK = true;
}
