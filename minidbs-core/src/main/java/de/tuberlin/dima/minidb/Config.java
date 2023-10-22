package de.tuberlin.dima.minidb;


import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.InvalidPropertiesFormatException;
import java.util.Properties;

import de.tuberlin.dima.minidb.io.cache.PageSize;


/**
 * The configuration of physical parameters of the database system. Supports loading and storing
 * to an XML file and also provides default values for all parameters that it serves.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public final class Config
{
	// --------------------------------------------------------------------------------------------
	//                           Constants for keys
	// --------------------------------------------------------------------------------------------
	
	private static final String DATA_DIRECTORY_KEY = "DATA_DIRECTORY";
	
	private static final String TEMPSPACE_DIRECTORY_KEY = "TEMPSPACE_DIRECTORY";
	
	private static final String QUERY_HEAP_SIZE_KEY = "QUERY_HEAP_SIZE";
	
	private static final String CACHE_SIZE_KEY_PREFIX = "CACHE_SIZE_FOR_PAGE_";
	
	private static final String NUM_IO_BUFFERS_KEY = "NUM_IO_BUFFERS";
	
	private static final String NUM_CONCURRENT_QUERIES_KEY = "NUM_CONCURRENT_QUERIES";
	
	private static final String BLOCK_READ_COST = "BLOCK_READ_TRANSFER_NSECS";
	
	private static final String BLOCK_WRITE_COST = "BLOCK_WRITE_TRANSFER_NSECS";
	
	private static final String BLOCK_RANDOM_READ_OVERHEAD = "BLOCK_RANDOM_READ_OVERHEAD_NSECS";
	
	private static final String BLOCK_RANDOM_WRITE_OVERHEAD = "BLOCK_RANDOM_WRITE_OVERHEAD_NSECS";
	
	// --------------------------------------------------------------------------------------------
	//                     Fields, Initialization and Persistence
	// --------------------------------------------------------------------------------------------
	
	
	/**
	 * The properties object that backs this configuration.
	 */
	private final Properties props;
	
	
	
	/**
	 * Creates a new Config object that returns the default values for all
	 * properties.
	 */
	private Config()
	{
		this.props = getDefaults();
	}
	
	/**
	 * Creates a new Config that loads its values from the given input stream. The
	 * input stream is expected to produce UTF-8 encoded XML.
	 * 
	 * @param in The input stream to read the configuration from.
	 * @throws InvalidPropertiesFormatException Thrown, if the format in which the configuration
	 *                                          data from the stream comes is invalid.
	 * @throws IOException Thrown, if the stream cannot be read for some reason.
	 */
	private Config(InputStream in) throws InvalidPropertiesFormatException, IOException 
	{
		Properties defaults = getDefaults();
		this.props = new Properties(defaults);
		this.props.loadFromXML(in);
	}
	
	
	/**
	 * Creates a Config object based on the configuration stored in the given file. The
	 * config object will provide for each property the value that were specified in the
	 * given file, or the default value, if there was no specification for that property
	 * in the file. 
	 * 
	 * @param xmlConfigFile The file containing the configuration as InputStream.
	 * @return A config object, based on the config file and the default values.
	 * 
	 * @throws InvalidPropertiesFormatException Thrown, if the format in which the configuration
	 *                                          data from the stream comes is invalid.
	 * @throws IOException Thrown, if the stream cannot be read for some reason.
	 */
	public static Config loadConfig(InputStream xmlConfigFile)
	throws IOException, InvalidPropertiesFormatException
	{
		// read the properties
		Config conf = new Config(xmlConfigFile);
		
		// check the read contents
		String violatingKey = conf.getInvalidFormatKey();
		if (violatingKey != null) {
			throw new InvalidPropertiesFormatException("The config entry '" + violatingKey +
					"' has an invalid value associated with it.");
		}
		
		return conf;
	}
	
	/**
	 * Creates a new Config object that returns the default values for all
	 * properties.
	 * 
	 * @return A Config object for the default configuration.
	 */
	public static Config getDefaultConfig()
	{
		return new Config();
	}
	
	/**
	 * Stores this configuration to a file in XML format.
	 * 
	 * @param xmlConfFile The abstract pathname to the file to write to config to.
	 * @throws IOException Thrown, if the file cannot be written for some reason.
	 */
	public void saveConfig(File xmlConfFile)
	throws IOException
	{
		OutputStream outStream = new BufferedOutputStream(new FileOutputStream(xmlConfFile));
		this.props.storeToXML(outStream, "Database Physical Parameters");
	}
	
	
	// --------------------------------------------------------------------------------------------
	//                           Constants for keys
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Gets the directory for user data.
	 * 
	 * @return The user data directory name.
	 */
	public String getDataDirectory()
	{
		try {
			return this.props.getProperty(DATA_DIRECTORY_KEY);
		} catch (Exception e) {
			return "";
		}
	}
	
	/**
	 * Gets the directory for temp-space data.
	 * 
	 * @return The temp-space directory name.
	 */
	public String getTempspaceDirectory()
	{
		return this.props.getProperty(TEMPSPACE_DIRECTORY_KEY);
	}
	
	/**
	 * Gets the query heap size.
	 * 
	 * @return The query heap size.
	 */
	public long getQueryHeapSize()
	{
		String val = this.props.getProperty(QUERY_HEAP_SIZE_KEY);
		return Long.parseLong(val);
	}
	
	/**
	 * Gets the capacity of the cache that serves pages of the given size.
	 *  
	 * @param pageSize The page size of the cache to get the capacity for.
	 * @return The cache capacity.
	 */
	public int getCacheSize(PageSize pageSize)
	{
		String key = CACHE_SIZE_KEY_PREFIX + pageSize.name();
		String val = this.props.getProperty(key);
		return Integer.parseInt(val);
	}
	
	/**
	 * Gets the number of I/O buffers to be used by the buffer pool.
	 * 
	 * @return The number of I/O buffers.
	 */
	public int getNumIOBuffers()
	{
		String val = this.props.getProperty(NUM_IO_BUFFERS_KEY);
		return Integer.parseInt(val);
	}
	
	/**
	 * Gets the maximal number of concurrent queries.
	 * 
	 * @return The maximal number of concurrent queries.
	 */
	public int getMaxConcurrentQueries()
	{
		String val = this.props.getProperty(NUM_CONCURRENT_QUERIES_KEY);
		return Integer.parseInt(val);
	}
	
	/**
	 * Gets the cost (in nanoseconds) that it takes to transfer a block
	 * of data from secondary storage to main memory.
	 * 
	 * @return The nanoseconds for a block read.
	 */
	public long getBlockReadCost()
	{
		String val = this.props.getProperty(BLOCK_READ_COST);
		return Long.parseLong(val);
	}
	
	/**
	 * Gets the cost (in nanoseconds) that it takes to transfer a block
	 * of data from main memory to secondary storage.
	 * 
	 * @return The nanoseconds for a block write.
	 */
	public long getBlockWriteCost()
	{
		String val = this.props.getProperty(BLOCK_WRITE_COST);
		return Long.parseLong(val);
	}
	
	/**
	 * Gets the cost (in nanoseconds) that is added as an overhead
	 * when a block read is random compared to when it is part of
	 * a sequence.
	 * 
	 * @return The nanoseconds overhead for a random block read.
	 */
	public long getBlockRandomReadOverhead()
	{
		String val = this.props.getProperty(BLOCK_RANDOM_READ_OVERHEAD);
		return Long.parseLong(val);
	}
	
	/**
	 * Gets the cost (in nanoseconds) that is added as an overhead
	 * when a block write is random compared to when it is part of
	 * a sequence.
	 * 
	 * @return The nanoseconds overhead for a random block write operation.
	 */
	public long getBlockRandomWriteOverhead()
	{
		String val = this.props.getProperty(BLOCK_RANDOM_WRITE_OVERHEAD);
		return Long.parseLong(val);
	}
	
	
	// --------------------------------------------------------------------------------------------
	//                           Setup of default values
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Sets up a properties object containing for every key the default value.
	 * 
	 * @return A properties object containing the default configuration.
	 */
	private final Properties getDefaults()
	{
		Properties p = new Properties();
		
		// set the default data and temp-space directory
		p.setProperty(DATA_DIRECTORY_KEY, Constants.DEFAULT_DATA_DIRECTORY);
		p.setProperty(TEMPSPACE_DIRECTORY_KEY, Constants.DEFAULT_TEMPSPACE_DIRECTORY);
		
		// set the default query heap size
		p.setProperty(QUERY_HEAP_SIZE_KEY, String.valueOf(Constants.DEFAULT_QUERY_HEAP_SIZE));
		
		// create the default cache sizes
		PageSize[] sizes = PageSize.values();
		for (int i = 0; i < sizes.length; i++) {
			p.setProperty(CACHE_SIZE_KEY_PREFIX + sizes[i].name(),
					String.valueOf(Constants.DEFAULT_INITIAL_CACHE_SIZE));
		}
		
		// set the I/O buffer default
		p.setProperty(NUM_IO_BUFFERS_KEY, String.valueOf(Constants.DEFAULT_NUM_IO_BUFFERS));
		
		// set the concurrent queries default
		p.setProperty(NUM_CONCURRENT_QUERIES_KEY,
				String.valueOf(Constants.MAX_CONCURRENT_QUERIES));
		
		// set the I/O cost values
		p.setProperty(BLOCK_READ_COST, String.valueOf(Constants.DEFAULT_BLOCK_TRANSFER_TIME_READ));
		p.setProperty(BLOCK_WRITE_COST, String.valueOf(Constants.DEFAULT_BLOCK_TRANSFER_TIME_WRITE));
		p.setProperty(BLOCK_RANDOM_READ_OVERHEAD, String.valueOf(Constants.DEFAULT_RANDOM_READ_OVERHEAD));
		p.setProperty(BLOCK_RANDOM_WRITE_OVERHEAD, String.valueOf(Constants.DEFAULT_RANDOM_WRITE_OVERHEAD));
		
		return p;
	}
	
	
	/**
	 * Tests this config object and checks whether it contains a malformed entry.
	 * 
	 * @return The first key for which a malformed entry was encountered.
	 */
	private String getInvalidFormatKey()
	{
		try {
			getDataDirectory();
		}
		catch (Throwable t) {
			return DATA_DIRECTORY_KEY;
		}
		
		try {
			getQueryHeapSize();
		}
		catch (Throwable t) {
			return QUERY_HEAP_SIZE_KEY;
		}
		
		for (PageSize pz : PageSize.values()) {
			try {
				getCacheSize(pz);
			}
			catch (Throwable t) {
				return CACHE_SIZE_KEY_PREFIX + pz.name();
			}
		}
		
		try {
			getNumIOBuffers();
		}
		catch (Throwable t) {
			return NUM_IO_BUFFERS_KEY;
		}
		
		try {
			getMaxConcurrentQueries();
		}
		catch (Throwable t) {
			return NUM_CONCURRENT_QUERIES_KEY;
		}
		
		try {
			getBlockReadCost();
		}
		catch (Throwable t) {
			return BLOCK_READ_COST;
		}
		
		try {
			getBlockWriteCost();
		}
		catch (Throwable t) {
			return BLOCK_WRITE_COST;
		}
		
		try {
			getBlockRandomReadOverhead();
		}
		catch (Throwable t) {
			return BLOCK_RANDOM_READ_OVERHEAD;
		}
		
		try {
			getBlockRandomWriteOverhead();
		}
		catch (Throwable t) {
			return BLOCK_RANDOM_WRITE_OVERHEAD;
		}
		
		return null;
	}
}
