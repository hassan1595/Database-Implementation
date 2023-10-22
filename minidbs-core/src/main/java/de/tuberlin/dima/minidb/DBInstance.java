package de.tuberlin.dima.minidb;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.InvalidPropertiesFormatException;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import de.tuberlin.dima.minidb.api.AbstractExtensionFactory;
import de.tuberlin.dima.minidb.catalogue.Catalogue;
import de.tuberlin.dima.minidb.catalogue.CatalogueFormatException;
import de.tuberlin.dima.minidb.catalogue.IndexDescriptor;
import de.tuberlin.dima.minidb.catalogue.TableDescriptor;
import de.tuberlin.dima.minidb.core.InternalOperationFailure;
import de.tuberlin.dima.minidb.io.cache.PageFormatException;
import de.tuberlin.dima.minidb.io.index.IndexResourceManager;
import de.tuberlin.dima.minidb.io.manager.BufferPoolException;
import de.tuberlin.dima.minidb.io.manager.BufferPoolManager;
import de.tuberlin.dima.minidb.io.manager.ResourceManager;
import de.tuberlin.dima.minidb.io.tables.TableResourceManager;
import de.tuberlin.dima.minidb.qexec.heap.QueryHeap;
import de.tuberlin.dima.minidb.util.InputStreamFileReader;


/**
 * The root object representing a running instance of the database management system. Acts as the root control block
 * that gives static access to the important structures like <tt>Config</tt>, <tt>Catalogue</tt>,
 * <tt>BufferPoolManager</tt>, ...
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class DBInstance
{
	// --------------------------------------------------------------------------------------------
	//                                        Constants
	// --------------------------------------------------------------------------------------------

	/**
	 * The name of the root logger for this database system.
	 */
	private static final String DEFAULT_GLOBAL_LOGGER_NAME = "MINIDBS-LOGGER";

	/**
	 * The return code signaling normal operation.
	 */
	public static final int RETURN_CODE_OKAY = 0;

	/**
	 * The return code signaling invalid command line parameters.
	 */
	public static final int RETURN_CODE_UNKNOWN_COMMAND_LINE_ARGUMENT = 1;

	/**
	 * The return code signaling invalid files (config, catalogue, ...).
	 */
	public static final int RETURN_CODE_INVALID_CONIG_PARAMETER = 2;

	/**
	 * The return code signaling invalid files (table, index, ...).
	 */
	public static final int RETURN_CODE_CORRUPTED_DATA_FILE = 3;

	/**
	 * The return code signaling an error rooting in the underlying operating system or hardware, such as I/O failures
	 * or missing security privileges.
	 */
	public static final int RETURN_CODE_SYSTEM_PROBLEM = 4;

	/**
	 * The return code signaling that an internal bug prevented the system from continuing its operation.
	 */
	public static final int RETURN_CODE_INTERNAL_PROBLEM = 5;

	/**
	 * The return code signaling that complications occurred during shutdown that might have left the system in an
	 * inconsistent state.
	 */
	public static final int RETURN_CODE_SHUTDOWN_INCOLMPLETE = 6;

	// --------------------------------------------------------------------------------------------
	//                          Global Fields (Top level Control Block)
	// --------------------------------------------------------------------------------------------

	/**
	 * The global logger for messages.
	 */
	protected final Logger LOGGER;

	/**
	 * The configuration of physical parameters that apply to this instance.
	 */
	protected Config CONFIG;

	/**
	 * The catalogue of all registered tables and indexes for this instance.
	 */
	protected Catalogue CATALOGUE;

	/**
	 * The buffer pool through which all I/O of this instance runs.
	 */
	protected BufferPoolManager BUFFER_POOL;
	
	/**
	 * The query heap managing the sort, hash and temp memory for queries.
	 */
	protected QueryHeap QUERY_HEAP;
	
	/**
	 * The query processor used by this instance.
	 */
	private QueryProcessor queryProcessor;

	/**
	 * The path of the configuration file.
	 */
	private final String configFileName;

	/**
	 * The path of the catalogue file.
	 */
	private final String catalogueFileName;
	
	/**
	 * Flag to indicate that the instance is running.
	 */
	private boolean running;

	/**
	 * Gets the logger for this instance.
	 * 
	 * @return The logger for this instance.
	 */
	public Logger getLogger()
	{
		return this.LOGGER;
	}

	/**
	 * Gets the configuration of physical parameters that apply to this instance.
	 * 
	 * @return The configuration of physical parameters that apply to this instance.
	 * @throws InstanceNotStartedException Thrown, if this method is called before the configuration has been
	 *             initialized during system startup.
	 */
	public Config getConfig()
	{
		if (this.CONFIG == null) {
			throw new InstanceNotStartedException();
		}
		else {
			return this.CONFIG;
		}
	}

	/**
	 * Gets the catalogue of tables and indexes for this instance.
	 * 
	 * @return The catalogue of tables and indexes for this instance.
	 * @throws InstanceNotStartedException Thrown, if this method is called before the catalogue has been initialized
	 *             during system startup.
	 */
	public Catalogue getCatalogue()
	{
		if (this.CATALOGUE == null) {
			throw new InstanceNotStartedException();
		}
		else {
			return this.CATALOGUE;
		}
	}

	/**
	 * Gets the buffer pool for this instance.
	 * 
	 * @return The buffer pool for this instance.
	 * @throws InstanceNotStartedException Thrown, if this method is called before the buffer pool has been initialized
	 *             during system startup.
	 */
	public BufferPoolManager getBufferPool()
	{
		if (this.BUFFER_POOL == null) {
			throw new InstanceNotStartedException();
		}
		else {
			return this.BUFFER_POOL;
		}
	}

	/**
	 * Gets the query heap for this instance.
	 * 
	 * @return The query heap for this instance.
	 * @throws InstanceNotStartedException Thrown, if this method is called before the
	 *              query heap has been initialized during system startup.
	 */
	public QueryHeap getQueryHeap()
	{
		if (this.QUERY_HEAP == null) {
			throw new InstanceNotStartedException();
		}
		else {
			return this.QUERY_HEAP;
		}
	}
	
	/**
	 * Checks if this instance is running.
	 * 
	 * @return True, if the instance is running, false otherwise.
	 */
	public boolean isRunning()
	{
		return this.running;
	}

	// --------------------------------------------------------------------------------------------
	//                                  Startup and Shutdown
	// --------------------------------------------------------------------------------------------

	/**
	 * Creates a new instance object that will use a default logger and the given configuration and catalogue.
	 * 
	 * @param configFileName The path to the file that the configuration is stored in.
	 * @param catalogueFileName The path of the file that the catalogue is stored in.
	 */
	public DBInstance(String configFileName, String catalogueFileName)
	{
		// set the parameters
		this.LOGGER = Logger.getLogger(DEFAULT_GLOBAL_LOGGER_NAME);
		this.LOGGER.setLevel(Level.INFO);
		this.LOGGER.setUseParentHandlers(false);
		this.configFileName = configFileName;
		this.catalogueFileName = catalogueFileName;
	}

	/**
	 * Creates a new instance object that will use the given logger and instantiate objects from the extension factory
	 * described by the given class.
	 * 
	 * @param logger The logger to be used with this instance.
	 * @param configFileName The path to the file that the configuration is stored in.
	 * @param catalogueFileName The path of the file that the catalogue is stored in.
	 */
	public DBInstance(Logger logger, String configFileName, String catalogueFileName)
	{
		// set the parameters
		this.LOGGER = logger;
		this.configFileName = configFileName;
		this.catalogueFileName = catalogueFileName;
	}

	/**
	 * Starts up this database instance by loading the configuration and catalogue as well as starting up the I/O layer
	 * and opening all resources and reading their schemas.
	 * 
	 * @return The return code, indicating whether the startup was regular (<tt>RETURN_CODE_OKAY</tt>) or whether
	 *         problems occurred (other return codes <tt>RETURN_CODE_*</tt>).
	 */
	public int startInstance()
	{
		if (this.running) {
			throw new IllegalStateException("The instance is already running.");
		}

		// log the parameters
		this.LOGGER.info("System is starting up...");
		this.LOGGER.info("Using configuration file: " + this.configFileName);
		this.LOGGER.info("Using catalogue file: " + this.catalogueFileName);

		// load the configuration from the config file
		try {
			InputStream is = this.getClass().getResourceAsStream(configFileName);
	    	this.CONFIG = Config.loadConfig(is);
		}
		catch (InvalidPropertiesFormatException ipfex) {
			this.LOGGER.log(Level.SEVERE, "Configuration file '" + this.configFileName + "' has an invalid format: " + ipfex.getMessage(), ipfex);
			return RETURN_CODE_INVALID_CONIG_PARAMETER;
		}
		catch (FileNotFoundException fnfex) {
			this.LOGGER.log(Level.SEVERE, "Configuration file '" + this.configFileName + "' does not exist: " + fnfex.getMessage(), fnfex);
			return RETURN_CODE_INVALID_CONIG_PARAMETER;
		}
		catch (IOException ioex) {
			this.LOGGER.log(Level.SEVERE,
					"Configuration file '" + this.configFileName + "' could not be loaded due to an I/O problem: " + ioex.getMessage(), ioex);
			return RETURN_CODE_SYSTEM_PROBLEM;
		}
		catch (Exception e) {
			this.LOGGER.log(Level.SEVERE, "An error occurred loading the config: " + e.getMessage(), e);
			return RETURN_CODE_INTERNAL_PROBLEM;
		}

		// check for existence of data directory and temp-space directory

		// adapt path in case of jar source
		// The code below tries to access a resource in the class path as a file. Not sure how this ever worked.
		// We skip these tests for now.
//		File file = null;
//		System.out.println("this.CONFIG.getDataDirectory() = " + this.CONFIG.getDataDirectory());
//		if(this.getClass().getResource(this.CONFIG.getDataDirectory()) == null) {
//			file = new File(this.CONFIG.getDataDirectory());
//			System.out.println("1");
//		} else {
//			file = new File(this.getClass().getResource(this.CONFIG.getDataDirectory()).getPath());
//			System.out.println("2");
//		}
//		if (!file.exists() && this.getClass().getResource(this.CONFIG.getDataDirectory()) != null) {
//			this.LOGGER.log(Level.SEVERE, "Data directory '" + file + "' does not exist.");
//			return RETURN_CODE_INVALID_CONIG_PARAMETER;
//		}
//		if(this.getClass().getResource(this.CONFIG.getTempspaceDirectory()) == null)
//			file = new File(this.CONFIG.getTempspaceDirectory());
//		else
//			file = new File(this.getClass().getResource(this.CONFIG.getTempspaceDirectory()).getPath());
//		if (!file.exists() && this.getClass().getResource(this.CONFIG.getTempspaceDirectory()) != null) {
//			this.LOGGER.log(Level.SEVERE, "Tempspace directory '" + file + "' does not exist.");
//			return RETURN_CODE_INVALID_CONIG_PARAMETER;
//		}

		// load the catalogue
		try {
			InputStream is = this.getClass().getResourceAsStream(catalogueFileName);
			this.CATALOGUE = Catalogue.loadCatalogue(is);
		}
		catch (CatalogueFormatException e) {
			this.LOGGER.log(Level.SEVERE, "Catalogue file '" + this.catalogueFileName + "' has an invalid format: " + e.getMessage(), e);
			return RETURN_CODE_INVALID_CONIG_PARAMETER;
		}
		catch (FileNotFoundException fnfex) {
			this.LOGGER.log(Level.SEVERE, "Catalogue file '" + this.catalogueFileName + "' does not exist." + fnfex.getMessage(), fnfex);
			return RETURN_CODE_INVALID_CONIG_PARAMETER;
		}
		catch (IOException ioex) {
			this.LOGGER.log(Level.SEVERE,
					"Catalogue file '" + this.catalogueFileName + "' could not be loaded due to an I/O problem:" + ioex.getMessage(), ioex);
			return RETURN_CODE_SYSTEM_PROBLEM;
		}
		catch (Exception ex) {
			this.LOGGER.log(Level.SEVERE, "An error occurred loading the catalogue: " + ex.getMessage(), ex);
			return RETURN_CODE_INTERNAL_PROBLEM;
		}

		/*
		 * 
		 * 
		 */
		
		// ----------------------------------------------------------------------------------------
		//            from here on, make sure that any error results in a shutdown
		// ----------------------------------------------------------------------------------------

		// open all resources
		try {
			// initialize buffer pool
			try {
				this.BUFFER_POOL = AbstractExtensionFactory.getExtensionFactory().createBufferPoolManager(this.CONFIG, this.LOGGER);
				this.BUFFER_POOL.startIOThreads();
			}
			catch (Exception ex) {
				this.LOGGER.log(Level.SEVERE, "An error occurred starting the buffer pool: " + ex.getMessage(), ex);
				return RETURN_CODE_INTERNAL_PROBLEM;
			}
			
			// initialize the query heap
			try {
				this.QUERY_HEAP = new QueryHeap(this.LOGGER, this.CONFIG);
			}
			catch (Exception ex) {
				this.LOGGER.log(Level.SEVERE, "An error occurred creating the query heap: " + ex.getMessage(), ex);
				return RETURN_CODE_INTERNAL_PROBLEM;
			}

			// open all resources
			try {
				openResources(this.CONFIG, this.CATALOGUE, this.BUFFER_POOL);
			}
			catch (Exception ex) {
				this.LOGGER.log(Level.SEVERE, "An error occurred opening the tables/indexes: " + ex.getMessage(), ex);
				return RETURN_CODE_INTERNAL_PROBLEM;
			}
			
		    // start the query processor
//		    try {
//		    	this.queryProcessor = new QueryProcessor(CATALOGUE, CONFIG, BUFFER_POOL, QUERY_HEAP, 100);
//		    }
//		    catch (Exception ex) {
//		    	LOGGER.log(Level.SEVERE, "An error occurred creating the query processor: " + 
//		    			ex.getMessage(), ex);
//		    	return RETURN_CODE_INTERNAL_PROBLEM;
//		    }

			this.LOGGER.info("System has started.");

			this.running = true;
			return RETURN_CODE_OKAY;
		}
		finally {
			if (!this.running) {
				// shut down all that was started. Make best effort...
			if (this.QUERY_HEAP != null) {
				try {
					this.QUERY_HEAP.closeQueryHeap();
				}
				catch (Throwable t) {}
					this.QUERY_HEAP = null;
				}
			
				if (this.BUFFER_POOL != null) {
					try {
						this.BUFFER_POOL.closeBufferPool();
					}
					catch (Throwable t) {
					}
					this.BUFFER_POOL = null;
				}

				if (this.CATALOGUE != null) {
					closeResources(this.CATALOGUE);
					this.CATALOGUE = null;
				}
			}
		}
	}

	/**
	 * Shuts down this instance. The buffer pool is flushed an closed, all resources are released. The configuration and
	 * catalogue are written and released.
	 * 
	 * @return The return code, indicating whether the shutdown was regular (<tt>RETURN_CODE_OKAY</tt>) or whether
	 *         problems occurred (<tt>RETURN_CODE_SHUTDOWN_INCOMPLETE</tt>).
	 */
	public int shutdownInstance()
	{
		return shutdownInstance(false);
	}
	
	public int shutdownInstance(boolean writeCatalogue)
	{
		this.LOGGER.info("System is shutting down...");

		// now close all the resources in the catalogue as best effort
		boolean allSmooth = true;

	    // close the query heap
	    try {
			this.QUERY_HEAP.closeQueryHeap();
		}
		catch (Exception ex) {
			allSmooth = false;
	    	this.LOGGER.log(Level.WARNING, "Query heap was not properly closed: " + ex.getMessage(), ex);
		}
	    this.QUERY_HEAP = null;
	    
		// close the buffer pool
		try {
			this.BUFFER_POOL.closeBufferPool();
		}
		catch (Exception ex) {
			allSmooth = false;
			this.LOGGER.log(Level.WARNING, "Buffer Pool was not properly closed. " + "Data inconsistencies may occur." + ex.getMessage(), ex);
		}
		this.BUFFER_POOL = null;

		// close the data in the catalogue
		try {
			allSmooth &= closeResources(this.CATALOGUE);
		}
		catch (Exception ex) {
			allSmooth = false;
			this.LOGGER.log(Level.WARNING,
					"An error occurred while closing the resources. " + "Data inconsistencies may occur." + ex.getMessage(), ex);
		}

		// write the none transient elements from the catalogue
		if (writeCatalogue) {
			try {
				File catalogueFile = new File(this.catalogueFileName);
				this.CATALOGUE.writeCatalogue(catalogueFile);
			}
			catch (IOException ioex) {
				allSmooth = false;
				this.LOGGER.log(Level.WARNING, "Catalogue could not be written due to an I/O problem: " + ioex.getMessage()
						+ ". Schema inconsistencies may occur.", ioex);
			}
			catch (Exception ex) {
				allSmooth = false;
				this.LOGGER.log(Level.WARNING, "Catalogue could not be written due to an unexpected problem: " + ex.getMessage()
						+ ". Schema inconsistencies may occur.", ex);
			}
		}
		this.CATALOGUE = null;
		
		// leave config unchanged.
		this.CONFIG = null;

		// done
		this.running = false;
		this.LOGGER.info("System has shut down.");

		return allSmooth ? RETURN_CODE_OKAY : RETURN_CODE_SHUTDOWN_INCOLMPLETE;
	}

	// The old code tried to access a resource in the classpath as a file. Not sure how this ever worked, but when we
	// upload new example solutions, it cannot open the file anymore. However, subsequent code expects to access the
	// data as a file. Our solution is to access the resource as an InputStream and copy the file to the tmp folder.
	// We don't clean up afterwards and the copying might also incur a race condition if multiple users execute the
	// test at the same time. For now, we ignore these edge conditions.
	private static File copyResourceAsTmpFile(String folder, String resourceName) throws IOException {
		final String fileName = folder + resourceName;
		final InputStream resourceAsStream = DBInstance.class.getResourceAsStream(fileName);
		if (resourceAsStream == null) {
			throw new RuntimeException("Could not open resource: " + fileName);
		}
		final Path target = Files.createTempFile("minidbs_", ".tmp");
		Files.copy(resourceAsStream, target, StandardCopyOption.REPLACE_EXISTING);
		final File f = target.toFile();
		f.deleteOnExit();
		return f;
	}

	/**
	 * Creates the runtime structures to access the resources in the catalogue. This method goes over all tables and
	 * indexes in the catalogue and add IDs and resource managers to the Table- and IndexDescriptors.
	 * 
	 * @param config The physical configuration of the system.
	 * @param catalogue The catalogue containing the persistent fields but no runtime structures (resource managers,
	 *            IDs, etc...).
	 * @param buffer The buffer pool where the resources are registered for I/O access.
	 * @throws IOException Thrown, if the files for the tables or indexes cannot be accessed.
	 * @throws PageFormatException Thrown, if the header page of a resource contains invalid data.
	 * @throws BufferPoolException Thrown if the resource cannot be registered at the buffer pool.
	 * @throws CatalogueFormatException Thrown, if the catalogue contains inconsistent descriptions of the tables or
	 *             indexes.
	 */
	private static void openResources(Config config, Catalogue catalogue, BufferPoolManager buffer) throws Exception
	{
		// go over all the tables
		Iterator<TableDescriptor> tableIter = catalogue.getAllTables();

		while (tableIter.hasNext()) {
			TableDescriptor td = tableIter.next();
			//adapt path in case of jar source
			File tableFile = null;
			if(DBInstance.class.getResource(config.getDataDirectory()) == null) {
                tableFile = InputStreamFileReader.inputStreamToFile(DBInstance.class, config.getDataDirectory() + "" + td.getFileName());
                System.out.println("1");
            } else {
				tableFile = copyResourceAsTmpFile(config.getDataDirectory(), td.getFileName());
            }
			// open the table
			TableResourceManager manager = null;
			try {
				manager = TableResourceManager.openTable(tableFile);
				int id = catalogue.reserveNextId();
				buffer.registerResource(id, manager);
				td.setResourceProperties(manager, id);
			}
			catch (Exception e) {
				// close the resource, if it has been opened.
				if (manager != null) {
					try {
						manager.closeResource();
					}
					catch (Throwable t) {
					}
				}
				throw new Exception("The table '" + td.getTableName() + "' in file '" + td.getFileName()
						+ "' could not be opend and registered. Reason:" + e.getMessage(), e);
			}
		}

		// go over all indexes
		Iterator<IndexDescriptor> indexIter = catalogue.getAllIndexes();

		while (indexIter.hasNext()) {
			IndexDescriptor id = indexIter.next();

			// get the table that the index was built upon
			TableDescriptor table = catalogue.getTable(id.getIndexedTableName());
			if (table == null) {
				throw new CatalogueFormatException("Index '" + id.getName() + "' targets none existent table '" + id.getIndexedTableName()
						+ "'.");
			}

			// open the index
			File indexFile = null;
			if(DBInstance.class.getResource(config.getDataDirectory()) == null) {
				indexFile = InputStreamFileReader.inputStreamToFile(DBInstance.class,
						config.getDataDirectory() + "" + id.getFileName());
			} else {
				indexFile = copyResourceAsTmpFile(config.getDataDirectory(), id.getFileName());
			}
			IndexResourceManager manager = null;
			try {
				manager = IndexResourceManager.openIndex(indexFile, table.getSchema());
				int resourceId = catalogue.reserveNextId();
				buffer.registerResource(resourceId, manager);
				id.setResourceProperties(manager, table, resourceId);
			}
			catch (Exception e) {
				// close the resource, if it has been opened.
				if (manager != null) {
					try {
						manager.closeResource();
					}
					catch (Throwable t) {
					}
				}
				throw new Exception("The index '" + id.getName() + "' in file '" + id.getFileName()
						+ "' could not be opend and registered. Reason:" + e.getMessage(), e);
			}
		}
	}

	/**
	 * Closes all resources that are known by the catalogue. This method does not fail when an error occurs, but logs
	 * the error and continues.
	 * 
	 * @param catalogue The catalogue whose resources are to be closed.
	 */
	private boolean closeResources(Catalogue catalogue)
	{
		boolean allSmooth = true;

		// close all indexes first
		Iterator<IndexDescriptor> indexIter = catalogue.getAllIndexes();
		while (indexIter.hasNext()) {
			IndexDescriptor id = indexIter.next();
			try {
				ResourceManager manager = id.getResourceManager();
				if (manager != null) {
					manager.closeResource();
				}
			}
			catch (Exception ex) {
				allSmooth = false;
				this.LOGGER.log(Level.WARNING, "The index '" + id.getName() + "' could not be properly closed. Inconsistencies may occur.", ex);
			}
		}

		// now remove the tables
		Iterator<TableDescriptor> tableIter = catalogue.getAllTables();

		while (tableIter.hasNext()) {
			TableDescriptor td = tableIter.next();
			try {
				ResourceManager manager = td.getResourceManager();
				if (manager != null) {
					manager.closeResource();
				}
			}
			catch (Exception ex) {
				allSmooth = false;
				this.LOGGER.log(Level.WARNING, "The table '" + td.getTableName() + "' could not be properly closed. Inconsistencies may occur.",
						ex);
			}
		}

		return allSmooth;
	}
	
	// --------------------------------------------------------------------------------------------
	//                                    Query processing
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Root function that accepts a query string and uses the query processor to evaluate the
	 * query described by it. The query result is returned through the given result set.
	 * 
	 * @param queryString The query as an SQL string.
	 * @param resultSet The result set to which the resulting tuples are added.
	 */
	public void processQuery(String queryString, ResultHandler resultSet)
	{	    
		if (this.queryProcessor == null) {
			throw new InstanceNotStartedException();
		}
		
		try {
			this.queryProcessor.processQuery(queryString, resultSet);
		}
		catch (InternalOperationFailure iof) {
			resultSet.handleException(iof);
			if (iof.isFatal()) {
				this.LOGGER.log(Level.SEVERE, "A failure of internal operation has occurred: " +
						iof.getMessage() + 
						" Operation cannot continue, the system has to be shut down.", iof);
				shutdownInstance();
			}
			else {
				this.LOGGER.log(Level.WARNING, 
						"A non-critical failure of internal operation has occurred: " +
						iof.getMessage() + " Operation continues.", iof);
			}
		}
	}
}