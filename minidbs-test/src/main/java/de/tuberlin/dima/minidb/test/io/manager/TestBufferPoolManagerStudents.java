package de.tuberlin.dima.minidb.test.io.manager;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import de.tuberlin.dima.minidb.Config;
import de.tuberlin.dima.minidb.api.AbstractExtensionFactory;
import de.tuberlin.dima.minidb.catalogue.ColumnSchema;
import de.tuberlin.dima.minidb.catalogue.TableSchema;
import de.tuberlin.dima.minidb.core.BigIntField;
import de.tuberlin.dima.minidb.core.CharField;
import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataFormatException;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.core.DataType;
import de.tuberlin.dima.minidb.core.DateField;
import de.tuberlin.dima.minidb.core.DoubleField;
import de.tuberlin.dima.minidb.core.FloatField;
import de.tuberlin.dima.minidb.core.IntField;
import de.tuberlin.dima.minidb.core.SmallIntField;
import de.tuberlin.dima.minidb.core.TimeField;
import de.tuberlin.dima.minidb.core.TimestampField;
import de.tuberlin.dima.minidb.core.VarcharField;
import de.tuberlin.dima.minidb.io.cache.CacheableData;
import de.tuberlin.dima.minidb.io.cache.PageExpiredException;
import de.tuberlin.dima.minidb.io.cache.PageFormatException;
import de.tuberlin.dima.minidb.io.cache.PageSize;
import de.tuberlin.dima.minidb.io.manager.BufferPoolException;
import de.tuberlin.dima.minidb.io.manager.ResourceManager;
import de.tuberlin.dima.minidb.io.tables.PageTupleAccessException;
import de.tuberlin.dima.minidb.io.tables.TablePage;

/**
 * The public test case for the BufferPoolManager.
 * 
 * @author Michael Saecker
 */
public class TestBufferPoolManagerStudents extends AbstractTestBufferPoolManager
{

	/**
	 * Location of the config file for the database instance.
	 */
	protected static final String configFileName = "/config.xml";

	/**
	 * The configuration of the database instance.
	 */
	protected Config config = null;
	
	/**
	 * Seed for the random number generator.
	 */
	private static final long SEED = 23956235235L;
	
	/**
	 * The random number generator.
	 */
	private static Random random = new Random(SEED);
	
	/**
	 * The available resource managers mapped to their ids.
	 */
	protected Map<Integer, ResourceManager> resourceManagers;
	
	/**
	 * The schemas of the resource managers mapped to their ids.
	 */
	protected Map<Integer, TableSchema> schemas;
	
	/**
	 * List of threads that are allowed to run after a test has finished.
	 */
	private Thread[] allowedThreads;
	
	
	
	/**
	 * Sets up the environment before each test.
	 */
	@Before
	public void setUp() throws Exception
	{
		// initialize the extension factory to have access to the user methods
		AbstractExtensionFactory.initializeDefault();

		InputStream is = this.getClass().getResourceAsStream(configFileName);

    	this.config = Config.loadConfig(is);
    	
    	this.resourceManagers = new HashMap<Integer, ResourceManager>();
    	this.schemas = new HashMap<Integer, TableSchema>();

    	// remember threads that are allowed to live
    	this.allowedThreads = new Thread[Thread.currentThread().getThreadGroup().activeCount()];
    	
    	Thread.currentThread().getThreadGroup().enumerate(this.allowedThreads, true);
    	    	
    	// get buffer pool manager from extension factory
		this.underTest = AbstractExtensionFactory.getExtensionFactory().createBufferPoolManager(this.config, Logger.getLogger("Test-BufferPoolManager-Logger"));
		// start buffer pool manager
		this.underTest.startIOThreads();
	}
	
	/**
	 * Clears up the environment after each test. 
	 */
	@SuppressWarnings("deprecation")
	@After
	public void tearDown() 
		throws Exception
	{
    	this.underTest.closeBufferPool();
    	
    	try
    	{
    		// wait a second for the bpm to shut down
    		Thread.sleep(1000);
    	} 
    	catch (InterruptedException e) {}

		// clear resources
		this.underTest = null;
		this.config = null;
		// kill all threads that are not allowed to run (garbage remaining from wrong shutdown methods)
		boolean foundAnyNonAllowed = false;
		Thread[] runningThreads = new Thread[Thread.currentThread().getThreadGroup().activeCount()]; 
		Thread.currentThread().getThreadGroup().enumerate(runningThreads, true);
		for(Thread run : runningThreads)
		{
			// iterate all allowed threads to find out if the thread is allowed to run (ran before the test)
			boolean found = false;
			for(int i = 0; i < this.allowedThreads.length; i++)
			{
				if(this.allowedThreads[i] == run)
				{
					found = true;
				}
			}
			// check if thread is allowed to run
			if(!found)
			{
				// stop thread the hard way (deprecation issue noted) 
				System.out.println("Killing thread: " + run.getName());
				run.stop();
				foundAnyNonAllowed = true;
			}
		}
		// if any thread was still running a deadlock probably occured
		if(foundAnyNonAllowed)
		{
			throw new IllegalStateException("Deadlock");
		}
		// clear resource managers
		closeTableResources();
	}
	
	/**
	 * Tests whether the buffer pool exception is correctly thrown after the BPM has been closed.
	 */
	@Test
	public void testClosing()
	{
		// close buffer pool 
		this.underTest.closeBufferPool();
		// try adding a resource
		try
		{
			// try adding a resource manager
			MockResourceManager mrm = new MockResourceManager(generateSchema(PageSize.SIZE_8192), PageSize.SIZE_8192, false);
			this.underTest.registerResource(42346, mrm);
			fail("BufferPoolException should have been thrown because the BufferPoolManager was closed.");
		}
		catch (BufferPoolException bpe)
		{
		// do nothing
		}
		
		// try creating a new page
		try
		{
			// try adding a resource manager
			this.underTest.createNewPageAndPin(1);
			fail("BufferPoolException should have been thrown because the BufferPoolManager was closed.");
		}
		catch (BufferPoolException bpe)
		{
			// expected exception was thrown
		} catch (IOException e) 
		{
			e.printStackTrace();
			fail("A different Exception than a BufferPoolException was thrown.");
		}

		// try creating a new page
		try
		{
			// try adding a resource manager
			this.underTest.prefetchPage(5, 10);
			fail("BufferPoolException should have been thrown because the BufferPoolManager was closed.");
		}
		catch (BufferPoolException bpe)
		{
			// expected exception was thrown
		}

		// try creating a new page
		try
		{
			// try adding a resource manager
			this.underTest.getPageAndPin(5, 10);
			fail("BufferPoolException should have been thrown because the BufferPoolManager was closed.");
		}
		catch (BufferPoolException bpe)
		{
			// expected exception was thrown
		} catch (IOException e) {
			e.printStackTrace();
			fail("A different Exception than a BufferPoolException was thrown.");
		}	
	}
	
	/**
	 * Tests the creation of pages.
	 */
	@Test
	public void testCreateNewPageAndPin()
	{
		
		// try creating a page for a not contained resource
		try {
			this.underTest.createNewPageAndPin(234);
			fail("Creating a new page for a not registered resource should throw a BufferPoolException.");
		} 
		catch (BufferPoolException e1) 
		{
			// correct behavior
		} 
		catch (IOException e1) 
		{
			e1.printStackTrace();
			fail("An exception different from a BufferPoolException has been thrown.");
		}
		
		// create a new page, pin and retrieve it again
		try 
		{
			// initialize all resources and add them to the buffer pool manager.
			registerTableResources(false);
			// use table region 
			int resource = getRandomResource();
			CacheableData data = this.underTest.createNewPageAndPin(resource);
			if (data instanceof TablePage)
			{
				// insert a tuple to test if cacheable data object is correctly created
				TablePage tp = (TablePage) data;
				// insert a tuple
				DataTuple tuple = generateTuple(this.schemas.get(resource));
				// insert tuple into page
				tp.insertTuple(tuple);
			}
			else
			{
				fail("Every cacheable data object created in this test should be an instance of TablePage.");
			}
		} 
		catch (BufferPoolException e) 
		{
			e.printStackTrace();
			fail("An exception has been thrown.");
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
			fail("An exception has been thrown.");
		} 
		catch (DataFormatException e) 
		{
			e.printStackTrace();
			fail("An exception has been thrown.");
		} 
		catch (PageExpiredException e) 
		{
			e.printStackTrace();
			fail("An exception has been thrown.");
		} 
		catch (PageFormatException e) 
		{
			e.printStackTrace();
			fail("An exception has been thrown.");
		}
	}

	
	/**
	 * Tests the retrieval and pinning of tuples through the BufferPoolManager.
	 */
	@Test
	public void testGetPageAndPin()
	{
		try 
		{
			// initialize all resources and add them to the buffer pool manager.
			registerTableResources(false);
			// use table region 
			int resource = getRandomResource();
			CacheableData data = this.underTest.createNewPageAndPin(resource);
			int pinnedPageNumber = data.getPageNumber();
			if (!(data instanceof TablePage))
			{
				fail("Every cacheable data object created in this test should be an instance of TablePage.");
			}
			// insert a tuple to test if cacheable data object is correctly created
			TablePage tp = (TablePage) data;
			TableSchema schema = this.schemas.get(resource);
			DataTuple tuple = generateTuple(schema);
			// insert tuple into page
			tp.insertTuple(tuple);
			// flood cache with additional pages
			for ( int i = 0; i < this.config.getCacheSize(PageSize.SIZE_8192) * 2; i++)
			{
				// create page
				CacheableData insert = this.underTest.createNewPageAndPin(resource);
				DataTuple insertTuple = generateTuple(schema);
				// insert tuple into page
				((TablePage) insert).insertTuple(insertTuple);
				// unpin page
				this.underTest.unpinPage(resource, insert.getPageNumber());
			}
			MockResourceManager mrm = (MockResourceManager) this.resourceManagers.get(resource);
			LinkedList<Integer> oldReadRequests = mrm.getReadRequests();
			
			// check that originally pinned page is still contained
			CacheableData refetched = this.underTest.getPageAndPin(resource, data.getPageNumber());
			TablePage tpRefetched = (TablePage) refetched;
			try
			{
				DataTuple tupleRefetched = tpRefetched.getDataTuple(0, Long.MAX_VALUE, schema.getNumberOfColumns());
				LinkedList<Integer> newReadRequests = mrm.getReadRequests();
				this.underTest.unpinPage(resource, pinnedPageNumber); // counter should be 1
				assertTrue("The refetched tuple should be equal to the inserted tuple.", tuple.equals(tupleRefetched));
				assertTrue("No new read requests should have been issued to fetch the pinned element.", newReadRequests.size() == oldReadRequests.size());
			} 
			catch (PageTupleAccessException e) 
			{
				fail("Inserted tuple should be contained in table page.");
			}
			
			// flood cache with additional pages	
			for ( int i = 0; i < this.config.getCacheSize(PageSize.SIZE_8192) * 2; i++)
			{
				// create page
				CacheableData insert = this.underTest.createNewPageAndPin(resource);
				DataTuple insertTuple = generateTuple(schema);
				// insert tuple into page
				((TablePage) insert).insertTuple(insertTuple);
				// unpin page
				this.underTest.unpinPage(resource, insert.getPageNumber());
			}

			oldReadRequests = mrm.getReadRequests();
			
			// check that originally pinned page is still contained
			refetched = this.underTest.getPageAndPin(resource, data.getPageNumber());
			System.out.println(refetched.getPageNumber());
			tpRefetched = (TablePage) refetched;
			try 
			{
				DataTuple tupleRefetched = tpRefetched.getDataTuple(0, Long.MAX_VALUE, schema.getNumberOfColumns());
				LinkedList<Integer> newReadRequests = mrm.getReadRequests();
				this.underTest.unpinPage(resource, pinnedPageNumber); // counter should be 1
				this.underTest.unpinPage(resource, pinnedPageNumber); // counter should be 1
				assertTrue("The refetched tuple should be equal to the inserted tuple.", tuple.equals(tupleRefetched));
				assertTrue("No new read requests should have been issued to fetch the pinned element.", newReadRequests.size() == oldReadRequests.size());
			} 
			catch (PageTupleAccessException e) 
			{
				fail("Inserted tuple should be contained in table page.");
			}
			
			// wait 500 ms for writing threads to complete
			try 
			{
				Thread.sleep(500);
			} 
			catch (InterruptedException e) 
			{
				// do nothing
			}	
			LinkedList<Integer> writeRequests = mrm.getWriteRequests();
			assertTrue("All created and evicted pages should have been written to disk: " + writeRequests.size() + ": " + (3 * this.config.getCacheSize(PageSize.SIZE_8192) + 1), writeRequests.size() == 3 * this.config.getCacheSize(PageSize.SIZE_8192) + 1);			
		} 
		catch (BufferPoolException e) 
		{
			e.printStackTrace();
			fail("An exception has been thrown.");
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
			fail("An exception has been thrown.");
		} 
		catch (DataFormatException e) 
		{
			e.printStackTrace();
			fail("An exception has been thrown.");
		} 
		catch (PageExpiredException e) 
		{
			e.printStackTrace();
			fail("An exception has been thrown.");
		} 
		catch (PageFormatException e) 
		{
			e.printStackTrace();
			fail("An exception has been thrown.");
		}
	
	}

	/**
	 * Tests prefetching of pages.
	 */
	@Test
	public void testPrefetchPage()
	{
		// prefetch pages from the bpm and test that returning from it is faster than the 
		// wait time in the mock resource manager.
		try 
		{
			// initialize all resources and add them to the buffer pool manager.
			registerTableResources(true);
			// use table region 
			int resource = getRandomResource();
			CacheableData data = this.underTest.createNewPageAndPin(resource);
			int oldPageNumber = data.getPageNumber();
			this.underTest.unpinPage(resource, oldPageNumber);
			if (!(data instanceof TablePage))
			{
				fail("Every cacheable data object created in this test should be an instance of TablePage.");
			}
			// insert a tuple to test if cacheable data object is correctly created
			TablePage tp = (TablePage) data;
			TableSchema schema = this.schemas.get(resource);
			DataTuple tuple = generateTuple(schema);
			// insert tuple into page
			tp.insertTuple(tuple);
			// flood cache with additional pages and evict page	
			for ( int i = 0; i < this.config.getCacheSize(PageSize.SIZE_8192) * 2; i++)
			{
				// create page
				CacheableData insert = this.underTest.createNewPageAndPin(resource);
				DataTuple insertTuple = generateTuple(schema);
				// insert tuple into page
				((TablePage) insert).insertTuple(insertTuple);
				// unpin page
				this.underTest.unpinPage(resource, insert.getPageNumber());
			}
			// prefetch old page
			long startTime = System.currentTimeMillis();
			this.underTest.prefetchPage(resource, oldPageNumber);
			assertTrue("Prefetch should return immediately and not perform any IO operations.", System.currentTimeMillis() - startTime < 200);
			// wait a bit and see that the read requests was issued
			try 
			{
				Thread.sleep(1000);
			} 
			catch (InterruptedException e) 
			{
				// do nothing
			}
			MockResourceManager rmr = (MockResourceManager) this.resourceManagers.get(resource);
			List<Integer> readRequests = rmr.getReadRequests();
			assertTrue("A prefetched request should be requested by the BufferPoolManager after the method has returned.", readRequests.get(readRequests.size() - 1) == oldPageNumber);
		} 
		catch (BufferPoolException e1) 
		{
			e1.printStackTrace();
			fail("An exception has been thrown.");
		} 
		catch (IOException e1) 
		{
			e1.printStackTrace();
			fail("An exception has been thrown.");
		} 
		catch (PageFormatException e1) 
		{
			e1.printStackTrace();
			fail("An exception has been thrown.");
		} 
		catch (DataFormatException e) 
		{
			e.printStackTrace();
			fail("An exception has been thrown.");
		}
	}
	
	/**
	 * Tests whether the synchronization works without deadlocks.
	 */
	@Test
	public void testSynchronization()
	{
		// initialize all resources and add them to the buffer pool manager.
		try {
			registerTableResources(false);
			// generate flooding threads (issuing requests non-stop)
			Map<Integer, List<Integer>> pageNumbersForResource = new HashMap<Integer, List<Integer>>();		
	
			
			// generate pages for resources
			for(Integer resource : this.resourceManagers.keySet())
			{
				pageNumbersForResource.put(resource, generatePages(resource));
			}
			// 3 threads per resource
			int numberOfFloodingThreads = pageNumbersForResource.keySet().size() * 3;
			FloodingThread[] flooder = new FloodingThread[numberOfFloodingThreads];
			int a = 0;
			for (Integer resource : pageNumbersForResource.keySet())
			{
				flooder[a] = new FloodingThread(this, resource, pageNumbersForResource.get(resource));
				flooder[a].setName("Flooding - " + a);
				flooder[a].start();
				a++;
				flooder[a] = new FloodingThread(this, resource, pageNumbersForResource.get(resource));
				flooder[a].setName("Flooding - " + a);
				flooder[a].start();
				a++;
				flooder[a] = new FloodingThread(this, resource, pageNumbersForResource.get(resource));
				flooder[a].setName("Flooding - " + a);
				flooder[a].start();
				a++;
			}		
			long runtime = 120000;
			long startTime = System.currentTimeMillis();
			// number of threads per resource
			int numberOfThreads = 2;
			TestSerializationThread[] threads = new TestSerializationThread[numberOfThreads * pageNumbersForResource.keySet().size()];
			for (Integer resource : pageNumbersForResource.keySet())
			{
				for (int i = 0; i < numberOfThreads; i++)
				{
					threads[resource * numberOfThreads + i] = new TestSerializationThread(this, resource, pageNumbersForResource.get(resource));
					threads[resource * numberOfThreads + i].setName("Serialization - " + resource + " - " + i);
					threads[resource * numberOfThreads + i].start();
				}
			}
			boolean stillRunning;	
			do
			{
				try
				{
					Thread.sleep(runtime/100);
				} 
				catch (InterruptedException e)
				{
					// do nothing
				}
				stillRunning = false;
				// check status of threads
				for (int i = 0; i < threads.length && !stillRunning; i++)
				{
					if (threads[i].isAlive())
					{
						stillRunning = true;
					}
				}
			}
			while ((System.currentTimeMillis() < startTime + runtime) && stillRunning);
	
			// check status of threads
			stillRunning = false;
			for (int i = 0; i < threads.length && !stillRunning; i++)
			{
				if (threads[i].isAlive())
				{
					stillRunning = true;
				}
			}
			// stop flooding threads
			for (int i = 0; i < flooder.length; i++)
			{
				flooder[i].stopThread();
				flooder[i].interrupt();
				try {
					flooder[i].join();
				} catch (InterruptedException e) {
					// do nothing
				}
			}
			// check if any thread was still running (deadlock?!)
			if (stillRunning)
			{
				fail("Deadlock assumed cause the runtime largely exceeds the estimated time.");
			}
			else
			{			
				// check modifications
				// generate accumulated list
				HashMap<Integer, HashMap<Integer, Integer>> resourcePageNumberMapping = new HashMap<Integer, HashMap<Integer, Integer>>();
				for (Integer resource : pageNumbersForResource.keySet())
				{
					for (int i = 0; i < numberOfThreads; i++)
					{
						HashMap<Integer, Integer> modified = threads[resource * numberOfThreads + i].getModifiedList();
						if (resourcePageNumberMapping.get(resource) == null)
						{
							resourcePageNumberMapping.put(resource, modified);
						}
						else
						{
							// get already contained mapping for this resource
							HashMap<Integer, Integer> containedMapping = resourcePageNumberMapping.get(resource);
							for (Integer pNumber : modified.keySet())
							{
								if (containedMapping.get(pNumber) == null)
								{
									// set the page accesses for this page number
									containedMapping.put(pNumber, modified.get(pNumber));
								}
								else
								{
									// sum the accesses of the threads
									Integer accesses = containedMapping.get(pNumber);
									accesses += modified.get(pNumber);
									containedMapping.put(pNumber, accesses);
								}
							}
						}
					}
				}
				// check if modifications are correct
				for (Integer resource : resourcePageNumberMapping.keySet())
				{
					HashMap<Integer, Integer> pageModifications = resourcePageNumberMapping.get(resource);
					for (Integer pNumber : pageModifications.keySet())
					{
						int mods = pageModifications.get(pNumber);
						try 
						{
							CacheableData cd = this.underTest.getPageAndPin(resource, pNumber);
							assertTrue("Number of modifications does not match stored value in the buffer.",
									IntField.getFieldFromBinary(cd.getBuffer(), TestSerializationThread.fieldPos).getValue() == mods);
						} 
						catch (BufferPoolException e) 
						{
							e.printStackTrace();
							fail("Exception has been thrown.");
						} 
						catch (IOException e) 
						{
							e.printStackTrace();
							fail("Exception has been thrown.");
						}
					}
				}
			}
		} catch (BufferPoolException e1) {
			e1.printStackTrace();
			fail("An exception has been thrown.");
		} catch (IOException e1) {
			e1.printStackTrace();
			fail("An exception has been thrown.");
		} catch (PageFormatException e1) {
			e1.printStackTrace();
			fail("An exception has been thrown.");
		}			
	}

	/**
	 * Registers all tables of the test instance with the buffer pool manager.
	 * 
	 * @throws BufferPoolException
	 * @throws IOException
	 * @throws PageFormatException
	 */
	private void registerTableResources(boolean prefetch) 
		throws BufferPoolException, IOException, PageFormatException
	{
		for (int i = 0; i < 5; i++)
		{
			TableSchema schema = generateSchema(PageSize.SIZE_8192);
			MockResourceManager mrm = new MockResourceManager(schema, PageSize.SIZE_8192, prefetch);
			this.underTest.registerResource(i, mrm);
			this.resourceManagers.put(i, mrm);	
			this.schemas.put(i, schema);
		}
	}

	/**
	 * Closes all tables of the test instance and releases the locks.
	 * 
	 * @throws IOException
	 */
	private void closeTableResources() 
		throws IOException 
	{
		for(ResourceManager rm : this.resourceManagers.values())
		{
			rm.closeResource();
		}
	}
		
	/**
	 * Generates pages for the given resource and returns a list of the 
	 * contained page numbers.
	 * 
	 * @param resource The resource to generate pages for.
	 * @return A list of page numbers.
	 */
	protected List<Integer> generatePages(int resource) 
	{
		ArrayList<Integer> pageNumbers = new ArrayList<Integer>(this.config.getCacheSize(PageSize.SIZE_8192)*2);
		try
		{
			// insert a tuple to test if cacheable data object is correctly created
			TableSchema schema = this.schemas.get(resource);
			// flood cache with additional pages	
			for ( int i = 0; i < this.config.getCacheSize(PageSize.SIZE_8192) * 2; i++)
			{
				// create page
				CacheableData insert = this.underTest.createNewPageAndPin(resource);
				pageNumbers.add(insert.getPageNumber());
				DataTuple insertTuple = generateTuple(schema);
				// insert tuple into page
				((TablePage) insert).insertTuple(insertTuple);
				// store 0 at specified position for serialization test
				IntField.encodeIntAsBinary(0, insert.getBuffer(), TestSerializationThread.fieldPos);
				// unpin page
				this.underTest.unpinPage(resource, insert.getPageNumber());
			}
		} catch (PageExpiredException e) 
		{
			e.printStackTrace();
		} catch (PageFormatException e) 
		{
			e.printStackTrace();
		} catch (DataFormatException e) 
		{
			e.printStackTrace();
		} catch (BufferPoolException e) 
		{
			e.printStackTrace();
		} catch (IOException e) 
		{
			e.printStackTrace();
		}
		return pageNumbers;
	}	
	
	/**
	 * Generates a data field of the given type with a random value.
	 * 
	 * @param type The type of the data field.
	 * @return A data field with a random value.
	 * @throws DataFormatException Thrown, if the generation of a field fails.
	 */
	private static DataField generateRandomField(DataType type)
	throws DataFormatException
	{
		if (random.nextInt(20) == 0) {
			return type.getNullValue();
		}
		
		switch (type.getBasicType()) {
		case SMALL_INT:
			return new SmallIntField((short) random.nextInt());
		case INT:
			return new IntField(random.nextInt());
		case BIG_INT:
			return new BigIntField(random.nextLong());
		case FLOAT:
			return new FloatField(random.nextFloat());
		case DOUBLE:
			return new DoubleField(random.nextDouble());
		case CHAR:
			return new CharField(getRandomString(type.getLength()));
		case VAR_CHAR:
			return new VarcharField(getRandomString(type.getLength()).substring(0, random.nextInt(type.getLength())));
		case TIME:
			int h = random.nextInt(24);
			int m = random.nextInt(60);
			int s = random.nextInt(60);
			int o = random.nextInt(24*60*60*1000) - 12*60*60*1000;
			return new TimeField(h, m, s, o);
		case TIMESTAMP:
			int yy = random.nextInt(2999) + 1600;
			int tt = random.nextInt(12);
			int dd = random.nextInt(28) + 1;
			int hh = random.nextInt(24);
			int mm = random.nextInt(60);
			int ss = random.nextInt(60);
			int ll = random.nextInt(1000);
			return new TimestampField(dd, tt, yy, hh, mm, ss, ll);
		case DATE:
			int y = random.nextInt(9999) + 1;
			int t = random.nextInt(12);
			int d = random.nextInt(28) + 1;
			return new DateField(d, t, y);
		default:
			return new IntField(1);
		}
	}	

	/**
	 * Creates a random string of the given length.
	 * 
	 * @param len The length of the string.
	 * @return A random string of the given length.
	 */
	private static String getRandomString(int len)
	{
		StringBuilder buffer = new StringBuilder(len);
		
		for (int i = 0; i < len; i++) {
			int ch = random.nextInt(255) + 1;
			buffer.append((char) ch);
		}
		return buffer.toString();
	}	

	/**
	 * Initializes a schema for the given page size.
	 * 
	 * @param pageSize The page size.
	 * @return The schema.
	 */
	private TableSchema generateSchema(PageSize pageSize)
	{
		TableSchema schema = new TableSchema(pageSize);
		// generate a random set of columns
		int numCols = random.nextInt(20) + 1;
		// create the columns as given
		for (int col = 0; col < numCols; col++) 
		{
			DataType type = getRandomDataType();
			schema.addColumn(ColumnSchema.createColumnSchema("Random Column " + col, type, true));
		}
		return schema;
	}	
	
	/**
	 * Generates a random data type for the schema.
	 * 
	 * @return A random data type.
	 */
	private DataType getRandomDataType()
	{
		int num = random.nextInt(10);
		
		switch (num) {
		case 0:
			return DataType.smallIntType();
		case 1:
			return DataType.intType();
		case 2:
			return DataType.bigIntType();
		case 3:
			return DataType.floatType();
		case 4:
			return DataType.doubleType();
		case 5:
			return DataType.charType(random.nextInt(256) + 1);
		case 6:
			return DataType.varcharType(random.nextInt(256) + 1);
		case 7:
			return DataType.dateType();
		case 8:
			return DataType.timeType();
		case 9:
			return DataType.timestampType();
		default:
			return DataType.intType();	
		}
	}	

	/**
	 * Generates a tuple for the given schema.
	 * 
	 * @param schema The schema of the table.
	 * @return The generated data tuple.
	 * @throws DataFormatException
	 */
	protected DataTuple generateTuple(TableSchema schema)
			throws DataFormatException 
	{
		DataTuple tuple = new DataTuple(schema.getNumberOfColumns());	
		// fill with random data
		for (int col = 0; col < schema.getNumberOfColumns(); col++) 
		{
			ColumnSchema cs = schema.getColumn(col);
			tuple.assignDataField(generateRandomField(cs.getDataType()), col);
		}
		return tuple;
	}
	
	/**
	 * Returns the id of randomly chosen resource.
	 * 
	 * @return Random id of resource manager.
	 */
	protected int getRandomResource() 
	{
		int pos = random.nextInt(this.resourceManagers.size());
		Iterator<Integer> iter = this.resourceManagers.keySet().iterator();
		// due to size request that many elements must exist
		int i = 0;
		while(i < pos)
		{
			iter.next();
			i++;
		}
		// return n th element
		return iter.next();
		
	}	
}
