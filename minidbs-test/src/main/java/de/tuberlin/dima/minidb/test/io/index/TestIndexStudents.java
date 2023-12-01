package de.tuberlin.dima.minidb.test.io.index;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
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
import de.tuberlin.dima.minidb.catalogue.IndexSchema;
import de.tuberlin.dima.minidb.catalogue.TableSchema;
import de.tuberlin.dima.minidb.core.BigIntField;
import de.tuberlin.dima.minidb.core.CharField;
import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataFormatException;
import de.tuberlin.dima.minidb.core.DataType;
import de.tuberlin.dima.minidb.core.DateField;
import de.tuberlin.dima.minidb.core.DoubleField;
import de.tuberlin.dima.minidb.core.FloatField;
import de.tuberlin.dima.minidb.core.IntField;
import de.tuberlin.dima.minidb.core.RID;
import de.tuberlin.dima.minidb.core.SmallIntField;
import de.tuberlin.dima.minidb.core.TimeField;
import de.tuberlin.dima.minidb.core.TimestampField;
import de.tuberlin.dima.minidb.io.cache.PageFormatException;
import de.tuberlin.dima.minidb.io.cache.PageSize;
import de.tuberlin.dima.minidb.io.index.BTreeIndex;
import de.tuberlin.dima.minidb.io.index.BTreeIndexPage;
import de.tuberlin.dima.minidb.io.index.BTreeInnerNodePage;
import de.tuberlin.dima.minidb.io.index.BTreeLeafPage;
import de.tuberlin.dima.minidb.io.index.IndexFormatCorruptException;
import de.tuberlin.dima.minidb.io.index.IndexResourceManager;
import de.tuberlin.dima.minidb.io.index.IndexResultIterator;
import de.tuberlin.dima.minidb.io.manager.BufferPoolException;
import de.tuberlin.dima.minidb.io.manager.BufferPoolManager;
import de.tuberlin.dima.minidb.util.Pair;

/**
 * The public test case for B+ tree index operations.
 * 
 * @author Stephan Ewen (sewen@cs.tu-berlin.de)
 * @author Michael Saecker
 */
public class TestIndexStudents 
{
	/**
	 * Number of values to insert/delete.
	 */
	private static final int numValuesToInsert = 200000;

	/**
	 * Resource id of the index under test.
	 */
	private static final int INDEX_RESOURCE_ID = 5;

	/**
	 * Number of schemas to test per page size.
	 */
	private static final int NUM_SCHEMAS = 6;

	/**
	 * Fixed seed to make tests reproducible. Teacher test uses a different seed so please try your solution with another seed to check for remaining errors.
	 */
	private static final long SEED = 985843783L;

	/**
	 * Index schemas generated for the tests.
	 */
	private static final IndexSchema[] indexSchemas = new IndexSchema[NUM_SCHEMAS];
	
	/**
	 * The random number generator used to create elements.
	 */
	private static Random random = new Random(SEED);

	/**
	 * File where the index data is stored.
	 */
	private final File indexFileInsert = new File(System.getProperty("java.io.tmpdir") + File.separatorChar + "minidbstestindex.mdix");

	/**
	 * Files where the index data for retrieval tests is stored.
	 */
	private final File[] indexFileGet = new File[]{ 
			new File(this.getClass().getResource("/index/minidbstestindex0.mdix").getPath()),
			new File(this.getClass().getResource("/index/minidbstestindex1.mdix").getPath()),
			new File(this.getClass().getResource("/index/minidbstestindex2.mdix").getPath()),
			new File(this.getClass().getResource("/index/minidbstestindex3.mdix").getPath()),
			new File(this.getClass().getResource("/index/minidbstestindex4.mdix").getPath()),
			new File(this.getClass().getResource("/index/minidbstestindex5.mdix").getPath()),
	};
	
	/**
	 * Root pages of the different index schemas.
	 */
	private final static int[] indexRootPages = new int[]{
		3, 343, 258, 3, 343, 3
	};

	/**
	 * The table schema used for all index operations.
	 */
	private static TableSchema tableSchema = null;

	/**
	 * Location of the configuration file for the database instance.
	 */
	protected static final String configFileName = "/config.xml";

	/**
	 * The configuration of the database instance.
	 */
	protected Config config = null;

	/**
	 * The logger passed to the buffer pool manager.
	 */
	protected Logger logger = null;
	
	/**
	 * Static initialization to make certain that the same schemas are generated for each test run.
	 */
	static 
	{
		System.out.println(new File(TestIndexStudents.class.getClass().getResource("/index/minidbstestindex0.mdix").getPath().replace("0.mdix", ".mdix")));
		initSchema(PageSize.SIZE_4096);
		// test all different schemas
		for (int si = 0; si < NUM_SCHEMAS; si++) 
		{
			IndexSchema ixSchema = new IndexSchema(tableSchema, si, tableSchema.getPageSize(), false, indexRootPages[si], 1);
			indexSchemas[si] = ixSchema;					
		}
	}
	
	/**
	 * Sets up the environment for each test.
	 */
	@Before
	public void setUp() 
		throws Exception 
	{
		AbstractExtensionFactory.initializeDefault();
		
		InputStream is = this.getClass().getResourceAsStream(configFileName);
    	this.config = Config.loadConfig(is);
		this.logger = Logger.getLogger("BPM - Logger");
	}

	/**
	 * Cleans up after each test run.
	 */
	@After
	public void tearDown() 
		throws Exception 
	{
		// make sure the index is cleaned up
		try 
		{
			IndexResourceManager.deleteIndex(this.indexFileInsert);
		} 
		catch (IOException ioex) 
		{ /* do nothing */ }
	}
	
	/**
	 * Tests retrieval of index schema.
	 */
	@Test
	public void testSchema()
	{
		for(int i = 0; i < NUM_SCHEMAS; i++)
		{
			try
			{
				IndexSchema schema = indexSchemas[i];
				DataType type = schema.getIndexedColumnSchema().getDataType();
				
				System.out.println("Creating Index for data-type " + type);
				
				// now create the new index
				IndexResourceManager resManager = IndexResourceManager.createIndex(this.indexFileInsert, schema);
				
				// create a buffer pool and register the index
				BufferPoolManager bufferPool = AbstractExtensionFactory.getExtensionFactory().createBufferPoolManager(this.config, this.logger);
				bufferPool.startIOThreads();
				bufferPool.registerResource(INDEX_RESOURCE_ID, resManager);
	
				BTreeIndex index = AbstractExtensionFactory.getExtensionFactory().createBTreeIndex(schema, bufferPool, INDEX_RESOURCE_ID);
				assertTrue("The stored schema is not equal to the schema handed to the index.", index.getIndexSchema() == indexSchemas[i]);
				// close resources
				bufferPool.closeBufferPool();
				bufferPool = null;
				resManager.closeResource();			}
			catch (Exception e) {
				e.printStackTrace();
				fail("An exception has been thrown.");
			}
		}
	}
	
	
	/**
	 * Tests the insertion of entries.
	 */
	@Test
	public void testInsert()
	{
		for(int i = 0; i < NUM_SCHEMAS; i++)
		{
			IndexResourceManager resManager = null;
			BufferPoolManager bufferPool = null;
			try
			{
				IndexSchema schema = indexSchemas[i];
				DataType type = schema.getIndexedColumnSchema().getDataType();
			
				System.out.println("Creating Index for data-type " + type);
				
				// now create the new index
				resManager = IndexResourceManager.createIndex(this.indexFileInsert, schema);
				
				// create a buffer pool and register the index
				bufferPool = AbstractExtensionFactory.getExtensionFactory().createBufferPoolManager(this.config, this.logger);
				bufferPool.startIOThreads();
				bufferPool.registerResource(INDEX_RESOURCE_ID, resManager);

				BTreeIndex index = AbstractExtensionFactory.getExtensionFactory().createBTreeIndex(schema, bufferPool, INDEX_RESOURCE_ID);
				
				// this map is for comparison. with really big values, it might run out of memory
				Map<DataField, List<RID>> comparisonMap = new HashMap<DataField, List<RID>>();
				
				System.out.println("Inserting values...");
				
				for (int j = 0; j < numValuesToInsert; j++) 
				{
					// generate a key/rid pair
					DataField key = generateRandomField(type);
					RID rid = new RID(random.nextInt(Integer.MAX_VALUE), random.nextInt(Integer.MAX_VALUE));
					
					// insert it into the comparison map
					List<RID> compList = comparisonMap.get(key);
					if (compList == null) 
					{
						compList = new ArrayList<RID>();
						comparisonMap.put(key, compList);
					}
					compList.add(rid);
					// insert it into the index
					index.insertEntry(key, rid);
					
					// check consistency regularly
					if (j % (numValuesToInsert/20) == 0)
					{
						checkConsistency(index, bufferPool);
					}
				}
				
				System.out.println("Checking values...");

				// check consistency before writing index to disk
				List<Pair<DataField, RID>> contained = checkConsistency(index, bufferPool);
				// create map for comparison
				Map<DataField, List<RID>> containedMap = new HashMap<DataField, List<RID>>();
				for(Pair<DataField, RID> pair : contained)
				{
					DataField key = pair.getFirst();
					RID rid = pair.getSecond();
					// insert it into the comparison map
					List<RID> compList = containedMap.get(key);
					if (compList == null) 
					{
						compList = new ArrayList<RID>();
						containedMap.put(key, compList);
					}
					compList.add(rid);
				}				
				
				// check that all values are contained using iterators
				Iterator<Map.Entry<DataField, List<RID>>> iter = comparisonMap.entrySet().iterator();
				while (iter.hasNext()) 
				{
					Map.Entry<DataField, List<RID>> entry = iter.next();
					List<RID> allRids = containedMap.get(entry.getKey());
					assertTrue("Not all values that should be contained in the index are contained.", allRids.containsAll(entry.getValue())); 
					assertTrue("The index returned values that should not have been returned.", entry.getValue().containsAll(allRids));
				}
				
				System.out.println("Closing Buffer, forcing all pages to disk...");
				
				// close the buffer pool, forcing all resources of the index to be written
				bufferPool.closeBufferPool();
				bufferPool = null;
				resManager.closeResource();

				System.out.println("Recreating buffer...");
				
				// reopen the index
				bufferPool = AbstractExtensionFactory.getExtensionFactory().createBufferPoolManager(this.config, this.logger);
				bufferPool.startIOThreads();
				resManager = IndexResourceManager.openIndex(this.indexFileInsert, schema.getIndexTableSchema());
				schema = resManager.getSchema();
				bufferPool.registerResource(INDEX_RESOURCE_ID, resManager);
				index = AbstractExtensionFactory.getExtensionFactory().createBTreeIndex(schema, bufferPool, INDEX_RESOURCE_ID);
	
				System.out.println("Checking values...");

				// check consistency after reading index from disk
				contained = checkConsistency(index, bufferPool);
				containedMap = new HashMap<DataField, List<RID>>();
				for(Pair<DataField, RID> pair : contained)
				{
					DataField key = pair.getFirst();
					RID rid = pair.getSecond();
					// insert it into the comparison map
					List<RID> compList = containedMap.get(key);
					if (compList == null) 
					{
						compList = new ArrayList<RID>();
						containedMap.put(key, compList);
					}
					compList.add(rid);
				}				
				
				// check that all values are contained using iterators
				iter = comparisonMap.entrySet().iterator();
				while (iter.hasNext()) 
				{
					Map.Entry<DataField, List<RID>> entry = iter.next();
					List<RID> allRids = containedMap.get(entry.getKey());
					assertTrue("Not all values that should be contained in the index are contained.", allRids.containsAll(entry.getValue())); 
					assertTrue("The index returned values that should not have been returned.", entry.getValue().containsAll(allRids));
				}
				
				// close resources
				bufferPool.closeBufferPool();
				bufferPool = null;
				resManager.closeResource();
			}
			catch (BufferPoolException e)
			{
				e.printStackTrace();
				fail("An exception has been thrown.");
			} 
			catch (IndexFormatCorruptException e) 
			{
				e.printStackTrace();
				fail("An exception has been thrown.");
			} 
			catch (IOException e) 
			{
				e.printStackTrace();
				fail("An exception has been thrown.");
			} 
			catch (PageFormatException e) 
			{
				e.printStackTrace();
				fail("An exception has been thrown.");
			} 
			catch (DataFormatException e) 
			{
				e.printStackTrace();
				fail("An exception has been thrown.");
			}
			finally
			{
				if(bufferPool != null)
				{
					bufferPool.closeBufferPool();
				}
				if(resManager != null)
				{
					try {
						resManager.closeResource();
					} catch (IOException e) {
						// do nothing
					}
				}
			}
		}
	}
	
	
	/**
	 * Tests the retrieval of entries from the index.
	 */
	@Test
	public void testRIDIterator()
	{
		for(int i = 0; i < NUM_SCHEMAS; i++)
		{
			IndexResourceManager resManager = null;
			BufferPoolManager bufferPool = null;
			try
			{
				
				// now create the new index
				resManager = IndexResourceManager.openIndex(this.indexFileGet[i], tableSchema);

				IndexSchema schema = resManager.getSchema();
				DataType type = schema.getIndexedColumnSchema().getDataType();

				// make sure the index file is not there				
				System.out.println("Reading Index for data-type " + type);

				// create a buffer pool and register the index
				bufferPool = AbstractExtensionFactory.getExtensionFactory().createBufferPoolManager(this.config, this.logger);
				bufferPool.startIOThreads();
				bufferPool.registerResource(INDEX_RESOURCE_ID, resManager);
	
				BTreeIndex index = AbstractExtensionFactory.getExtensionFactory().createBTreeIndex(schema, bufferPool, INDEX_RESOURCE_ID);
				
				// check consistency and fetch contained entries
				List<Pair<DataField, RID>> contained = checkConsistency(index, bufferPool);
				// create map for comparison
				Map<DataField, List<RID>> comparisonMap = new HashMap<DataField, List<RID>>();
				for(Pair<DataField, RID> pair : contained)
				{
					DataField key = pair.getFirst();
					RID rid = pair.getSecond();
					// insert it into the comparison map
					List<RID> compList = comparisonMap.get(key);
					if (compList == null) 
					{
						compList = new ArrayList<RID>();
						comparisonMap.put(key, compList);
					}
					compList.add(rid);
				}				

				System.out.println("Checking values...");
				
				// check that all values are contained using iterators
				Iterator<Map.Entry<DataField, List<RID>>> iter = comparisonMap.entrySet().iterator();
				while (iter.hasNext()) 
				{
					Map.Entry<DataField, List<RID>> entry = iter.next();
					IndexResultIterator<RID> ridIter = index.lookupRids(entry.getKey()); 
					List<RID> allRids = new ArrayList<RID>(entry.getValue().size() + 1);
					while(ridIter.hasNext())
					{
						allRids.add(ridIter.next());
					}
					assertTrue("Not all values that should be contained in the index are contained.", allRids.containsAll(entry.getValue())); 
					assertTrue("The index returned values that should not have been returned.", entry.getValue().containsAll(allRids));
				}
				
				System.out.println("Closing Buffer, forcing all pages to disk...");
				
				// close the buffer pool, forcing all resources of the index to be written
				bufferPool.closeBufferPool();
				bufferPool = null;
				resManager.closeResource();

			}
			catch (BufferPoolException e)
			{
				e.printStackTrace();
				fail("An exception has been thrown.");
			} 
			catch (IndexFormatCorruptException e) 
			{
				e.printStackTrace();
				fail("An exception has been thrown.");
			} 
			catch (IOException e) 
			{
				e.printStackTrace();
				fail("An exception has been thrown.");
			} 
			catch (PageFormatException e) 
			{
				e.printStackTrace();
				fail("An exception has been thrown.");
			}
			finally
			{
				if(bufferPool != null)
				{
					bufferPool.closeBufferPool();
				}
				if(resManager != null)
				{
					try {
						resManager.closeResource();
					} catch (IOException e) {
						// do nothing
					}
				}
			}
		}		
	}
	

	/**
	 * Tests the retrieval of a rid range from the index.
	 */
	@Test
	public void testRangeRIDIterator()
	{
		// look up rids range
		for(int i = 0; i < NUM_SCHEMAS; i++)
		{
			IndexResourceManager resManager = null;
			BufferPoolManager bufferPool = null;
			try
			{
				// now create the new index
				resManager = IndexResourceManager.openIndex(this.indexFileGet[i], tableSchema);
				
				IndexSchema schema = resManager.getSchema();
				DataType type = schema.getIndexedColumnSchema().getDataType();

				// make sure the index file is not there
				System.out.println("Creating Index for data-type " + type);
				
				// create a buffer pool and register the index
				bufferPool = AbstractExtensionFactory.getExtensionFactory().createBufferPoolManager(this.config, this.logger);
				bufferPool.startIOThreads();
				bufferPool.registerResource(INDEX_RESOURCE_ID, resManager);
	
				BTreeIndex index = AbstractExtensionFactory.getExtensionFactory().createBTreeIndex(schema, bufferPool, INDEX_RESOURCE_ID);

				// check consistency and fetch contained entries
				List<Pair<DataField, RID>> contained = checkConsistency(index, bufferPool);
				// create map for comparison
				Map<DataField, List<RID>> comparisonMap = new HashMap<DataField, List<RID>>();
				for(Pair<DataField, RID> pair : contained)
				{
					DataField key = pair.getFirst();
					RID rid = pair.getSecond();
					// insert it into the comparison map
					List<RID> compList = comparisonMap.get(key);
					if (compList == null) 
					{
						compList = new ArrayList<RID>();
						comparisonMap.put(key, compList);
					}
					compList.add(rid);
				}				

				System.out.println("Sorting keys in external list...");				
				
				// move values into sorted list
				ArrayList<DataField> keys = new ArrayList<DataField>(numValuesToInsert);
				for(DataField key : comparisonMap.keySet())
				{
					keys.add(key);
				}
				
				System.out.println("Checking different key ranges...");
				
				// sort keys
				Collections.sort(keys);
				// request ranges
				for(int x = 0; x < keys.size(); x += random.nextInt(500))
				{
					
					// request range (at least 1 element
					int range = random.nextInt(keys.size() - x - 2 > 51 ? 50 : keys.size() - x) + 1;
					boolean startKeyIncluded = random.nextBoolean();
					boolean stopKeyIncluded = random.nextBoolean();
					DataField startKey = keys.get(x);
					DataField stopKey = keys.get(x + range);
					
					// retrieve index results
					IndexResultIterator<RID> ridIter = index.lookupRids(startKey, stopKey, startKeyIncluded, stopKeyIncluded);					
					List<RID> allRids = new ArrayList<RID>();
					while(ridIter.hasNext())
					{
						allRids.add(ridIter.next());
					}
					
					// build list for comparison
					ArrayList<RID> compList = new ArrayList<RID>(allRids.size());
					int compSize = 0;
					int start = startKeyIncluded ? x : x + 1;
					int end = stopKeyIncluded ? x + range : x + range - 1;
					if(start <= end) // list is not empty?
					{
						int pos = start;
						while(pos <= end)
						{
							compList.addAll(comparisonMap.get(keys.get(pos)));
							compSize += comparisonMap.get(keys.get(pos)).size();
							pos++;
						}
					}					
					// compare results
					assertTrue("Not all values that should be contained in the index are contained.", allRids.containsAll(compList)); 
					assertTrue("The index returned values that should not have been returned.", compList.containsAll(allRids));
					assertTrue("Size of the results does not match the size of the reference list.", allRids.size() == compSize);
				}
				
				// close resources
				bufferPool.closeBufferPool();
				bufferPool = null;
				resManager.closeResource();			
			}
			catch (Exception e) {
				e.printStackTrace();
				fail("An exception has been thrown.");
			}
			finally
			{
				if(bufferPool != null)
				{
					bufferPool.closeBufferPool();
				}
				if(resManager != null)
				{
					try {
						resManager.closeResource();
					} catch (IOException e) {
						// do nothing
					}
				}
			}
		}
	}
	
	
	/**
	 * Tests the retrieval of keys from the index.
	 */
	@Test
	public void testKeyRangeIterator()
	{
		// look up keys
		for(int i = 0; i < NUM_SCHEMAS; i++)
		{
			IndexResourceManager resManager = null;
			BufferPoolManager bufferPool = null;
			try
			{
				// now create the new index
				resManager = IndexResourceManager.openIndex(this.indexFileGet[i], tableSchema);
				
				IndexSchema schema = resManager.getSchema();
				DataType type = schema.getIndexedColumnSchema().getDataType();
				
				System.out.println("Creating Index for data-type " + type);
				
				// create a buffer pool and register the index
				bufferPool = AbstractExtensionFactory.getExtensionFactory().createBufferPoolManager(this.config, this.logger);
				bufferPool.startIOThreads();
				bufferPool.registerResource(INDEX_RESOURCE_ID, resManager);
	
				BTreeIndex index = AbstractExtensionFactory.getExtensionFactory().createBTreeIndex(schema, bufferPool, INDEX_RESOURCE_ID);

				// check consistency and fetch contained entries
				List<Pair<DataField, RID>> contained = checkConsistency(index, bufferPool);
				// create map for comparison
				Map<DataField, List<RID>> comparisonMap = new HashMap<DataField, List<RID>>();
				for(Pair<DataField, RID> pair : contained)
				{
					DataField key = pair.getFirst();
					RID rid = pair.getSecond();
					// insert it into the comparison map
					List<RID> compList = comparisonMap.get(key);
					if (compList == null) 
					{
						compList = new ArrayList<RID>();
						comparisonMap.put(key, compList);
					}
					compList.add(rid);
				}
				
				System.out.println("Sorting keys in external list...");				
				
				// move values into sorted list
				ArrayList<DataField> keys = new ArrayList<DataField>(numValuesToInsert);
				for(DataField key : comparisonMap.keySet())
				{
					keys.add(key);
				}
				
				System.out.println("Checking different key ranges...");
				
				// sort keys
				Collections.sort(keys);
				// request ranges
				for(int x = 0; x < keys.size(); x += random.nextInt(500))
				{
					
					// request range (at least 1 element
					int range = random.nextInt(keys.size() - x - 2 > 51 ? 50 : keys.size() - x -2) + 1;
					boolean startKeyIncluded = random.nextBoolean();
					boolean stopKeyIncluded = random.nextBoolean();
					DataField startKey = keys.get(x);
					DataField stopKey = keys.get(x + range);
					
					// retrieve index results
					IndexResultIterator<DataField> ridIter = index.lookupKeys(startKey, stopKey, startKeyIncluded, stopKeyIncluded);					
					List<DataField> allRids = new ArrayList<DataField>();
					while(ridIter.hasNext())
					{
						allRids.add(ridIter.next());
					}
					
					// build list for comparison
					ArrayList<DataField> compList = new ArrayList<DataField>(allRids.size());
					int compSize = 0;
					int start = startKeyIncluded ? x : x + 1;
					int end = stopKeyIncluded ? x + range : x + range - 1;
					if(start <= end) // list is not empty?
					{
						int pos = start;
						while(pos <= end)
						{
							compList.add(keys.get(pos));
							compSize += comparisonMap.get(keys.get(pos)).size();
							pos++;
						}
					}					
					// compare results
					assertTrue("Not all values that should be contained in the index are contained.", allRids.containsAll(compList)); 
					assertTrue("The index returned values that should not have been returned.", compList.containsAll(allRids));
					assertTrue("Size of the results does not match the size of the reference list.", allRids.size() == compSize);
				}
				
				// close resources
				bufferPool.closeBufferPool();
				bufferPool = null;
				resManager.closeResource();			
			}
			catch (Exception e) {
				e.printStackTrace();
				fail("An exception has been thrown.");
			}
			finally
			{
				if(bufferPool != null)
				{
					bufferPool.closeBufferPool();
				}
				if(resManager != null)
				{
					try {
						resManager.closeResource();
					} catch (IOException e) {
						// do nothing
					}
				}
			}
		}
	}
	

	/**
	 * Checks the whole B+ index tree for consistency.
	 *  
	 * @param index The index to check.
	 * @param ioAccess The buffer pool manager to interact with.
	 * @throws Exception Thrown, if any consistency constraint is violated.
	 */
	public static List<Pair<DataField, RID>> checkConsistency(BTreeIndex index,
			BufferPoolManager ioAccess)
	{
		IndexSchema schema = index.getIndexSchema();
		// get minimum number of elements for leafs and inner nodes
		int minInner = schema.getFanOut() >> 1;
		int minLeaf = schema.getMaximalLeafEntries() >> 1;

		LinkedList<Integer> list = new LinkedList<Integer>();
		LinkedList<Integer> innerNodeList = new LinkedList<Integer>();
		LinkedList<Integer> leafNodeList = new LinkedList<Integer>();
		List<Pair<DataField, RID>> retList = new ArrayList<Pair<DataField, RID>>();

		// traverse whole reachable tree to collect all inner nodes and leafs
		BTreeIndexPage node = getNode(schema
				.getRootPageNumber(), ioAccess);

		if (node instanceof BTreeInnerNodePage) 
		{
			list.addLast(node.getPageNumber());
			BTreeInnerNodePage innerNode = (BTreeInnerNodePage) node;

			// check if keys of root are in right order
			for (int i = 0; i < innerNode.getNumberOfKeys() - 1; i++) 
			{
				if (innerNode.getKey(i).compareTo(innerNode.getKey(i + 1)) > 0)
				{
					fail("Order of keys in root node (inner) is wrong");
				}
			}
		} 
		else 
		{
			BTreeLeafPage leafNode = (BTreeLeafPage) node;
			// check consistency of keys
			for (int i = 0; i < leafNode.getNumberOfEntries() - 1; i++) 
			{
				if (leafNode.getKey(i).compareTo(leafNode.getKey(i + 1)) > 0)
				{
					fail("Order of keys in root node (leaf) is wrong");
				}
				// add key/rid pair to return list
				retList.add(new Pair<DataField, RID>(leafNode.getKey(i), leafNode.getRidAtPosition(i)));				
			}
			// add last skipped element to return list
			retList.add(new Pair<DataField, RID>(leafNode.getKey(leafNode.getNumberOfEntries() - 1), leafNode.getRidAtPosition(leafNode.getNumberOfEntries() - 1)));
		}
		// unpin node
		unpinNode(node.getPageNumber(), ioAccess);
		
		while (!list.isEmpty()) 
		{
			int currentPageID = list.pop();
			BTreeInnerNodePage innerNode = (BTreeInnerNodePage) getNode(
					currentPageID, ioAccess);

			for (int i = 0; i <= innerNode.getNumberOfKeys(); i++) 
			{
				// reloead expired page
				if (innerNode.isExpired())
				{
					innerNode = (BTreeInnerNodePage) getNode(currentPageID,
							ioAccess);
				}
				
				BTreeIndexPage testNode = getNode(innerNode.getPointer(i),
						ioAccess);

				if (testNode.getPageNumber() < 1)
				{
					fail("Invalid page id: " + testNode.getPageNumber());
				}
				
				if (testNode instanceof BTreeInnerNodePage) 
				{
					// check for double page ids
					if (innerNodeList.contains(testNode.getPageNumber()))
						fail("InnerNode page Id double:"
								+ testNode.getPageNumber());
					// add inner node to list
					innerNodeList.addLast(testNode.getPageNumber());
					list.addLast(testNode.getPageNumber());
					
					// check elements in child
					if(i < innerNode.getNumberOfKeys() && ((BTreeInnerNodePage) testNode).getLastKey().compareTo(innerNode.getKey(i)) > 0)
					{
						fail("Child at position " + i + " contains a higher key than the parent node.");
					}
				} 
				else if (testNode instanceof BTreeLeafPage) 
				{
					// check for double page ids
					if (leafNodeList.contains(testNode.getPageNumber()))
					{
						fail("LeafNode page Id double:"
								+ testNode.getPageNumber());
					}
					// add leaf to list
					leafNodeList.addLast(testNode.getPageNumber());
					
					if(i < innerNode.getNumberOfKeys() && ((BTreeLeafPage) testNode).getLastKey().compareTo(innerNode.getKey(i)) > 0)
					{
						fail("Child at position " + i + " contains a higher key than the parent node.");
					}
				} 
				else 
				{
					fail("Page is neither an inner node nor a leaf node.");
				}
				unpinNode(testNode.getPageNumber(), ioAccess);
			}
			// check last pointer
			BTreeIndexPage lastNode = getNode(innerNode.getPointer(innerNode.getNumberOfKeys()), ioAccess);
			if(lastNode instanceof BTreeInnerNodePage)
			{
				if(innerNode.getLastKey().compareTo(((BTreeInnerNodePage) lastNode).getFirstKey()) > 0)
				{
					fail("The child at the last pointer of the inner node contains a smaller key.");
				}
			}
			else
			{
				if(innerNode.getLastKey().compareTo(((BTreeLeafPage) lastNode).getFirstKey()) > 0)
				{
					fail("The child at the last pointer of the inner node contains a smaller key.");
				}
			}
			// remove pinning
			unpinNode(lastNode.getPageNumber(), ioAccess);
			unpinNode(innerNode.getPageNumber(), ioAccess);

		}

		// check list of inner nodes excluding root for minimum filling
		for (int innerNodeID : innerNodeList) 
		{
			BTreeInnerNodePage innerNode = (BTreeInnerNodePage) getNode(
					innerNodeID, ioAccess);

			if (innerNode.getNumberOfKeys() < minInner)
			{
				fail("Consistency failed: Minimum number of elements in innerNode was undercut: "
								+ innerNode.getNumberOfKeys() + "/"
								+ schema.getFanOut());
			}
			for (int i = 0; i < innerNode.getNumberOfKeys() - 1; i++) 
			{
				if (innerNode.getKey(i).compareTo(innerNode.getKey(i + 1)) > 0)
				{
					fail("Order of keys in inner node is wrong");
				}
			}
			// remove pinning
			unpinNode(innerNode.getPageNumber(), ioAccess);
		}

		// copy list of leafNodes to check against missing page ids in tree
		// which can be traversed using the next page number id of the leafs
		List<Integer> allLeafs = new LinkedList<Integer>(leafNodeList);

		// check for leafs if no dual page ids occur and if the continues flag
		// is always set right
		while (!leafNodeList.isEmpty()) 
		{
			BTreeLeafPage leafNode = (BTreeLeafPage) getNode(leafNodeList
					.remove(0), ioAccess);

			// check filling grade
			if (leafNode.getNumberOfEntries() < minLeaf)
			{
				fail("Consistency failed: Minimum number of elements in leafNode was undercut: "
								+ leafNode.getNumberOfEntries() + "/"
								+ schema.getMaximalLeafEntries());
			}
			// check consistency of keys
			for (int i = 0; i < leafNode.getNumberOfEntries() - 1; i++) 
			{
				if (leafNode.getKey(i).compareTo(leafNode.getKey(i + 1)) > 0)
				{
					fail("Order of keys in leaf node is wrong");
				}
				// add key/rid pair to return list
				retList.add(new Pair<DataField, RID>(leafNode.getKey(i), leafNode.getRidAtPosition(i)));
				
			}
			// add last skipped element to return list
			retList.add(new Pair<DataField, RID>(leafNode.getKey(leafNode.getNumberOfEntries() - 1), leafNode.getRidAtPosition(leafNode.getNumberOfEntries() - 1)));
			
			// check consistency with next leaf
			int nextLeafID = leafNode.getNextLeafPageNumber();
			if (nextLeafID != -1) 
			{
				if (!allLeafs.contains(nextLeafID))
				{
					fail("Unreachable leaf detected.");
				}
				if (nextLeafID != leafNodeList.get(0))
				{
					fail("NextLeafID is not the next leaf in the list.");
				}
				BTreeLeafPage nextLeaf = (BTreeLeafPage) getNode(nextLeafID,
						ioAccess);

				if (leafNode.getLastKey().compareTo(nextLeaf.getFirstKey()) > 0)
				{
					fail("First key of next page is not greater than last key on current page.");
				}
				else if (leafNode.getLastKey()
						.compareTo(nextLeaf.getFirstKey()) == 0
						&& !leafNode.isLastKeyContinuingOnNextPage())
				{
					fail("Last key is continuing on next page but flag is not set.");
				}
				// remove pinning
				unpinNode(nextLeaf.getPageNumber(), ioAccess);
			} else if (!leafNodeList.isEmpty())
				fail("next page id is -1 but list is not empty. Elements left: "
								+ leafNodeList.size());
			// remove pinning
			unpinNode(leafNode.getPageNumber(), ioAccess);
		}
		
		return retList;
	}

	/**
	 * Returns the index page with the given page number.
	 * 
	 * @return node with given page number
	 */

	public static BTreeIndexPage getNode(int pageNumber,
			BufferPoolManager ioAccess) 
	{
		BTreeIndexPage node = null;
		try 
		{
			node = (BTreeIndexPage) ioAccess.getPageAndPin(INDEX_RESOURCE_ID,
					pageNumber);
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		}
		return node;
	}

	/**
	 * Unpins a node.
	 * 
	 * @param pageNumber The page number of the index page.
	 * @param ioAccess The BPM to access.
	 */
	public static void unpinNode(int pageNumber,
			BufferPoolManager ioAccess) 
	{
		try 
		{	
			ioAccess.unpinPage(INDEX_RESOURCE_ID, pageNumber);
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		}
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
	private static void initSchema(PageSize pageSize)
	{
		tableSchema = new TableSchema(pageSize);
		
		// create the columns as given
		tableSchema.addColumn(ColumnSchema.createColumnSchema("Random Column " + 1, DataType.smallIntType(), true));
		tableSchema.addColumn(ColumnSchema.createColumnSchema("Random Column " + 2, DataType.timeType(), true));
		tableSchema.addColumn(ColumnSchema.createColumnSchema("Random Column " + 3, DataType.charType(6), true));
		tableSchema.addColumn(ColumnSchema.createColumnSchema("Random Column " + 4, DataType.floatType(), true));
		tableSchema.addColumn(ColumnSchema.createColumnSchema("Random Column " + 5, DataType.bigIntType(), true));
		tableSchema.addColumn(ColumnSchema.createColumnSchema("Random Column " + 6, DataType.dateType(), true));
	}	
}
