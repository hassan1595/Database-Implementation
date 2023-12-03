package de.tuberlin.dima.minidb.test.qexec;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import de.tuberlin.dima.minidb.Config;
import de.tuberlin.dima.minidb.api.AbstractExtensionFactory;
import de.tuberlin.dima.minidb.api.ExtensionInitFailedException;
import de.tuberlin.dima.minidb.catalogue.ColumnSchema;
import de.tuberlin.dima.minidb.catalogue.IndexSchema;
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
import de.tuberlin.dima.minidb.core.RID;
import de.tuberlin.dima.minidb.core.SmallIntField;
import de.tuberlin.dima.minidb.core.TimeField;
import de.tuberlin.dima.minidb.core.TimestampField;
import de.tuberlin.dima.minidb.core.VarcharField;
import de.tuberlin.dima.minidb.io.cache.PageSize;
import de.tuberlin.dima.minidb.io.index.BTreeIndex;
import de.tuberlin.dima.minidb.io.index.IndexResourceManager;
import de.tuberlin.dima.minidb.io.index.IndexResultIterator;
import de.tuberlin.dima.minidb.io.manager.BufferPoolManager;
import de.tuberlin.dima.minidb.io.tables.TablePage;
import de.tuberlin.dima.minidb.io.tables.TableResourceManager;
import de.tuberlin.dima.minidb.parser.Predicate;
import de.tuberlin.dima.minidb.qexec.DeleteOperator;
import de.tuberlin.dima.minidb.qexec.IndexScanOperator;
import de.tuberlin.dima.minidb.qexec.InsertOperator;
import de.tuberlin.dima.minidb.qexec.LowLevelPredicate;
import de.tuberlin.dima.minidb.qexec.TableScanOperator;


/**
 * Final test case testing the table scan operator by performing inserts and then
 * scanning the table, validating against the inserted data. 
 * 
 * @author Stephan Ewen (sewen@cs.tu-berlin.de)
 * @author Michael Saecker
 */
public class TestPhysicalOperatorsIStudents
{	
	
	/**
	 * The number of values used in the test.
	 */
	private static final int numValuesToInsert = 5000;
	
	/**
	 * Resource id of the index under test.
	 */
	private static final int TABLE_RESOURCE_ID = 4;
	
	/**
	 * Resource id of the index under test.
	 */
	private static final int INDEX_RESOURCE_ID = 8;

	/**
	 * Number of schemas to test per page size.
	 */
	private static final int NUM_SCHEMAS = 2;
	
	/**
	 * Number of predicates to test.
	 */
	private static final int NUM_PREDICATES = 4;
	
	/**
	 * Fixed seed to make tests reproducible. Teacher test uses a different seed so please try your solution with another seed to check for remaining errors.
	 */
	private static final long SEED = 3514561854L;
	
	/**
	 * The random number generator used to create elements.
	 */
	private static Random random = new Random(SEED);
	
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
	 * File where the index data is stored.
	 */
	private final File tableFile = new File(this.getClass().getResource("/config.xml").getPath().replace("/config.xml", "/tempspace/minidbstesttable.mdtbl"));
	
	/**
	 * File where the index data is stored.
	 */
	private final File[] indexFile = new File[]{ 
			new File(this.getClass().getResource("/config.xml").getPath().replace("/config.xml", "/tempspace/minidbstestindexA.mdix")), 
			new File(this.getClass().getResource("/config.xml").getPath().replace("/config.xml", "/tempspace/minidbstestindexB.mdix")), 
			new File(this.getClass().getResource("/config.xml").getPath().replace("/config.xml", "/tempspace/minidbstestindexC.mdix")),
			new File(this.getClass().getResource("/config.xml").getPath().replace("/config.xml", "/tempspace/minidbstestindexD.mdix"))
	};
	
	
	static {
		try {
	        AbstractExtensionFactory.initializeDefault();
        }
        catch (ExtensionInitFailedException e) {
        	throw new RuntimeException(e.getMessage());
        }
	}
	
	/**
	 * Sets up the environment for each test.
	 */
	@Before
	public void setUp() throws Exception
	{
		InputStream is = this.getClass().getResourceAsStream(configFileName);
    	this.config = Config.loadConfig(is);
		this.logger = Logger.getLogger("BPM - Logger");		
	}

	/**
	 * Cleans up after each test run.
	 */
	@After
	public void tearDown() throws Exception
	{
		// make sure the table file is cleaned up
		try  {
			TableResourceManager.deleteTable(this.tableFile);
		}
		catch (IOException ioex) {}
		// make sure the index files are cleaned up
		for(File indexFile : this.indexFile)
		{
			try  
			{
				IndexResourceManager.deleteIndex(indexFile);
			}
			catch (IOException ioex) {}
		}		
	}
	
	/**
	 * Tests the table scan operator using different predicates and page sizes.
	 */
	@Test
	public void testTableScan() throws Exception
	{
		for (int pi = 0; pi < PageSize.values().length; pi++){

			PageSize ps = PageSize.values()[pi];
			// test all different schemas
			for (int si = 0; si < NUM_SCHEMAS; si++)
			{
				TableSchema schema = new TableSchema(ps);
				
				// generate a random set of columns
				int numCols = random.nextInt(16) + 1;
				
				// create the columns as given
				for (int col = 0; col < numCols; col++) {
					DataType type = getRandomDataType(true);
					schema.addColumn(ColumnSchema.createColumnSchema("COL" + col, type, true, false));
				}
			
				System.out.println("Creating table for schema " + schema.toString());
				
				BufferPoolManager bufferPool = null;
				TableResourceManager resManager = null;
				
				try 
				{
					// create the column map
					int[] colMap = new int[random.nextInt(schema.getNumberOfColumns()) + 1];
					for (int x = 0; x < colMap.length; x++)
					{
						colMap[x] = random.nextInt(schema.getNumberOfColumns());
					}
					
					// create the predicates
					LowLevelPredicate[][] testPred = new LowLevelPredicate[NUM_PREDICATES][NUM_PREDICATES];
					List<Set<DataTuple>> passingTuples = new ArrayList<Set<DataTuple>>(NUM_PREDICATES);
					
					for (int x = 0; x < NUM_PREDICATES; x++) 
					{
						testPred[x] = generatePredicates(schema, NUM_PREDICATES);
						passingTuples.add(new HashSet<DataTuple>(numValuesToInsert));
					}
					
					// make sure the table file does not exist
					try 
					{
						TableResourceManager.deleteTable(this.tableFile);
					}
					catch (IOException ioex) {}
					
					// now create the new table
					resManager = TableResourceManager.createTable(this.tableFile, schema);
					
					// create a buffer pool and register the index
					bufferPool = AbstractExtensionFactory.getExtensionFactory().createBufferPoolManager(this.config, this.logger);
					bufferPool.startIOThreads();
					bufferPool.registerResource(TABLE_RESOURCE_ID, resManager);	
					
					System.out.println("Inserting values...");
					TablePage currentPage = (TablePage) bufferPool.createNewPageAndPin(TABLE_RESOURCE_ID, null);
					
					for (int i = 0; i < numValuesToInsert; i++) 
					{
						// generate a tuple
						DataTuple tuple = new DataTuple(schema.getNumberOfColumns());
						
						// fill with random data
						for (int col = 0; col < schema.getNumberOfColumns(); col++) {
							ColumnSchema cs = schema.getColumn(col);
							tuple.assignDataField(generateRandomField(cs.getDataType()), col);
						}
						
						// add the projected tuple to the sets where the original tuple qualifies for the predicate
						for (int pred = 0; pred < NUM_PREDICATES; pred++) 
						{
							boolean passes = true;
							for(int lPred = 0; lPred < NUM_PREDICATES && passes; lPred++)
							{
								LowLevelPredicate lowPred = testPred[pred][lPred];
								DataField fieldToCheck = tuple.getField(lowPred.getColumnIndex());
								if (!lowPred.evaluateWithNull(fieldToCheck)) 
								{
									passes = false;
								}
							}
							if(passes)
							{
								// create the projected tuple
								DataTuple projected = new DataTuple(colMap.length);
								for (int col = 0; col < colMap.length; col++) 
								{
									projected.assignDataField(tuple.getField(colMap[col]), col);
								}
								passingTuples.get(pred).add(projected);
							}				
						}
						
						// insert the tuple into the table
						if (!currentPage.insertTuple(tuple)) 
						{
							// page full
							bufferPool.unpinPage(TABLE_RESOURCE_ID, currentPage.getPageNumber());
							currentPage = (TablePage) bufferPool.createNewPageAndPin(TABLE_RESOURCE_ID, null);
							if (!currentPage.insertTuple(tuple)) 
							{
								fail("Could not insert tuple into new blank page");
							}
						}
					}
					// unpin page
					bufferPool.unpinPage(TABLE_RESOURCE_ID, currentPage.getPageNumber());
					currentPage = null;
					System.out.println("Closing Buffer, forcing all pages to disk...");
					
					// close the buffer pool, forcing all pages of the table to be written
					bufferPool.closeBufferPool();
					bufferPool = null;
					resManager.closeResource();
					resManager = null;
					
					System.out.println("Recreating buffer...");
					
					// reopen the table
					bufferPool = AbstractExtensionFactory.getExtensionFactory().createBufferPoolManager(this.config, this.logger);
					bufferPool.startIOThreads();
					resManager = TableResourceManager.openTable(this.tableFile);
			
					bufferPool.registerResource(TABLE_RESOURCE_ID, resManager);
			
					System.out.println("Checking values...");
					
					// scan for all predicate sets
					for (int pred = 0; pred < NUM_PREDICATES; pred++) 
					{	
						// create the scan operator
						TableScanOperator scan = AbstractExtensionFactory.getExtensionFactory().createTableScanOperator(bufferPool, resManager, TABLE_RESOURCE_ID, colMap, testPred[pred], 16);
						
						// open the scan
						scan.open(null);
						Set<DataTuple> result = new HashSet<DataTuple>(passingTuples.get(pred).size());
						
						// go over all tuples
						DataTuple nextTuple = null;
						while ((nextTuple = scan.next()) != null) 
						{
							result.add(nextTuple);
						}
						
						// close the scan
						scan.close();
						
						// check that the tuple sets match
						Set<DataTuple> should = passingTuples.get(pred);
						assertTrue("Not all values that should be contained in the table scan result are contained.", result.containsAll(should));
						assertTrue("The table scan operator returned values that should not have been returned.", should.containsAll(result));
						
						// help the GC
						result.clear();
						passingTuples.get(pred).clear();
					}
					// clean up
					bufferPool.closeBufferPool();
					bufferPool = null;
					resManager.closeResource();
					resManager = null;
				}
				finally
				{
					if (bufferPool != null)
					{
						bufferPool.closeBufferPool();
					}
					if (resManager != null)
					{
						resManager.closeResource();
					}
				}
			}
		}
	}

	/**
	 * Tests the index scan operator using different page sizes and key ranges.
	 */
	@Test
	public void testIndexScan() throws Exception
	{
		for (int pi = 0; pi < PageSize.values().length; pi++)
		{
			PageSize ps = PageSize.values()[pi];
			// test all different schemas
			for (int si = 0; si < NUM_SCHEMAS; si++)
			{
				TableSchema schema = new TableSchema(ps);
				
				// generate a random set of columns
				int numCols = random.nextInt(5) + 1;
				
				// create the columns as given
				for (int col = 0; col < numCols; col++) 
				{
					DataType type = getRandomDataType(false);
					schema.addColumn(ColumnSchema.createColumnSchema("COL" + col, type, true, false));
				}
				IndexSchema ixSchema = new IndexSchema(schema, random.nextInt(schema.getNumberOfColumns()));
				System.out.println("Creating table for schema " + schema.toString());
				
				BufferPoolManager bufferPool = null;
				IndexResourceManager resManager = null;
				
				try 
				{
					// make sure the index file does not exist
					try 
					{
						IndexResourceManager.deleteIndex(this.indexFile[0]);
					}
					catch (IOException ioex) {}
					
					Map<DataField, List<RID>> comparisonMap = new HashMap<DataField, List<RID>>();
					// now create the new table
					resManager = IndexResourceManager.createIndex(this.indexFile[0], ixSchema);
					
					// create a buffer pool and register the index
					bufferPool = AbstractExtensionFactory.getExtensionFactory().createBufferPoolManager(this.config, this.logger);
					bufferPool.startIOThreads();
					bufferPool.registerResource(INDEX_RESOURCE_ID, resManager);	
					
					System.out.println("Inserting values...");
					BTreeIndex index = AbstractExtensionFactory.getExtensionFactory().createBTreeIndex(ixSchema, bufferPool, INDEX_RESOURCE_ID);
					
					for (int i = 0; i < numValuesToInsert; i++) 
					{
						// generate a key/rid pair
						DataField key = generateRandomField(ixSchema.getIndexedColumnSchema().getDataType());
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
					}
					System.out.println("Closing Buffer, forcing all pages to disk...");
					
					// close the buffer pool, forcing all pages of the table to be written
					bufferPool.closeBufferPool();
					bufferPool = null;
					resManager.closeResource();
					index = null;
					
					System.out.println("Recreating buffer...");
					
					// reopen the table
					bufferPool = AbstractExtensionFactory.getExtensionFactory().createBufferPoolManager(this.config, this.logger);
					bufferPool.startIOThreads();
					resManager = IndexResourceManager.openIndex(this.indexFile[0], schema);
			
					bufferPool.registerResource(INDEX_RESOURCE_ID, resManager);
					index = AbstractExtensionFactory.getExtensionFactory().createBTreeIndex(ixSchema, bufferPool, INDEX_RESOURCE_ID);
					
					System.out.println("Checking values...");
					
					// move values into sorted list
					ArrayList<DataField> keys = new ArrayList<DataField>(numValuesToInsert);
					for(DataField key : comparisonMap.keySet())
					{
						keys.add(key);
					}
					
					System.out.println("Checking different key ranges...");
					
					// sort keys
					Collections.sort(keys);			
					
					// scan for all predicate sets
					for (int pred = 0; pred < NUM_PREDICATES; pred++) 
					{	
						int range = random.nextInt(50);
						int x = random.nextInt(keys.size() - range);
						boolean startKeyIncluded = random.nextBoolean();
						boolean stopKeyIncluded = random.nextBoolean();
						DataField startKey = keys.get(x);
						DataField stopKey = keys.get(x + range);
						
						// create the scan operator
						IndexScanOperator scan = AbstractExtensionFactory.getExtensionFactory().createIndexScanOperator(index, startKey, stopKey, startKeyIncluded, stopKeyIncluded);
						
						// open the scan
						scan.open(null);
						ArrayList<DataTuple> result = new ArrayList<DataTuple>();
						
						// go over all tuples
						DataTuple nextTuple = null;
						while ((nextTuple = scan.next()) != null) {
							result.add(nextTuple);
						}
						
						// close the scan
						scan.close();

						// build list for comparison
						ArrayList<DataTuple> compList = new ArrayList<DataTuple>();
						int compSize = 0;
						int start = startKeyIncluded ? x : x + 1;
						int end = stopKeyIncluded ? x + range : x + range - 1;
						if(start <= end) // list is not empty?
						{
							int pos = start;
							while(pos <= end)
							{
								compList.add(new DataTuple(new DataField[] { keys.get(pos) }));
								compSize += comparisonMap.get(keys.get(pos)).size();
								pos++;
							}
						}					
						
						// check that the tuple sets match
						assertTrue("Not all values that should be contained in the index are contained.", result.containsAll(compList)); 
						assertTrue("The index returned values that should not have been returned.", compList.containsAll(result));
						assertTrue("Size of the results does not match the size of the reference list.", result.size() == compSize);
						
						// help the GC
						result.clear();
						compList.clear();
					} 
				}
				finally
				{
					if (bufferPool != null)
					{
						bufferPool.closeBufferPool();
					}
					if (resManager != null)
					{
						resManager.closeResource();
					}
				}
			}
		}
	}
	
	/**
	 * Generates a random data type for the schema.
	 * 
	 * @return A random data type.
	 */
	private static DataType getRandomDataType(boolean variableLength)
	{
		int num = random.nextInt(10);
		while(!variableLength && (num == 5 || num == 6))
		{
			num = random.nextInt(10); 
		}
		
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
			return DataType.charType(random.nextInt(32) + 1);
		case 6:
			return DataType.varcharType(random.nextInt(32) + 1);
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
			int y = random.nextInt(9999);
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
	 * Generates a single low level predicate.
	 * 
	 * @param schema The schema to use to generate a predicate.
	 * @return A predicate for the given schema.
	 * @throws Exception Thrown, if the generation of a field for the predicate fails.
	 */
	private LowLevelPredicate generatePredicate(TableSchema schema) throws Exception
	{
		int col = random.nextInt(schema.getNumberOfColumns());
		ColumnSchema colSchema = schema.getColumn(col);
		DataField literal = null;
		do 
		{
			literal = generateRandomField(colSchema.getDataType());
		}
		while (literal.isNULL());
		Predicate.Operator op = Predicate.Operator.values()[random.nextInt(Predicate.Operator.values().length - 1) + 1];
		return new LowLevelPredicate(op, literal, col);
	}
	
	/**
	 * Generates a concatenation of low level predicates.
	 * 
	 * @param schema The schema to use to generate a predicate.
	 * @param size The number of predicates that have to be contained in the array.
	 * @return An array of predicates for the given schema.
	 * @throws Exception Thrown, if the generation of a field for the predicate fails.
	 */
	private LowLevelPredicate[] generatePredicates(TableSchema schema, int size) throws Exception
	{
		LowLevelPredicate[] predicates = new LowLevelPredicate[size];
		for (int i = 0; i < size; i++)
		{
			predicates[i] = generatePredicate(schema);
		}
		return predicates;
	}
}
