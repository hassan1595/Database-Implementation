package de.tuberlin.dima.minidb.test.qexec;

import de.tuberlin.dima.minidb.Config;
import de.tuberlin.dima.minidb.api.AbstractExtensionFactory;
import de.tuberlin.dima.minidb.api.ExtensionInitFailedException;
import de.tuberlin.dima.minidb.catalogue.IndexSchema;
import de.tuberlin.dima.minidb.catalogue.TableSchema;
import de.tuberlin.dima.minidb.core.*;
import de.tuberlin.dima.minidb.io.index.BTreeIndex;
import de.tuberlin.dima.minidb.io.index.IndexResourceManager;
import de.tuberlin.dima.minidb.io.manager.BufferPoolManager;
import de.tuberlin.dima.minidb.io.manager.ResourceManager;
import de.tuberlin.dima.minidb.io.tables.TableResourceManager;
import de.tuberlin.dima.minidb.parser.Predicate.Operator;
import de.tuberlin.dima.minidb.qexec.*;
import de.tuberlin.dima.minidb.qexec.predicate.JoinPredicate;
import de.tuberlin.dima.minidb.qexec.predicate.JoinPredicateAtom;
import de.tuberlin.dima.minidb.qexec.predicate.LocalPredicate;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import static org.junit.Assert.assertTrue;


/**
 * Final test case testing all together the operators
 * - TableScan
 * - IndexScan (correlated and uncorrelated)
 * - Fetch
 * - Filter
 * - NestedLoopJoin
 *
 * The test case manually pieces together several query execution plans and evaluates them. The schema of the test
 * data is that of the TPC-H decision support benchmark. It is illustrated at
 *
 * {@link
 * <a href="http://www.informatik.hu-berlin.de/forschung/gebiete/wbi/teaching/archive/sose07/ue_dwhdm/tpch-schema.png">ttp://www.informatik.hu-berlin.de/forschung/gebiete/wbi/teaching/archive/sose07/ue_dwhdm/tpch-schema.png</a>}.
 *
 * @author Stephan Ewen (sewen@cs.tu-berlin.de)
 */
public class TestPhysicalOperatorsIIStudents
{	

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
	 * The offset for the index resource identifiers.
	 */
	protected int indexResourceIdOffset = 0;
	
	// --------------------------------------------------------------------------------------------
	//  The files pointing to the resources 
	// --------------------------------------------------------------------------------------------

	private static final String TEST_FILE_DIR = new File(TestPhysicalOperatorsIIStudents.class.getResource("/config.xml").getPath().replace("/config.xml", "/data")).getAbsolutePath() + "/";
	
	private static final String REGION_TABLE_FILE = "region.mdtbl";
	private static final String NATION_TABLE_FILE = "nation.mdtbl";
	private static final String SUPPLIER_TABLE_FILE = "supplier.mdtbl";
	private static final String PART_TABLE_FILE = "part.mdtbl";
	private static final String PARTSUPPLIER_TABLE_FILE = "partsupplier.mdtbl";
	private static final String CUSTOMER_TABLE_FILE = "customer.mdtbl";
	private static final String ORDER_TABLE_FILE = "order.mdtbl";
	private static final String LINEITEM_TABLE_FILE = "lineitem.mdtbl";
	
	private static final String[] TABLE_FILENAMES = {REGION_TABLE_FILE,
	                                                 NATION_TABLE_FILE,
	                                                 SUPPLIER_TABLE_FILE,
	                                                 PART_TABLE_FILE,
	                                                 PARTSUPPLIER_TABLE_FILE,
	                                                 CUSTOMER_TABLE_FILE,
	                                                 ORDER_TABLE_FILE,
	                                                 LINEITEM_TABLE_FILE};

	private static final String SUPPLIER_PKEY_INDEX_FILE = "supplier_pk.mdidx";
	private static final String PART_PKEY_INDEX_FILE = "part_pk.mdidx";
	private static final String PARTSUPPLIER_FKEY_PART_INDEX_FILE = "partsupplier_fk_part.mdidx";
	private static final String PARTSUPPLIER_FKEY_SUPPLIER_INDEX_FILE = "partsupplier_fk_supplier.mdidx";
	private static final String CUSTOMER_PKEY_INDEX_FILE = "customer_pk.mdidx";
	private static final String ORDER_PKEY_INDEX_FILE = "order_pk.mdidx";
	private static final String ORDER_FKEY_CUSTOMER_INDEX_FILE = "order_fk_customer.mdidx";
	private static final String LINEITEM_FKEY_ORDER_INDEX_FILE = "lineitem_fk_order.mdidx";
	private static final String LINEITEM_FKEY_PART_INDEX_FILE = "lineitem_fk_part.mdidx";
	private static final String LINEITEM_FKEY_SUPPLIER_INDEX_FILE = "lineitem_fk_supplier.mdidx";

	private static final String[] INDEX_FILENAMES = {SUPPLIER_PKEY_INDEX_FILE,
	                                                 PART_PKEY_INDEX_FILE,
	                                                 PARTSUPPLIER_FKEY_PART_INDEX_FILE,
	                                                 PARTSUPPLIER_FKEY_SUPPLIER_INDEX_FILE,
	                                                 CUSTOMER_PKEY_INDEX_FILE,
	                                                 ORDER_PKEY_INDEX_FILE,
	                                                 ORDER_FKEY_CUSTOMER_INDEX_FILE,
	                                                 LINEITEM_FKEY_ORDER_INDEX_FILE,
	                                                 LINEITEM_FKEY_PART_INDEX_FILE,
	                                                 LINEITEM_FKEY_SUPPLIER_INDEX_FILE};
	
	// --------------------------------------------------------------------------------------------
	//  The IDs for the resources 
	// --------------------------------------------------------------------------------------------
	
	private static final int REGION_TABLE_ID = 0;
	private static final int NATION_TABLE_ID = 1;
	private static final int SUPPLIER_TABLE_ID = 2;
	private static final int PART_TABLE_ID = 3;
	private static final int PARTSUPPLIER_TABLE_ID = 4;
	private static final int CUSTOMER_TABLE_ID = 5;
	private static final int ORDERS_TABLE_ID = 6;
	private static final int LINEITEM_TABLE_ID = 7;
	
	private static final int[] TABLE_IDS = {REGION_TABLE_ID,
		                                    NATION_TABLE_ID,
		                                    SUPPLIER_TABLE_ID,
		                                    PART_TABLE_ID,
		                                    PARTSUPPLIER_TABLE_ID,
		                                    CUSTOMER_TABLE_ID,
		                                    ORDERS_TABLE_ID,
		                                    LINEITEM_TABLE_ID };
	
	private static final int SUPPLIER_PKEY_INDEX_ID = 1;
	private static final int PART_PKEY_INDEX_ID = 1;
	private static final int PARTSUPPLIER_FKEY_PART_INDEX_ID = 2;
	private static final int PARTSUPPLIER_FKEY_SUPPLIER_INDEX_ID = 3;
	private static final int CUSTOMER_PKEY_INDEX_ID = 4;
	private static final int ORDER_PKEY_INDEX_ID = 5;
	private static final int ORDER_FKEY_CUSTOMER_INDEX_ID = 6;
	private static final int LINEITEM_FKEY_ORDER_INDEX_ID = 7;
	private static final int LINEITEM_FKEY_PART_INDEX_ID = 8;
	private static final int LINEITEM_FKEY_SUPPLIER_INDEX_ID = 9;

	
	private static final int[] INDEX_IDS = {SUPPLIER_PKEY_INDEX_ID,
	                                        PART_PKEY_INDEX_ID,
	                                        PARTSUPPLIER_FKEY_PART_INDEX_ID,
	                                        PARTSUPPLIER_FKEY_SUPPLIER_INDEX_ID,
	                                        CUSTOMER_PKEY_INDEX_ID,
	                                        ORDER_PKEY_INDEX_ID,
	                                        ORDER_FKEY_CUSTOMER_INDEX_ID,
	                                        LINEITEM_FKEY_ORDER_INDEX_ID,
	                                        LINEITEM_FKEY_PART_INDEX_ID,
	                                        LINEITEM_FKEY_SUPPLIER_INDEX_ID};
	
	private static final int[] INDEX_TABLE_ID = {SUPPLIER_TABLE_ID,
	                                             PART_TABLE_ID,
	                                             PARTSUPPLIER_TABLE_ID,
	                                             PARTSUPPLIER_TABLE_ID,
	                                             CUSTOMER_TABLE_ID,
	                                             ORDERS_TABLE_ID,
	                                             ORDERS_TABLE_ID,
	                                             LINEITEM_TABLE_ID,
	                                             LINEITEM_TABLE_ID,
	                                             LINEITEM_TABLE_ID};
	
	/**
	 * The path to the reference output data
	 */
	private static final String REFERENCE_DATA_PATH = new File("../minidbs-testdata/src/main/resources/data/operatorTests/").exists()? "../minidbs-testdata/src/main/resources/data/operatorTests/": "../minidbs-testdata/resources/data/operatorTests/";
	
	// --------------------------------------------------------------------------------------------
	//  Per Test Environment 
	// --------------------------------------------------------------------------------------------
	
	private BufferPoolManager bufferPool;
	
	private TableResourceManager[] tableResourceManagers;
	private TableSchema[] tableSchemas;
	
	private IndexSchema[] indexSchemas;
	
	private List<ResourceManager> allResources;
	
	
	// Initialize the extension factory from the default properties
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
    	this.logger = Logger.getLogger("BPM - Logger");		
		InputStream is = this.getClass().getResourceAsStream(configFileName);
    	this.config = Config.loadConfig(is);
		// start the buffer pool
		this.bufferPool = AbstractExtensionFactory.getExtensionFactory().createBufferPoolManager(this.config, this.logger);
		this.bufferPool.startIOThreads();
		this.allResources = new ArrayList<>();
		
		// Open all Table Resources. NOTE: That is a task that is in the final system a part of the
		// Start-up sequence, after reading in the catalogue.
		this.tableResourceManagers = new TableResourceManager[TABLE_IDS.length];
		this.tableSchemas = new TableSchema[TABLE_IDS.length];
		for (int i = 0; i < this.tableResourceManagers.length; i++) {
			File f = new File(TEST_FILE_DIR + TABLE_FILENAMES[i]);
			this.tableResourceManagers[i] = TableResourceManager.openTable(f);
			this.allResources.add(this.tableResourceManagers[i]);
			this.tableSchemas[i] = this.tableResourceManagers[i].getSchema();
			this.bufferPool.registerResource(i, this.tableResourceManagers[i]);
		}
		this.indexResourceIdOffset = this.tableResourceManagers.length + 10;
		// similarly, open the indexes
		this.indexSchemas = new IndexSchema[INDEX_IDS.length];
		for (int i = 0; i < this.indexSchemas.length; i++) {
			File f = new File(TEST_FILE_DIR + INDEX_FILENAMES[i]);
			IndexResourceManager resManager = IndexResourceManager.openIndex(f, this.tableSchemas[INDEX_TABLE_ID[i]]);
			this.allResources.add(resManager);
			this.indexSchemas[i] = resManager.getSchema();
			this.bufferPool.registerResource(i + this.indexResourceIdOffset, resManager);
		}
	}

	/**
	 * Cleans up after each test run.
	 */
	@After
    public void tearDown() throws Exception
    {
		// close all resources again
		this.bufferPool.closeBufferPool();
		this.bufferPool = null;
		for (final ResourceManager allResource : this.allResources) {
			allResource.closeResource();
		}
		this.allResources.clear();
		this.allResources = null;
    }
	
	/**
	 * Creates a plan that accesses an index and retrieves the RIDs for all tuples that
	 * have a certain value in that column.
	 * 
	 * @throws Exception Just as usual...
	 */
	@Test
	public void testUncorrelatedIndexEqualityPredicate() throws Exception
	{
		DataField[] testKeys = new DataField[] {
			new IntField(1627),
			new IntField(5297),
			new IntField(9127),
			new IntField(2056),
			new IntField(10366),
			new IntField(9181),
			new IntField(814),
			new IntField(2006),
			new IntField(9400),
			new IntField(11536)
		};
		
		// compose both the reference and the test plan
		int indexId = ORDER_FKEY_CUSTOMER_INDEX_ID;
		BTreeIndex index = AbstractExtensionFactory.getExtensionFactory().createBTreeIndex(this.indexSchemas[indexId], this.bufferPool, indexId + this.indexResourceIdOffset);
		
		// make some tests for all the testKeys
		for (int keyNum = 0; keyNum < testKeys.length; keyNum++) {
			
			IndexLookupOperator testIndexScan = AbstractExtensionFactory.getExtensionFactory().getIndexLookupOperator(index, testKeys[keyNum]);
			
			// plan is complete, that is it already
			exercisePlanTests(testIndexScan, REFERENCE_DATA_PATH+"OperatorsII_testUncorrelatedIndexEqualityPredicate_"+keyNum+".dat");
		}
	}
	
	/**
	 * Creates a plan that accesses a table through an index (IX-Scan/Fetch), retrieving
	 * all tuples where a foreign key is in a certain range in sorted order.
	 * 
	 * @throws Exception Just as usual...
	 */
	@Test
	public void testUncorrelatedIndexRangeFetch() throws Exception
	{
		DataField[] lowkeys = {new IntField(23), new IntField(522), new IntField(532), new IntField(123), new IntField(994), new IntField(365)};
		DataField[] highkeys = {new IntField(43), new IntField(556), new IntField(563), new IntField(123), new IntField(1000), new IntField(366)};
		boolean[] lowKeyIncluded = {true, false, false, true, true, true};
		boolean[] highKeyIncluded = {false, false, true, true, true, false};
		
		int indexId = PARTSUPPLIER_FKEY_SUPPLIER_INDEX_ID;
		int tableResourceId = INDEX_TABLE_ID[indexId];
		BTreeIndex index = AbstractExtensionFactory.getExtensionFactory().createBTreeIndex(this.indexSchemas[indexId], this.bufferPool, indexId + this.indexResourceIdOffset);
		int[] columnOutputMap = {0, 1, 2, 3, 4}; // identity map
		
		// make some tests for all the keys
		for (int keyNum = 0; keyNum < lowkeys.length; keyNum++) {
			
			IndexLookupOperator testIndexScan = AbstractExtensionFactory.getExtensionFactory().getIndexScanOperatorForBetweenPredicate(index, lowkeys[keyNum],
					lowKeyIncluded[keyNum], highkeys[keyNum], highKeyIncluded[keyNum]);
			
			FetchOperator testFetchOperator = AbstractExtensionFactory.getExtensionFactory().createFetchOperator(testIndexScan, this.bufferPool, tableResourceId, columnOutputMap);
			
			// plan is complete, that is it already
			exercisePlanTests(testFetchOperator, REFERENCE_DATA_PATH+"OperatorsII_testUncorrelatedIndexRangeFetch_"+keyNum+".dat");
		}
	}
	
	/**
	 * Creates a plan that joins the region and nation table through their keys. The
	 * join is a Nested-Loop-Join. Both inner and outer table are simply scanned. The
	 * nested-Loop-Join evaluates the join predicate.
	 * 
	 * @throws Exception Just as usual...
	 */
	@Test
	public void testNestedLoopJoinScanOnly() throws Exception
	{
		int outerTableId = REGION_TABLE_ID;
		int innerTableId = NATION_TABLE_ID;
		
		int[] outerTableColumnMap = {0, 1};
		int[] innerTableColumnMap = {0, 1, 2, 3};
		int[] joinOuterColumnMap = {-1, 1, -1};
		int[] joinInnerColumnMap = {1, -1, 3};
		
		JoinPredicate joinPred = new JoinPredicateAtom(Operator.EQUAL, 0, 2);
		
		TableScanOperator testOuterScanOperator = AbstractExtensionFactory.getExtensionFactory().createTableScanOperator(
				this.bufferPool, this.tableResourceManagers[outerTableId], outerTableId,
				outerTableColumnMap, null, 32);
		TableScanOperator testInnerScanOperator = AbstractExtensionFactory.getExtensionFactory().createTableScanOperator(
				this.bufferPool, this.tableResourceManagers[innerTableId], innerTableId,
				innerTableColumnMap, null, 32);
		NestedLoopJoinOperator testNLJNOperator = AbstractExtensionFactory.getExtensionFactory().createNestedLoopJoinOperator(
				testOuterScanOperator, testInnerScanOperator, joinPred,
				joinOuterColumnMap, joinInnerColumnMap);
		
		exercisePlanTests(testNLJNOperator, REFERENCE_DATA_PATH+"OperatorsII_testNestedLoopJoinScanOnly.dat");
	}
	
	/**
	 * Creates a plan that joins the orders and customer table through their keys. The
	 * join is a Nested-Loop-Join. The outer table is customer and it is scanned, 
	 * filtering the rows on the nation-key column. The inner side is a correlated index
	 * access and fetch, using the customer key to retrieve only the tuples for the
	 * current customer. The nested-Loop-Join has no join predicate, as it is
	 * represented in the correlation.
	 * 
	 * @throws Exception Just as usual...
	 */
	@Test
	public void testIndexNestedLoopJoin() throws Exception
	{
		// selection of the involved tables and produced columns
		int outerTableId = CUSTOMER_TABLE_ID;
		int innerTableId = ORDERS_TABLE_ID;
		int innerIndexId = ORDER_FKEY_CUSTOMER_INDEX_ID;
		
		int[] outerTableColumnMap = {0, 1, 6};
		int[] innerTableColumnMap = {1, 2, 3, 6};
		int[] joinOuterColumnMap = {1, 2, -1, -1, -1};
		int[] joinInnerColumnMap = {-1, -1, 1, 3, 2};
		
		// predicate will select only orders from customers from the country of Germany (nation key = 7)
		LowLevelPredicate selectPredicate = new LowLevelPredicate(Operator.EQUAL, new IntField(7), 3);
		
		// setup the plan under test
		TableScanOperator testOuterScanOperator = AbstractExtensionFactory.getExtensionFactory().createTableScanOperator(
				this.bufferPool, this.tableResourceManagers[outerTableId], outerTableId,
				outerTableColumnMap, new LowLevelPredicate[]{ selectPredicate }, 32);
		
		BTreeIndex testIndex = AbstractExtensionFactory.getExtensionFactory().createBTreeIndex(this.indexSchemas[innerIndexId], this.bufferPool, innerIndexId + this.indexResourceIdOffset);
		IndexCorrelatedLookupOperator testIxScan = AbstractExtensionFactory.getExtensionFactory().getIndexCorrelatedScanOperator(testIndex, 0);
		
		FetchOperator testFetch = AbstractExtensionFactory.getExtensionFactory().createFetchOperator(testIxScan, this.bufferPool, innerTableId, innerTableColumnMap);

		NestedLoopJoinOperator testNLJNOperator = AbstractExtensionFactory.getExtensionFactory().createNestedLoopJoinOperator(
				testOuterScanOperator, testFetch, null,
				joinOuterColumnMap, joinInnerColumnMap);
		
		exercisePlanTests(testNLJNOperator,  REFERENCE_DATA_PATH+"OperatorsII_testIndexNestedLoopJoin.dat");
	}
	
	/**
	 * Creates a plan that joins the orders and lineitem table through their keys. The
	 * join is a Nested-Loop-Join. The outer table is orders and it is scanned, 
	 * filtering the rows on the total price and priority. The inner side is a correlated index
	 * access and fetch, using the customer key to retrieve only the tuples for the
	 * current customer. The inner side (lineitem) is also filtered on the ship-mode column
	 * The nested-Loop-Join has no join predicate, as that is represented in the correlation.
	 * 
	 * @throws Exception Just as usual...
	 */
	@Test
	public void testIndexNestedLoopJoinWithFilter() throws Exception
	{
		// selection of the involved tables and produced columns
		int outerTableId = ORDERS_TABLE_ID;
		int innerTableId = LINEITEM_TABLE_ID;
		int innerIndexId = LINEITEM_FKEY_ORDER_INDEX_ID;
		
		int[] outerTableColumnMap = {0, 3, 4, 5};
		int[] innerTableColumnMap = {0, 3, 4, 8, 9, 10, 11, 12, 14};
		int[] joinOuterColumnMap = {-1, -1, -1, -1, -1, -1, -1, -1, 1, 2};
		int[] joinInnerColumnMap = {1, 2, 3, 4, 5, 6, 7, 8, -1, -1};
		
		// predicate will select only orders from customers from the country of Germany (nation key = 7)
		LowLevelPredicate[] outerSelectPredicate = 	new LowLevelPredicate[] {
						new LowLevelPredicate(Operator.GREATER_OR_EQUAL, new FloatField(400000.0f), 3),
						new LowLevelPredicate(Operator.EQUAL, new CharField("1-URGENT       "), 5)
				};
		LocalPredicate innerSelectPredicate = new LowLevelPredicate(Operator.EQUAL, new CharField("AIR       "), 8);
		
		// setup the plan under test
		TableScanOperator testOuterScanOperator = AbstractExtensionFactory.getExtensionFactory().createTableScanOperator(
				this.bufferPool, this.tableResourceManagers[outerTableId], outerTableId,
				outerTableColumnMap, outerSelectPredicate, 32);
		
		BTreeIndex testIndex = AbstractExtensionFactory.getExtensionFactory().createBTreeIndex(this.indexSchemas[innerIndexId], this.bufferPool, innerIndexId + this.indexResourceIdOffset);
		IndexCorrelatedLookupOperator testIxScan = AbstractExtensionFactory.getExtensionFactory().getIndexCorrelatedScanOperator(testIndex, 0);
		
		FetchOperator testFetch = AbstractExtensionFactory.getExtensionFactory().createFetchOperator(testIxScan, this.bufferPool, innerTableId, innerTableColumnMap);
		
		FilterOperator testFilter = AbstractExtensionFactory.getExtensionFactory().createFilterOperator(testFetch, innerSelectPredicate);

		NestedLoopJoinOperator testNLJNOperator = AbstractExtensionFactory.getExtensionFactory().createNestedLoopJoinOperator(
				testOuterScanOperator, testFilter, null,
				joinOuterColumnMap, joinInnerColumnMap);
		
		exercisePlanTests(testNLJNOperator, REFERENCE_DATA_PATH+"OperatorsII_testIndexNestedLoopJoinWithFilter.dat");
	}
	
	/**
	 * Sets up a plan joining across five tables, performing multiple scans,
	 * a correlated index access as well as correlated and uncorrelated filtering.
	 * 
	 * The plan answers the query: 
	 * Give me the name and brand of all parts, which are supplied by suppliers from Africa.
	 * 
	 * The plan is that:
	 * <pre>
	 *                                          RETURN
	 *                                             |
	 *                             +-------------NLJN--------------+
	 *                            /                                 \
	 *                           |                                   |
	 *                           |                             +----NLJN----+
	 *                +--------NLJN-------+                   /              \
	 *               /                      \                |                |
	 *              |                        |               |             FILTER
	 *         +---NLJN---+                  |             FILTER             |
	 *        /            \                 |          (correlated)        FETCH (Table: part)
	 *       |             |                 |               |                |
	 *      SCAN          SCAN              SCAN            SCAN           IX-SCAN
	 * TABLE: region  TABLE: nation   TABLE: supplier  TABLE: partsupp  INDEX: part(PK)    
	 * </pre>
	 * 
	 * The SCAN on region applies a predicate filtering out only the region of ARFICA.
	 * The FILTER on the part table lets only parts from Manufacturer#3 pass.
	 * Note that the whole plan dealing with tuples from partsupp and part is invoked
	 * multiple times. The correlated tuple gets passed to the FILTER on partsupp. The
	 * IX-SCAN is correlated to the current tuple from partsupp and hence transitively
	 * correlated to the plan that has joined region, nation and supplier.
	 * 
	 * The plan is quite inefficient, because some operators are not yet available.
	 * For example executing these many Nested-Loop-Joins is not a good decision, which
	 * we only do for the current lack of HashJoin or SortMergeJoin. 
	 * 
	 * @throws Exception As usual...
	 */
	@Test
	public void testMultiwayJoin() throws Exception
	{
		// set up the IDs
		int firstTableId = REGION_TABLE_ID;
		int secondTableId = NATION_TABLE_ID;
		int thirdTableId = SUPPLIER_TABLE_ID;
		int fourthTableId = PARTSUPPLIER_TABLE_ID;
		int fifthTableId = PART_TABLE_ID;
		
		int fifthIndexId = PART_PKEY_INDEX_ID;
		
		// set up the produced columns
		int[] firstTableColumns = {0, 1};
		int[] secondTableColumns = {0, 1, 2};
		int[] thirdTableColumns = {0, 1, 3};
		int[] fourthTableColumns = {0, 1};
		int[] fifthTableColumns = {1, 2, 3};
		
		int[] firstJoinLeftColumns = {-1, -1};
		int[] firstJoinRightColumns = {0, 1};
		int[] secondJoinLeftColumns = {-1, -1, 1};
		int[] secondJoinRightColumns = {0, 1, -1};
		int[] correlatedJoinLeftColumns = {1, -1, -1};
		int[] correlatedJoinRightColumns = {-1, 0, 2};
		int[] topJoinLeftColumns = {-1, -1, 1, 2};
		int[] topJoinRightColumns = {1, 2, -1, -1};
		
		// set up the predicates
		LowLevelPredicate leftScanPredicate = new LowLevelPredicate(Operator.EQUAL, new CharField("AFRICA                   "), 1);
		LowLevelPredicate fifthScanPredicate = new LowLevelPredicate(Operator.EQUAL, new CharField("Manufacturer#3           "), 1);
		
		JoinPredicate firstJoinPred = new JoinPredicateAtom(Operator.EQUAL, 0, 2);
		JoinPredicate secondJoinPred = new JoinPredicateAtom(Operator.EQUAL, 0, 2);
		
		JoinPredicate filterCorrelatedPredicate = new JoinPredicateAtom(Operator.EQUAL, 1, 0);
		
		// set up the plan under test
		TableScanOperator testFirstTableScan = AbstractExtensionFactory.getExtensionFactory().createTableScanOperator(
				this.bufferPool, this.tableResourceManagers[firstTableId], firstTableId,
				firstTableColumns, new LowLevelPredicate[] { leftScanPredicate }, 32);
		
		TableScanOperator testSecondTableScan = AbstractExtensionFactory.getExtensionFactory().createTableScanOperator(
				this.bufferPool, this.tableResourceManagers[secondTableId], secondTableId,
				secondTableColumns, null, 32);
		
		NestedLoopJoinOperator testFirstJoinOp = AbstractExtensionFactory.getExtensionFactory().createNestedLoopJoinOperator(
				testFirstTableScan, testSecondTableScan, firstJoinPred,
				firstJoinLeftColumns, firstJoinRightColumns);
		
		TableScanOperator testThirdTableScan = AbstractExtensionFactory.getExtensionFactory().createTableScanOperator(
				this.bufferPool, this.tableResourceManagers[thirdTableId], thirdTableId,
				thirdTableColumns, null, 32);
		
		NestedLoopJoinOperator testSecondJoinOp = AbstractExtensionFactory.getExtensionFactory().createNestedLoopJoinOperator(
				testFirstJoinOp, testThirdTableScan, secondJoinPred,
				secondJoinLeftColumns, secondJoinRightColumns);
		
		TableScanOperator testFourthScan = AbstractExtensionFactory.getExtensionFactory().createTableScanOperator(
				this.bufferPool, this.tableResourceManagers[fourthTableId], fourthTableId, fourthTableColumns, null, 32);
		
		FilterCorrelatedOperator testFourthFilter = AbstractExtensionFactory.getExtensionFactory().createCorrelatedFilterOperator(
				testFourthScan, filterCorrelatedPredicate);

		
		BTreeIndex testFifthIndex = AbstractExtensionFactory.getExtensionFactory().createBTreeIndex(this.indexSchemas[fifthIndexId],
				this.bufferPool, fifthIndexId + this.indexResourceIdOffset);
		IndexCorrelatedLookupOperator testIndexScan = AbstractExtensionFactory.getExtensionFactory().getIndexCorrelatedScanOperator(
				testFifthIndex, 0);
		FetchOperator testFifthFetch = AbstractExtensionFactory.getExtensionFactory().createFetchOperator(testIndexScan,
				this.bufferPool, fifthTableId, fifthTableColumns);
		FilterOperator testFifthFilter = AbstractExtensionFactory.getExtensionFactory().createFilterOperator(
				testFifthFetch, fifthScanPredicate); 
		
		NestedLoopJoinOperator testCorrelatedNLJNOperator = AbstractExtensionFactory.getExtensionFactory().createNestedLoopJoinOperator(
				testFourthFilter, testFifthFilter, null,
				correlatedJoinLeftColumns, correlatedJoinRightColumns);
		
		NestedLoopJoinOperator testTopNLJNOperator = AbstractExtensionFactory.getExtensionFactory().createNestedLoopJoinOperator(
				testSecondJoinOp, testCorrelatedNLJNOperator, null,
				topJoinLeftColumns, topJoinRightColumns);
		
		exercisePlanTests(testTopNLJNOperator,  REFERENCE_DATA_PATH+"OperatorsII_testMultiwayJoin.dat");
	}


	/**
	 * Executes the plan constructed using the ExtensionFactory and the reference plan to check that
	 * the result of both is identical.
	 * 
	 * @param testPlan The plan constructed using the ExtensionFactory.
	 * @param referenceSolutionPath The path to the reference solution data
	 */
	private static void exercisePlanTests(PhysicalPlanOperator testPlan, String referenceSolutionPath)
	throws Exception
	{
		Set<DataTuple> testTuples = new HashSet<>();
		Set<DataTuple> referenceTuples = readFromFile(referenceSolutionPath);
		
		DataTuple tuple;
		
		// root is always opened uncorrelated
		System.out.println("Building test plan tuples...");
		testPlan.open(null);
		while ((tuple = testPlan.next()) != null)
		{
			testTuples.add(tuple);
		}
		testPlan.close();
		System.out.println("Test plan tuples built.");
		
		assertTrue("Tuple sets must be equal.", areTupleSetsEqual(testTuples, referenceTuples));
	}
	
	
	/**
	 * Checks whether two sets of tuples are equal.
	 * 
	 * @param testTuples First set to check.
	 * @param referenceTuples Second set to check.
	 * @return True, if the two sets are equal, false otherwise.
	 */
	private static boolean areTupleSetsEqual(Set<DataTuple> testTuples, Set<DataTuple> referenceTuples)
	{
		boolean userTuplesAllAvailable = testTuples.containsAll(referenceTuples);
		boolean userNoFalseTupels = referenceTuples.containsAll(testTuples);
		
		return userTuplesAllAvailable & userNoFalseTupels;
	}
	
	/**
	 * Writes a list of tuples to a file.
	 * You can use this method to load previously stored outputs.
	 * 
	 * @param path The path of the output file
	 */
	private static Set<DataTuple> readFromFile(String path) throws IOException, ClassNotFoundException{
		FileInputStream fis = new FileInputStream(path);
		ObjectInputStream ois = new ObjectInputStream(fis);
		@SuppressWarnings("unchecked")
		Set<DataTuple> result = (Set<DataTuple>) ois.readObject();
		ois.close();
		return result;
	}
}
