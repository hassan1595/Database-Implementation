package de.tuberlin.dima.minidb.test.qexec;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import de.tuberlin.dima.minidb.DBInstance;
import de.tuberlin.dima.minidb.api.AbstractExtensionFactory;
import de.tuberlin.dima.minidb.catalogue.IndexDescriptor;
import de.tuberlin.dima.minidb.catalogue.TableDescriptor;
import de.tuberlin.dima.minidb.core.BasicType;
import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.core.DataType;
import de.tuberlin.dima.minidb.core.DoubleField;
import de.tuberlin.dima.minidb.core.FloatField;
import de.tuberlin.dima.minidb.core.IntField;
import de.tuberlin.dima.minidb.io.index.BTreeIndex;
import de.tuberlin.dima.minidb.io.manager.BufferPoolManager;
import de.tuberlin.dima.minidb.parser.OutputColumn;
import de.tuberlin.dima.minidb.parser.Predicate.Operator;
import de.tuberlin.dima.minidb.qexec.GroupByOperator;
import de.tuberlin.dima.minidb.qexec.IndexLookupOperator;
import de.tuberlin.dima.minidb.qexec.LowLevelPredicate;
import de.tuberlin.dima.minidb.qexec.MergeJoinOperator;
import de.tuberlin.dima.minidb.qexec.NestedLoopJoinOperator;
import de.tuberlin.dima.minidb.qexec.PhysicalPlanOperator;
import de.tuberlin.dima.minidb.qexec.SortOperator;
import de.tuberlin.dima.minidb.qexec.TableScanOperator;
import de.tuberlin.dima.minidb.qexec.predicate.JoinPredicate;
import de.tuberlin.dima.minidb.qexec.predicate.JoinPredicateAtom;

/**
 * Test case testing the sort based operators.
 * 
 * The test case manually pieces together several query execution plans and evaluates them. The schema of the test
 * data is that of the TPC-H decision support benchmark. It is illustrated at
 * {@link http://www.informatik.hu-berlin.de/forschung/gebiete/wbi/teaching/archive/sose07/ue_dwhdm/tpch-schema.png}.
 * 
 * @author Stephan Ewen (sewen@cs.tu-berlin.de)
 * @author Michael Saecker (modified)
 * +
 */
public class TestPhysicalOperatorsIIIStudents {

	/**
	 * The path to the file with the configuration.
	 */
	private static final String CONFIG_FILE_NAME = "/config.xml";
	
	/**
	 * The path to the file with the catalogue.
	 */
	private static final String CATALOGUE_FILE_NAME = "/catalogue.xml";
	
	/**
	 * The logger to be used.
	 */
	private static final Logger OUT_LOGGER = Logger.getLogger("Testlogger");
	
	/**
	 * The instance used for this query.
	 */
	private DBInstance theInstance;
	
	/**
	 * The path to the reference output data
	 */
	private static final String REFERENCE_DATA_PATH = new File("../minidbs-testdata/src/main/resources/data/operatorTests/").exists()? "../minidbs-testdata/src/main/resources/data/operatorTests/": "../minidbs-testdata/resources/data/operatorTests/";

	static {
		// initialize the logger
		OUT_LOGGER.setLevel(Level.INFO);
		OUT_LOGGER.setUseParentHandlers(false);
		
		Handler consoleHandler = new ConsoleHandler();
		consoleHandler.setLevel(Level.ALL);
//		consoleHandler.setFormatter(new ConsoleMessageFormatter());
		OUT_LOGGER.addHandler(consoleHandler);
	}

	/**
	 * Sets up the environment for each test.
	 */
	@Before
	public void setUp() throws Exception
	{
		// get the class name of the extension factory class
		AbstractExtensionFactory.initializeDefault();
		
		
		// create the instance object
		this.theInstance = new DBInstance(OUT_LOGGER, CONFIG_FILE_NAME, CATALOGUE_FILE_NAME);
		if (DBInstance.RETURN_CODE_OKAY != this.theInstance.startInstance())
		{
			throw new Exception(
					"The instance could not be started. Refer to the console for more detail...");
		}
	}

	/**
	 * Cleans up after each test. 
	 */
	@After
    public void tearDown() throws Exception
    {
		this.theInstance.shutdownInstance();
    }

	
	/**
	 * Performs a sort on a projection of the tuples from the 'SUPPLIER' table.
	 * The table has only 1000 rows, so the sort is internal with the default sort
	 * heap configuration...
	 * 
	 * @throws Exception As usual...
	 */
	@Test
	public void testInternalSort() throws Exception
	{
		// get the table descriptor from the catalogue
		TableDescriptor td = this.theInstance.getCatalogue().getTable("SUPPLIER");
		
		int[] producedCols =  {0, 1, 2, 3, 4, 5};
		int[] sortColumns = {3, 1};
		DataType[] sortColumnTypes = new DataType[producedCols.length];
		for (int i = 0; i < sortColumnTypes.length; i++) {
			sortColumnTypes[i] = td.getSchema().getColumn(producedCols[i]).getDataType();
		}
		boolean[] sortDirection = { false, true };
		
		// create the operators under test 
		TableScanOperator testTableScan = AbstractExtensionFactory.getExtensionFactory().createTableScanOperator(this.theInstance.getBufferPool(),
				td.getResourceManager(), td.getResourceId(), producedCols, null, 150);
		
		SortOperator testSort = AbstractExtensionFactory.getExtensionFactory().createSortOperator(testTableScan,
				this.theInstance.getQueryHeap(), sortColumnTypes, (int) td.getStatistics().getCardinality(), 
				sortColumns, sortDirection);
		
		exerciseSortedPlanTest(testSort, sortColumns, sortDirection);
	}
	
	
	/**
	 * Performs a sort on a projection of the tuples from the 'LINEITEM' table.
	 * The table has only 600,000 rows, so the sort is external with the default sort
	 * heap configuration...
	 * 
	 * @throws Exception As usual...
	 */
	@Test
	public void testExternalSort() throws Exception
	{		
		// get the table descriptor from the catalogue
		TableDescriptor td = this.theInstance.getCatalogue().getTable("LINEITEM");
		
		int[] producedCols =  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
		int[] sortColumns = {1, 4, 8};
		DataType[] sortColumnTypes = new DataType[producedCols.length];
		for (int i = 0; i < sortColumnTypes.length; i++) {
			sortColumnTypes[i] = td.getSchema().getColumn(producedCols[i]).getDataType();
		}
		boolean[] sortDirection = {true, true, true};
		
		// create the operators under test 
		
		TableScanOperator testTableScan = AbstractExtensionFactory.getExtensionFactory().createTableScanOperator(
				this.theInstance.getBufferPool(), td.getResourceManager(),
				td.getResourceId(), producedCols, null, 150);
		
		SortOperator testSort = AbstractExtensionFactory.getExtensionFactory().createSortOperator(testTableScan,
				this.theInstance.getQueryHeap(), sortColumnTypes,
				(int) td.getStatistics().getCardinality(), 
				sortColumns, sortDirection);
		
		exerciseSortedPlanTest(testSort, sortColumns, sortDirection);
	}
	
	
	/**
	 * Counts the number of rows in the 'PARTSUPPLIER' table. It actually counts the number
	 * of RIDS in the index on the foreign key column 'PART_KEY' (which is the same).
	 * 
	 *  There is no sort prior to this group by operator, because we have no grouping columns!
	 * 
	 * @throws Exception As usual...
	 */
	@Test
	public void testSimpleCount() throws Exception
	{
		IndexDescriptor id = this.theInstance.getCatalogue().getIndex("PARTSUPPLIER_FK_PART");
		BufferPoolManager pool = this.theInstance.getBufferPool();
		
		// set up the test plan
		BTreeIndex testIndex = AbstractExtensionFactory.getExtensionFactory().createBTreeIndex(id.getSchema(), pool, id.getResourceId());
		IndexLookupOperator testIndexScan = AbstractExtensionFactory.getExtensionFactory().getIndexScanOperatorForBetweenPredicate(
				testIndex, DataType.intType().getNullValue(), true, new IntField(Integer.MAX_VALUE), true);
		
		GroupByOperator testGroupBy = AbstractExtensionFactory.getExtensionFactory().createGroupByOperator(testIndexScan,
				new int[] {}, // no grouping columns
				new int[] {0}, // the only column (RID column) is aggregated once
				new OutputColumn.AggregationType[] { OutputColumn.AggregationType.COUNT }, // count the rids
				new DataType[] { DataType.ridType() }, // type is RID
				new int[] { -1 }, // group columns produce no output
				new int[] { 0 } ); // aggregate column 0 goes to out column 0
		
		exercisePlanTests(testGroupBy, REFERENCE_DATA_PATH+"OperatorsIII_testSimpleCount.dat");
	}
	
	/**
	 * Groups and counts customers by their nation and computes the minimal, maximal, total and average
	 * account balance. It afterwards joins in the nation name.
	 * 
	 * @throws Exception As usual...
	 */
	@Test
	public void testGroupCustomers() throws Exception
	{
		TableDescriptor custTable = this.theInstance.getCatalogue().getTable("CUSTOMER");
		TableDescriptor nationTable = this.theInstance.getCatalogue().getTable("NATION");
		
		BufferPoolManager bufferPool = this.theInstance.getBufferPool();
		
		int[] customerColumns = new int[] {3, 5};   	  // table customer: nation_key and balance
		
		int[] sortAndGroupCols = new int[] {0};     	  // sort and group on nation key
		boolean[] sortDirections = new boolean[] {true};  // sort ascending
		DataType[] sortType =						  // schema of sorted column 
			new DataType[] { custTable.getSchema().getColumn(3).getDataType(),
				                 custTable.getSchema().getColumn(5).getDataType()};
		
		int[] aggColIndices = new int[] {1, 1, 1, 1, 1};  // aggregate 5 aggregates on same column
		OutputColumn.AggregationType[] aggs = new OutputColumn.AggregationType[] {
				OutputColumn.AggregationType.COUNT,
				OutputColumn.AggregationType.SUM,
				OutputColumn.AggregationType.AVG,
				OutputColumn.AggregationType.MIN,
				OutputColumn.AggregationType.MAX};
		DataType[] aggTypes = new DataType[] {
				DataType.intType(),
				DataType.floatType(),
				DataType.floatType(),
				DataType.floatType(),
				DataType.floatType()};
		int[] groupOutput = new int[] {0, -1, -1, -1, -1, -1};
		int[] aggOutput = new int[] {-1 , 0, 1, 2, 3, 4};
		
		int[] nationColumns = new int[] {0, 1};
		
		JoinPredicate joinPred = new JoinPredicateAtom(Operator.EQUAL, 0, 0);
		int[] leftHandOutput = new int[] {-1, 1, 2, 3, 4, 5};
		int[] rightHandOutput = new int[] {1, -1, -1, -1, -1, -1};
		
		int[] finalSortColumns = new int[] { 0 };
		boolean[] finalSortOrder = new boolean[] { true };
		DataType fType = custTable.getSchema().getColumn(5).getDataType();
		DataType[] finalSortType = new DataType[] {
				nationTable.getSchema().getColumn(1).getDataType(),
				DataType.intType(), fType, fType, fType, fType};
		
		// set up the test plan
		TableScanOperator testTableCustomerScan = AbstractExtensionFactory.getExtensionFactory().createTableScanOperator(
				bufferPool, custTable.getResourceManager(), custTable.getResourceId(),
				customerColumns, null, 150);
		
		SortOperator testSort = AbstractExtensionFactory.getExtensionFactory().createSortOperator(testTableCustomerScan,
				this.theInstance.getQueryHeap(), sortType,
				(int) custTable.getStatistics().getCardinality(), sortAndGroupCols, sortDirections);
		
		GroupByOperator testGroupBy = AbstractExtensionFactory.getExtensionFactory().createGroupByOperator(testSort,
				sortAndGroupCols, aggColIndices, aggs, aggTypes,
				groupOutput, aggOutput);
		
		TableScanOperator testNationScan = AbstractExtensionFactory.getExtensionFactory().createTableScanOperator(
				bufferPool, nationTable.getResourceManager(), nationTable.getResourceId(),
				nationColumns, null, 10);
		
		NestedLoopJoinOperator testNlJoin = AbstractExtensionFactory.getExtensionFactory().createNestedLoopJoinOperator(
				testGroupBy, testNationScan, joinPred, leftHandOutput, rightHandOutput);
		
		SortOperator testFinalSort = AbstractExtensionFactory.getExtensionFactory().createSortOperator(testNlJoin,
				this.theInstance.getQueryHeap(), finalSortType,
				(int) nationTable.getStatistics().getCardinality(), finalSortColumns, finalSortOrder);
				
		exercisePlanTests(testFinalSort, REFERENCE_DATA_PATH+"OperatorsIII_testGroupCustomers.dat");
	}
	
	/**
	 * Same as before, but with a predicate that kills all rows...
	 * 
	 * @throws Exception As usual...
	 */
	@Test
	public void testGroupCustomersEmpty() throws Exception
	{
		TableDescriptor custTable = this.theInstance.getCatalogue().getTable("CUSTOMER");
		
		BufferPoolManager bufferPool = this.theInstance.getBufferPool();
		
		int[] customerColumns = new int[] {3, 5};   	  // table customer: nation_key and balance
		LowLevelPredicate pred = new LowLevelPredicate(Operator.SMALLER, new IntField(0), 0);
		
		int[] sortAndGroupCols = new int[] {0};     	  // sort and group on nation key
		boolean[] sortDirections = new boolean[] {true};  // sort ascending
		DataType[] sortType =						  // schema of sorted column 
			new DataType[] { custTable.getSchema().getColumn(3).getDataType(),
				                 custTable.getSchema().getColumn(5).getDataType()};
		
		int[] aggColIndices = new int[] {1, 1, 1, 1, 1};  // aggregate 5 aggregates on same column
		OutputColumn.AggregationType[] aggs = new OutputColumn.AggregationType[] {
				OutputColumn.AggregationType.COUNT,
				OutputColumn.AggregationType.SUM,
				OutputColumn.AggregationType.AVG,
				OutputColumn.AggregationType.MIN,
				OutputColumn.AggregationType.MAX};
		DataType[] aggTypes = new DataType[] {
				DataType.intType(),
				DataType.floatType(),
				DataType.floatType(),
				DataType.floatType(),
				DataType.floatType()};
		int[] groupOutput = new int[] {0, -1, -1, -1, -1, -1};
		int[] aggOutput = new int[] {-1 , 0, 1, 2, 3, 4};
		
		// set up the test plan
		TableScanOperator testTableCustomerScan = AbstractExtensionFactory.getExtensionFactory().createTableScanOperator(
				bufferPool, custTable.getResourceManager(), custTable.getResourceId(),
				customerColumns, new LowLevelPredicate[]{ pred }, 150);
		
		SortOperator testSort = AbstractExtensionFactory.getExtensionFactory().createSortOperator(testTableCustomerScan,
				this.theInstance.getQueryHeap(), sortType,
				(int) custTable.getStatistics().getCardinality(), sortAndGroupCols, sortDirections);
		
		GroupByOperator testGroupBy = AbstractExtensionFactory.getExtensionFactory().createGroupByOperator(testSort,
				sortAndGroupCols, aggColIndices, aggs, aggTypes,
				groupOutput, aggOutput);
				
		exercisePlanTests(testGroupBy, REFERENCE_DATA_PATH+"OperatorsIII_testGroupCustomersEmpty.dat");
	}
	
	/**
	 * Same as before, but only aggregating, no grouping...
	 * 
	 * @throws Exception As usual...
	 */
	@Test
	public void testAggregateCustomersEmpty() throws Exception
	{
		TableDescriptor custTable = this.theInstance.getCatalogue().getTable("CUSTOMER");
		
		BufferPoolManager bufferPool = this.theInstance.getBufferPool();
		
		int[] customerColumns = new int[] {5}; 
		LowLevelPredicate pred = new LowLevelPredicate(Operator.SMALLER, new IntField(0), 0);
		
		int[] groupCols = new int[] {};
		
		int[] aggColIndices = new int[] {0, 0, 0, 0, 0};
		OutputColumn.AggregationType[] aggs = new OutputColumn.AggregationType[] {
				OutputColumn.AggregationType.COUNT,
				OutputColumn.AggregationType.SUM,
				OutputColumn.AggregationType.AVG,
				OutputColumn.AggregationType.MIN,
				OutputColumn.AggregationType.MAX};
		DataType[] aggTypes = new DataType[] {
				DataType.intType(),
				DataType.floatType(),
				DataType.floatType(),
				DataType.floatType(),
				DataType.floatType()};
		int[] groupOutput = new int[] {-1, -1, -1, -1, -1};
		int[] aggOutput = new int[] {0, 1, 2, 3, 4};
		
		// set up the test plan
		TableScanOperator testTableCustomerScan = AbstractExtensionFactory.getExtensionFactory().createTableScanOperator(
				bufferPool, custTable.getResourceManager(), custTable.getResourceId(),
				customerColumns, new LowLevelPredicate[]{ pred }, 150);
		
		GroupByOperator testGroupBy = AbstractExtensionFactory.getExtensionFactory().createGroupByOperator(testTableCustomerScan,
				groupCols, aggColIndices, aggs, aggTypes,
				groupOutput, aggOutput);
		
		exercisePlanTests(testGroupBy, REFERENCE_DATA_PATH+"OperatorsIII_testAggregateCustomersEmpty.dat");
	}

	/**
	 * Tests the MGJN by joining 
	 * @throws Exception
	 */
	@Test
	public void testMergeJoin() throws Exception
	{

		BufferPoolManager bufferPool = this.theInstance.getBufferPool();
		
		// customer_id (int), nation_id (int)
		int[] customerColumns = new int[] { 0, 3 };
		// nation_id (int), nation name (char(25))
		int[] nationColumns = new int[] { 0, 1 };
		// custkey (int) , totalprice (float)
		int[] ordersColumns = new int[] { 1, 3 };
		
		TableDescriptor customerTable = this.theInstance.getCatalogue().getTable("CUSTOMER");
		TableDescriptor ordersTable = this.theInstance.getCatalogue().getTable("ORDERS");
		TableDescriptor nationTable = this.theInstance.getCatalogue().getTable("NATION");

		
		// set up test plan
		TableScanOperator testTableCustomerScan = AbstractExtensionFactory.getExtensionFactory().createTableScanOperator(
				bufferPool, customerTable.getResourceManager(), customerTable.getResourceId(),
				customerColumns, new LowLevelPredicate[0], 150);
		
		SortOperator testNationKeyCustomerTableSort = AbstractExtensionFactory.getExtensionFactory().createSortOperator(testTableCustomerScan, 
				this.theInstance.getQueryHeap(), new DataType[] { customerTable.getSchema().getColumn(0).getDataType(), customerTable.getSchema().getColumn(3).getDataType() }, 
				(int) customerTable.getStatistics().getCardinality(), new int[]{ 1 }, new boolean[]{ true } );

		TableScanOperator testTableNationScan = AbstractExtensionFactory.getExtensionFactory().createTableScanOperator(
				bufferPool, nationTable.getResourceManager(), nationTable.getResourceId(),
				nationColumns, new LowLevelPredicate[0], 150);
		
		SortOperator testNationSort = AbstractExtensionFactory.getExtensionFactory().createSortOperator(testTableNationScan, 
				this.theInstance.getQueryHeap(), new DataType[] { nationTable.getSchema().getColumn(0).getDataType(), nationTable.getSchema().getColumn(1).getDataType() }, 
				(int) nationTable.getStatistics().getCardinality(), new int[]{ 0 }, new boolean[]{ true } );

		// join nation and customer to get tuples of (customer_id, nation_name)
		MergeJoinOperator testCustomerNationMGJN = AbstractExtensionFactory.getExtensionFactory().createMergeJoinOperator(
				testNationSort, testNationKeyCustomerTableSort, new int[] { 0 }, new int[] { 1 }, new int[] { -1, 1 }, new int[] { 0, -1 });

		SortOperator testCustomerNationJoinSort = AbstractExtensionFactory.getExtensionFactory().createSortOperator(testCustomerNationMGJN, 
				this.theInstance.getQueryHeap(), new DataType[] { customerTable.getSchema().getColumn(0).getDataType(), nationTable.getSchema().getColumn(1).getDataType() }, 
				(int) ordersTable.getStatistics().getCardinality(), new int[]{ 0 }, new boolean[]{ true } );
		
		TableScanOperator testTableOrdersScan = AbstractExtensionFactory.getExtensionFactory().createTableScanOperator(
				bufferPool, ordersTable.getResourceManager(), ordersTable.getResourceId(),
				ordersColumns, new LowLevelPredicate[0], 150);

		SortOperator testCustomerKeyOrdersTableSort = AbstractExtensionFactory.getExtensionFactory().createSortOperator(testTableOrdersScan, 
				this.theInstance.getQueryHeap(), new DataType[] { ordersTable.getSchema().getColumn(1).getDataType(), ordersTable.getSchema().getColumn(3).getDataType() }, 
				(int) ordersTable.getStatistics().getCardinality(), new int[]{ 0 }, new boolean[]{ true } );
		
		// join orders to get tuples of (totalprice, customer_id, nation_name)
		MergeJoinOperator testCustomerNationOrdersMGJN = AbstractExtensionFactory.getExtensionFactory().createMergeJoinOperator(
				testCustomerKeyOrdersTableSort, testCustomerNationJoinSort, new int[] { 0 }, new int[] { 0 }, new int[] { 1, -1, -1 }, new int[] { -1, 0, 1 });
		
		// sort by region
		SortOperator testCustomerNationOrdersJoinSort = AbstractExtensionFactory.getExtensionFactory().createSortOperator(
				testCustomerNationOrdersMGJN, this.theInstance.getQueryHeap(), new DataType[] { ordersTable.getSchema().getColumn(3).getDataType(), customerTable.getSchema().getColumn(0).getDataType(), nationTable.getSchema().getColumn(1).getDataType() },
				(int) ordersTable.getStatistics().getCardinality(), new int[]{ 2 }, new boolean[]{ true } );

		// group totalprice per region
		OutputColumn.AggregationType[] aggs = new OutputColumn.AggregationType[] { OutputColumn.AggregationType.SUM	};
		DataType[] aggTypes = new DataType[] { DataType.floatType() };
		int[] groupCols = new int[] { 2 };		
		int[] aggColIndices = new int[] { 0 };
		
		GroupByOperator testGroupBy = AbstractExtensionFactory.getExtensionFactory().createGroupByOperator(
				testCustomerNationOrdersJoinSort, groupCols, aggColIndices, aggs, 
				aggTypes, new int[]{ 0, -1 } , new int[]{ -1, 0 });
	
		exercisePlanTests(testGroupBy, REFERENCE_DATA_PATH+"OperatorsIII_testMergeJoin.dat");
	}
	

	/**
	 * Executes the plan containing sorting and checks whether the result is correctly sorted.
	 * 
	 * @param testPlan The plan generating the sorted tuples.
	 * @param sortColumns The columns after which the tuples should be sorted.
	 * @param sortDirection The direction of the sorting for each column (ascending/descending).
	 */
	private static final void exerciseSortedPlanTest(PhysicalPlanOperator testPlan, int[] sortColumns, boolean[] sortDirection)
	throws Exception
	{
		testPlan.open(null);
	
		System.out.println("Starting to produce tuples...");
		DataTuple lastTuple = testPlan.next();
		DataTuple testTuple = null;
		int counter = 1;
		
		while ((testTuple = testPlan.next()) != null)
		{
			// print some info
			counter++;
			if (counter % 1000 == 0) {
				System.out.println(counter + " tuples produced...");
			}
			
			// make sure the earlier tuple is smaller
			for (int i = 0; i < sortColumns.length; i++)
			{
				DataField smaller = lastTuple.getField(sortColumns[i]);
				DataField larger = testTuple.getField(sortColumns[i]);
			
				int comp = smaller.compareTo(larger);
				if (comp == 0) {
					continue;
				}
				else if (sortDirection[i]) {
					if (comp > 0) {
						fail("Tuple is not smaller or equal than the previous tuple");
					}
					else {
						break;
					}
				}
				else {
					if (comp < 0) {
						fail("Tuple is not larger or equal than the previous tuple");
					}
					else {
						break;
					}
				}
			}
			
			lastTuple = testTuple;
		}
		
		testPlan.close();
	}
	
	
	/**
	 * Executes the plans and compares their results with each other.
	 *
	 * @param testPlan The plan of operators to be tested.
	 * @param referenceSolutionPath The path to the reference solution data
	 */
	private static final void exercisePlanTests(PhysicalPlanOperator testPlan, String referenceSolutionPath)
	throws Exception
	{
		List<DataTuple> testTuples = new ArrayList<DataTuple>();
		List<DataTuple> referenceTuples = readFromFile(referenceSolutionPath);
		
		DataTuple tuple = null;
		
		// root is always opened uncorrelated
		System.out.println("Test Plan tuples:");
		testPlan.open(null);
		while ((tuple = testPlan.next()) != null)
		{
			testTuples.add(tuple);
			System.out.println(tuple);
		}
		testPlan.close();
		
		assertTupleListsEqual(testTuples, referenceTuples);
	}
	
	
	/**
	 * Checks that two given lists of tuples are equal.
	 * 
	 * @param testTuples The tuples of the operators to test.
	 * @param referenceTuples The tuples of the reference operators.
	 */
	private static final void assertTupleListsEqual(List<DataTuple> testTuples, List<DataTuple> referenceTuples)
	{
		if (testTuples.size() != referenceTuples.size()) {
			fail("Test and Reference Operators produced a different tuple count.");
		}
		
		for (int i = 0; i < testTuples.size(); i++) {
			assertTuplesEqualWithTolerance(testTuples.get(i), referenceTuples.get(i));
		}
	}
	
	/**
	 * Checks whether two given tuples are equal.
	 * 
	 * @param t1 The first tuple to check.
	 * @param t2 The second tuple to check.
	 */
	private static final void assertTuplesEqualWithTolerance(DataTuple t1, DataTuple t2)
	{
		if (t1.getNumberOfFields() != t2.getNumberOfFields()) {
			fail("Test and Reference Operators produced tuples with different number of columns.");
		}
		
		// check all fields
		for (int i = 0; i < t1.getNumberOfFields(); i++) {
			DataField f1 = t1.getField(i);
			DataField f2 = t2.getField(i);
			BasicType dt1 = f1.getBasicType();
			BasicType dt2 = f2.getBasicType();
			
			assertTrue("Tuples and reference tuples have columns of different data types.", dt1 == dt2);
			
			if (dt1 == DataType.floatType().getBasicType()) {
				assertTrue("Float result fields not close enough within tolerance.",
						floatsWithinTolerance((FloatField) f1, (FloatField) f2)); 
			}
			else if (dt1 == DataType.doubleType().getBasicType()) {
				assertTrue("Double result fields not close enough within tolerance.",
						doublesWithinTolerance((DoubleField) f1, (DoubleField) f2));
			}
			else {
				assertTrue("Tuples have an unequal field.", f1.equals(f2));
			}
		}
	}
	
	/**
	 * Checks whether the difference between two float values is within the
	 * acceptable tolerance.
	 * 
	 * @param f1 First float value to compare.
	 * @param f2 Second float value to compare.
	 * @return True, if the two float values are within tolerance, false otherwise.
	 */
	private static final boolean floatsWithinTolerance(FloatField f1, FloatField f2)
	{
		if (f1.isNULL()) {
			return f2.isNULL();
		}
		else {
			float absDiff = Math.abs(f1.getValue() - f2.getValue());
			return  absDiff <= Math.abs(f1.getValue() / 100.0f);
		}
	}
	
	/**
	 * Checks whether the difference between two double values is within the
	 * acceptable tolerance.
	 * 
	 * @param f1 First double value to compare.
	 * @param f2 Second double value to compare.
	 * @return True, if the two double values are within tolerance, false otherwise.
	 */
	private static final boolean doublesWithinTolerance(DoubleField f1, DoubleField f2)
	{
		if (f1.isNULL()) {
			return f2.isNULL();
		}
		else {
			double absDiff = Math.abs(f1.getValue() - f2.getValue());
			return  absDiff <= Math.abs(f1.getValue() / 100.0f);
		}
	}
	
	/**
	 * Writes a list of tuples to a file.
	 * You can use this method to load previously stored outputs.
	 * 
	 * @param tuples The tuples to write to the output file
	 * @param path The path of the output file
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 */
	private static final List<DataTuple> readFromFile(String path) throws IOException, ClassNotFoundException{
		FileInputStream fis = new FileInputStream(path);
		ObjectInputStream ois = new ObjectInputStream(fis);
		@SuppressWarnings("unchecked")
		List<DataTuple> result = (List<DataTuple>) ois.readObject();
		ois.close();
		return result;
	}
}
