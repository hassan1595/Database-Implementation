package de.tuberlin.dima.minidb.test.optimizer.cost;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import de.tuberlin.dima.minidb.DBInstance;
import de.tuberlin.dima.minidb.api.AbstractExtensionFactory;
import de.tuberlin.dima.minidb.catalogue.ColumnSchema;
import de.tuberlin.dima.minidb.catalogue.IndexDescriptor;
import de.tuberlin.dima.minidb.catalogue.TableDescriptor;
import de.tuberlin.dima.minidb.optimizer.cost.CostEstimator;
import de.tuberlin.dima.minidb.semantics.BaseTableAccess;
import de.tuberlin.dima.minidb.semantics.Column;
import de.tuberlin.dima.minidb.semantics.Relation;

public class TestCostEstimatorStudents
{
	/**
	 * Initial seed for the random number generator.
	 */
	private static final long SEED = 458934897214L;

	/**
	 * The default time (nanoseconds) that is needed to transfer a block of the
	 * default block size from secondary storage to main memory.
	 */
	private static final long READ_COST = 40;

	/**
	 * The default time (nanoseconds) that is needed to transfer a block of the
	 * default block size from main memory to secondary storage.
	 */
	private static final long WRITE_COST = 40;

	/**
	 * The overhead for a single block read operation if the block is not part
	 * of a sequence that is read. For magnetic disks, that would correspond to
	 * seek time + rotational latency.
	 */
	private static final long RANDOM_READ_OVERHEAD = 5000;

	/**
	 * The overhead for a single block write if the block is not part of a
	 * sequence that is written. For magnetic disks, that would correspond to
	 * seek time + rotational latency.
	 */
	private static final long RANDOM_WRITE_OVERHEAD = 5000;

	/**
	 * Location of the config file for the database instance.
	 */
	protected final String configFile = "/config.xml";

	/**
	 * Location of the catalogue file for the database instance.
	 */
	protected final String catalogueFile = "/catalogue.xml";

	/**
	 * The database instance to perform the tests on.
	 */
	protected DBInstance dbInstance = null;

	/**
	 * The random number generator to be used in the unit tests.
	 */
	protected Random random = new Random();

	/**
	 * The subject under test.
	 */
	protected CostEstimator estimator;


	// ------------------------------------------------------------------------

	@BeforeClass
	public static void initExtensionFactory() throws Exception
	{
		// initialize the extension factory to have access to the user methods
		AbstractExtensionFactory.initializeDefault();
	}


	@Before
	public void setUp() throws Exception
	{
		// initialize a database instance and start it
		this.dbInstance = new DBInstance(this.configFile, this.catalogueFile);
		int returncode = this.dbInstance.startInstance();
		if (returncode != DBInstance.RETURN_CODE_OKAY)
		{
			throw new Exception("DBInstance could not be started." + returncode);
		}

		this.random.setSeed(SEED);
		this.estimator = AbstractExtensionFactory.getExtensionFactory().createCostEstimator(READ_COST, WRITE_COST, RANDOM_READ_OVERHEAD, RANDOM_WRITE_OVERHEAD);
	}


	@After
	public void tearDown() throws Exception
	{
		// check if instance is running
		if (this.dbInstance != null && this.dbInstance.isRunning())
		{
			// stop running instance
			this.dbInstance.shutdownInstance(false);
		}
	}


	// ------------------------------------------------------------------------

	@Test
	public void testComputeTableScanCosts()
	{
		Map<String, Long> expectedResults = new HashMap<String, Long>();
		expectedResults.put("REGION", 5040L);
		expectedResults.put("NATION", 5080L);
		expectedResults.put("PART", 15840L);
		expectedResults.put("SUPPLIER", 5600L);
		expectedResults.put("PARTSUPPLIER", 50920L);
		expectedResults.put("ORDERS", 63920L);

		for (Map.Entry<String, Long> entry : expectedResults.entrySet())
		{
			String tableName = entry.getKey();
			TableDescriptor table = this.dbInstance.getCatalogue().getTable(tableName);
			Long expCost = entry.getValue();

			Assert.assertEquals("Bad cost estimate for table scan (" + tableName + ");", expCost.longValue(), this.estimator.computeTableScanCosts(table));
		}
	}


	@Test
	public void testComputeIndexLookupCosts()
	{
		Map<String, Collection<IndexLookupFixture>> fixtures = new HashMap<String, Collection<IndexLookupFixture>>();
		// PART:PART_PK
		fixtures.put("PART:PART_PK", new ArrayList<IndexLookupFixture>());
		fixtures.get("PART:PART_PK").add(new IndexLookupFixture(3480L, 5840L));
		fixtures.get("PART:PART_PK").add(new IndexLookupFixture(3160L, 5760L));
		fixtures.get("PART:PART_PK").add(new IndexLookupFixture(2280L, 5560L));
		fixtures.get("PART:PART_PK").add(new IndexLookupFixture(2640L, 5640L));
		fixtures.get("PART:PART_PK").add(new IndexLookupFixture(3680L, 5880L));
		fixtures.get("PART:PART_PK").add(new IndexLookupFixture(2480L, 5600L));
		fixtures.get("PART:PART_PK").add(new IndexLookupFixture(3960L, 5920L));
		fixtures.get("PART:PART_PK").add(new IndexLookupFixture(1600L, 5400L));
		fixtures.get("PART:PART_PK").add(new IndexLookupFixture(3120L, 5720L));
		fixtures.get("PART:PART_PK").add(new IndexLookupFixture(3320L, 5800L));
		// CUSTOMER:CUSTOMER_PK
		fixtures.put("CUSTOMER:CUSTOMER_PK", new ArrayList<IndexLookupFixture>());
		fixtures.get("CUSTOMER:CUSTOMER_PK").add(new IndexLookupFixture(1050L, 5240L));
		fixtures.get("CUSTOMER:CUSTOMER_PK").add(new IndexLookupFixture(1709L, 5400L));
		fixtures.get("CUSTOMER:CUSTOMER_PK").add(new IndexLookupFixture(2010L, 5480L));
		fixtures.get("CUSTOMER:CUSTOMER_PK").add(new IndexLookupFixture(2970L, 5680L));
		fixtures.get("CUSTOMER:CUSTOMER_PK").add(new IndexLookupFixture(1560L, 5360L));
		fixtures.get("CUSTOMER:CUSTOMER_PK").add(new IndexLookupFixture(2550L, 5600L));
		fixtures.get("CUSTOMER:CUSTOMER_PK").add(new IndexLookupFixture(2280L, 5520L));
		fixtures.get("CUSTOMER:CUSTOMER_PK").add(new IndexLookupFixture(2040L, 5480L));
		fixtures.get("CUSTOMER:CUSTOMER_PK").add(new IndexLookupFixture(2460L, 5560L));
		fixtures.get("CUSTOMER:CUSTOMER_PK").add(new IndexLookupFixture(930L, 5240L));
		// LINEITEM:LINEITEM_FK_PART
		fixtures.put("LINEITEM:LINEITEM_FK_PART", new ArrayList<IndexLookupFixture>());
		fixtures.get("LINEITEM:LINEITEM_FK_PART").add(new IndexLookupFixture(115694L, 24000L));
		fixtures.get("LINEITEM:LINEITEM_FK_PART").add(new IndexLookupFixture(96412L, 20840L));
		fixtures.get("LINEITEM:LINEITEM_FK_PART").add(new IndexLookupFixture(78334L, 17880L));
		fixtures.get("LINEITEM:LINEITEM_FK_PART").add(new IndexLookupFixture(62667L, 15280L));
		fixtures.get("LINEITEM:LINEITEM_FK_PART").add(new IndexLookupFixture(104848L, 22200L));
		fixtures.get("LINEITEM:LINEITEM_FK_PART").add(new IndexLookupFixture(51821L, 13520L));
		fixtures.get("LINEITEM:LINEITEM_FK_PART").add(new IndexLookupFixture(43385L, 12120L));
		fixtures.get("LINEITEM:LINEITEM_FK_PART").add(new IndexLookupFixture(86770L, 19240L));
		fixtures.get("LINEITEM:LINEITEM_FK_PART").add(new IndexLookupFixture(63872L, 15480L));
		fixtures.get("LINEITEM:LINEITEM_FK_PART").add(new IndexLookupFixture(120515L, 24760L));

		for (Map.Entry<String, Collection<IndexLookupFixture>> entry : fixtures.entrySet())
		{
			String key = entry.getKey();

			String tableName = key.substring(0, key.indexOf(':'));
			String indexName = key.substring(key.indexOf(':') + 1);

			TableDescriptor table = this.dbInstance.getCatalogue().getTable(tableName);
			IndexDescriptor index = this.dbInstance.getCatalogue().getIndex(indexName);

			for (IndexLookupFixture fixture : entry.getValue())
			{
				Assert.assertEquals("Bad cost estimate for index lookup (" + key + ");", fixture.expCost, this.estimator.computeIndexLookupCosts(index, table, fixture.cardinality));
			}

//			System.out.println(key);
//			for (int i = 0; i < 10; i++)
//			{
//				// random cardinality in the [30%,100%] full cardinality
//				// interval
//				long cardinality = new Double(((30.0 + random.nextInt(71)) / 100.0) * table.getStatistics().getCardinality()).longValue();
//
//				System.out.println("fixtures.get(\"" + key + "\").add(new IndexLookupFixture(" + cardinality + "L, " + estimator.computeIndexLookupCosts(index, table, cardinality) + "L));");
//			}
		}
	}


	@Test
	public void testComputeSortCosts()
	{
		Map<String, Collection<SortFixture>> fixtures = new HashMap<String, Collection<SortFixture>>();
		// PARTSUPPLIER
		fixtures.put("PARTSUPPLIER", new ArrayList<SortFixture>());
		fixtures.get("PARTSUPPLIER").add(new SortFixture(13L, 15360L, 33320L));
		fixtures.get("PARTSUPPLIER").add(new SortFixture(12L, 5760L, 12552L));
		fixtures.get("PARTSUPPLIER").add(new SortFixture(18L, 5600L, 151792L));
		fixtures.get("PARTSUPPLIER").add(new SortFixture(26L, 13600L, 361360L));
		fixtures.get("PARTSUPPLIER").add(new SortFixture(4L, 12640L, 16328L));
		fixtures.get("PARTSUPPLIER").add(new SortFixture(16L, 14720L, 390624L));
		fixtures.get("PARTSUPPLIER").add(new SortFixture(19L, 6400L, 172560L));
		fixtures.get("PARTSUPPLIER").add(new SortFixture(23L, 9600L, 271208L));
		fixtures.get("PARTSUPPLIER").add(new SortFixture(21L, 5120L, 139048L));
		fixtures.get("PARTSUPPLIER").add(new SortFixture(17L, 12160L, 323600L));
		// PART
		fixtures.put("PART", new ArrayList<SortFixture>());
		fixtures.get("PART").add(new SortFixture(461L, 2200L, 38040L));
		fixtures.get("PART").add(new SortFixture(126L, 2280L, 55976L));
		fixtures.get("PART").add(new SortFixture(507L, 1600L, 42760L));
		fixtures.get("PART").add(new SortFixture(106L, 2680L, 41816L));
		fixtures.get("PART").add(new SortFixture(351L, 3440L, 100344L));
		fixtures.get("PART").add(new SortFixture(285L, 2080L, 38512L));
		fixtures.get("PART").add(new SortFixture(167L, 3360L, 51256L));
		fixtures.get("PART").add(new SortFixture(340L, 1560L, 29544L));
		fixtures.get("PART").add(new SortFixture(72L, 1480L, 13024L));
		fixtures.get("PART").add(new SortFixture(70L, 1920L, 33792L));
		// SUPPLIER
		fixtures.put("SUPPLIER", new ArrayList<SortFixture>());
		fixtures.get("SUPPLIER").add(new SortFixture(111L, 170L, 9248L));
		fixtures.get("SUPPLIER").add(new SortFixture(125L, 176L, 9248L));
		fixtures.get("SUPPLIER").add(new SortFixture(45L, 180L, 6416L));
		fixtures.get("SUPPLIER").add(new SortFixture(47L, 164L, 6888L));
		fixtures.get("SUPPLIER").add(new SortFixture(94L, 128L, 8304L));
		fixtures.get("SUPPLIER").add(new SortFixture(20L, 150L, 6416L));
		fixtures.get("SUPPLIER").add(new SortFixture(2L, 200L, 5944L));
		fixtures.get("SUPPLIER").add(new SortFixture(2L, 138L, 5472L));
		fixtures.get("SUPPLIER").add(new SortFixture(39L, 200L, 7360L));
		fixtures.get("SUPPLIER").add(new SortFixture(32L, 160L, 5472L));

		for (Map.Entry<String, Collection<SortFixture>> entry : fixtures.entrySet())
		{
			String tableName = entry.getKey();

			TableDescriptor table = this.dbInstance.getCatalogue().getTable(tableName);

			for (SortFixture fixture : entry.getValue())
			{
				Assert.assertEquals("Bad cost estimate for index lookup (" + tableName + ");", fixture.expCost, this.estimator.computeSortCosts(fixture.getColumns(table), fixture.cardinality));
			}

//			System.out.println(tableName);
//			for (int i = 0; i < 10; i++)
//			{
//				int numberOfColumns = table.getStatistics().getNumberOfColumns();
//				long cardinality = new Double(((30.0 + random.nextInt(71)) / 100.0) * table.getStatistics().getCardinality()).longValue();
//				long numOfColumns = new Double(((20.0 + random.nextInt(81)) / 100.0) * numberOfColumns).longValue();
//
//				// fetch random column indexes
//				Set<Integer> columnIndexes = new HashSet<Integer>();
//				while (columnIndexes.size() < numOfColumns)
//				{
//					columnIndexes.add(random.nextInt(numberOfColumns));
//				}
//
//				Relation relation = new BaseTableAccess(table);
//				Collection<Column> columns = new ArrayList<Column>();
//
//				long colBitmap = 0;
//				for (Integer j : columnIndexes)
//				{
//					ColumnSchema colSchema = table.getSchema().getColumn(j);
//					columns.add(new Column(relation, colSchema.getDataType(), j));
//
//					colBitmap |= 0x1L << j;
//				}
//
//				long expCosts = estimator.computeSortCosts(columns.toArray(new Column[] {}), cardinality);
//
//				System.out.println("fixtures.get(\"" + tableName + "\").add(new SortFixture(" + colBitmap + "L, " + cardinality + "L, " + expCosts + "L));");
//			}
		}
	}
	
	@Test
	public void testComputeFetchCosts()
	{
		Map<String, Collection<FetchFixture>> fixtures = new HashMap<String, Collection<FetchFixture>>();
		// PART
		fixtures.put("CUSTOMER", new ArrayList<FetchFixture>());
		fixtures.get("CUSTOMER").add(new FetchFixture(2400L, false, 4158343L));
		fixtures.get("CUSTOMER").add(new FetchFixture(1560L, false, 2988487L));
		fixtures.get("CUSTOMER").add(new FetchFixture(1290L, true, 174051L));
		fixtures.get("CUSTOMER").add(new FetchFixture(2160L, false, 3839214L));
		fixtures.get("CUSTOMER").add(new FetchFixture(3000L, true, 170500L));
		fixtures.get("CUSTOMER").add(new FetchFixture(1709L, true, 170500L));
		fixtures.get("CUSTOMER").add(new FetchFixture(2970L, true, 170500L));
		fixtures.get("CUSTOMER").add(new FetchFixture(2550L, false, 4352547L));
		fixtures.get("CUSTOMER").add(new FetchFixture(2040L, true, 170500L));
		fixtures.get("CUSTOMER").add(new FetchFixture(930L, false, 1986012L));
		// SUPPLIER
		fixtures.put("PARTSUPPLIER", new ArrayList<FetchFixture>());
		fixtures.get("PARTSUPPLIER").add(new FetchFixture(12640L, false, 21226959L));
		fixtures.get("PARTSUPPLIER").add(new FetchFixture(10560L, true, 766196L));
		fixtures.get("PARTSUPPLIER").add(new FetchFixture(9920L, false, 17514452L));
		fixtures.get("PARTSUPPLIER").add(new FetchFixture(6400L, true, 781812L));
		fixtures.get("PARTSUPPLIER").add(new FetchFixture(13280L, false, 22068342L));
		fixtures.get("PARTSUPPLIER").add(new FetchFixture(7840L, true, 770112L));
		fixtures.get("PARTSUPPLIER").add(new FetchFixture(8800L, false, 15912144L));
		fixtures.get("PARTSUPPLIER").add(new FetchFixture(10560L, true, 766196L));
		fixtures.get("PARTSUPPLIER").add(new FetchFixture(14880L, true, 766196L));
		fixtures.get("PARTSUPPLIER").add(new FetchFixture(14240L, true, 766196L));
		// ORDERS
		fixtures.put("NATION", new ArrayList<FetchFixture>());
		fixtures.get("NATION").add(new FetchFixture(12L, true, 2852L));
		fixtures.get("NATION").add(new FetchFixture(13L, false, 28495L));
		fixtures.get("NATION").add(new FetchFixture(16L, false, 31381L));
		fixtures.get("NATION").add(new FetchFixture(13L, true, 2852L));
		fixtures.get("NATION").add(new FetchFixture(18L, false, 32950L));
		fixtures.get("NATION").add(new FetchFixture(14L, true, 2852L));
		fixtures.get("NATION").add(new FetchFixture(17L, false, 32198L));
		fixtures.get("NATION").add(new FetchFixture(22L, true, 2852L));
		fixtures.get("NATION").add(new FetchFixture(14L, true, 2852L));
		fixtures.get("NATION").add(new FetchFixture(10L, false, 24832L));
		
		

		for (Map.Entry<String, Collection<FetchFixture>> entry : fixtures.entrySet())
		{
			String tableName = entry.getKey();

			TableDescriptor table = this.dbInstance.getCatalogue().getTable(tableName);

			for (FetchFixture fixture : entry.getValue())
			{
				Assert.assertEquals("Bad cost estimate for fetch (" + tableName + ");", fixture.expCost, this.estimator.computeFetchCosts(table, fixture.cardinality, fixture.sequential));
			}
			
//			System.out.println(tableName);
//			for (int i = 0; i < 10; i++)
//			{
//				boolean sequential = random.nextBoolean();
//				long cardinality = new Double(((30.0 + random.nextInt(71)) / 100.0) * table.getStatistics().getCardinality()).longValue();
//
//				long expCosts = estimator.computeFetchCosts(table, cardinality, sequential);
//
//				System.out.println("fixtures.get(\"" + tableName + "\").add(new FetchFixture(" + cardinality + "L, " + sequential + ", " + expCosts + "L));");
//			}
		}
	}
	
	@Test
	public void testComputeFilterCost()
	{
		Assert.assertEquals("Bad cost estimate for filter;", 0L, this.estimator.computeFilterCost(null, this.random.nextLong()));
	}

	@Test
	public void testComputeMergeJoinCost()
	{
		Assert.assertEquals("Bad cost estimate for merge join;", 0L, this.estimator.computeMergeJoinCost());
	}
	
	@Test
	public void testComputeNestedLoopJoinCost()
	{
		Map<String, Collection<NestedLoopJoinFixture>> fixtures = new HashMap<String, Collection<NestedLoopJoinFixture>>();
		// PART
		fixtures.put("PART", new ArrayList<NestedLoopJoinFixture>());
		fixtures.get("PART").add(new NestedLoopJoinFixture(2440L, 780L, 1903200L));
		fixtures.get("PART").add(new NestedLoopJoinFixture(3000L, 854L, 2562000L));
		fixtures.get("PART").add(new NestedLoopJoinFixture(1600L, 807L, 1291200L));
		fixtures.get("PART").add(new NestedLoopJoinFixture(1280L, 853L, 1091840L));
		fixtures.get("PART").add(new NestedLoopJoinFixture(2520L, 734L, 1849680L));
		fixtures.get("PART").add(new NestedLoopJoinFixture(2440L, 532L, 1298080L));
		fixtures.get("PART").add(new NestedLoopJoinFixture(3040L, 927L, 2818080L));
		fixtures.get("PART").add(new NestedLoopJoinFixture(3280L, 560L, 1836800L));
		fixtures.get("PART").add(new NestedLoopJoinFixture(3400L, 642L, 2182800L));
		fixtures.get("PART").add(new NestedLoopJoinFixture(3880L, 722L, 2801360L));
		// SUPPLIER
		fixtures.put("SUPPLIER", new ArrayList<NestedLoopJoinFixture>());
		fixtures.get("SUPPLIER").add(new NestedLoopJoinFixture(174L, 623L, 108402L));
		fixtures.get("SUPPLIER").add(new NestedLoopJoinFixture(113L, 942L, 106446L));
		fixtures.get("SUPPLIER").add(new NestedLoopJoinFixture(184L, 670L, 123280L));
		fixtures.get("SUPPLIER").add(new NestedLoopJoinFixture(198L, 758L, 150084L));
		fixtures.get("SUPPLIER").add(new NestedLoopJoinFixture(156L, 736L, 114816L));
		fixtures.get("SUPPLIER").add(new NestedLoopJoinFixture(184L, 551L, 101384L));
		fixtures.get("SUPPLIER").add(new NestedLoopJoinFixture(64L, 540L, 34560L));
		fixtures.get("SUPPLIER").add(new NestedLoopJoinFixture(120L, 924L, 110880L));
		fixtures.get("SUPPLIER").add(new NestedLoopJoinFixture(156L, 981L, 153036L));
		fixtures.get("SUPPLIER").add(new NestedLoopJoinFixture(150L, 754L, 113100L));
		// ORDERS
		fixtures.put("ORDERS", new ArrayList<NestedLoopJoinFixture>());
		fixtures.get("ORDERS").add(new NestedLoopJoinFixture(28800L, 582L, 16761600L));
		fixtures.get("ORDERS").add(new NestedLoopJoinFixture(19500L, 760L, 14820000L));
		fixtures.get("ORDERS").add(new NestedLoopJoinFixture(26100L, 997L, 26021700L));
		fixtures.get("ORDERS").add(new NestedLoopJoinFixture(10800L, 928L, 10022400L));
		fixtures.get("ORDERS").add(new NestedLoopJoinFixture(15900L, 913L, 14516700L));
		fixtures.get("ORDERS").add(new NestedLoopJoinFixture(10500L, 504L, 5292000L));
		fixtures.get("ORDERS").add(new NestedLoopJoinFixture(20100L, 899L, 18069900L));
		fixtures.get("ORDERS").add(new NestedLoopJoinFixture(15600L, 689L, 10748400L));
		fixtures.get("ORDERS").add(new NestedLoopJoinFixture(22800L, 754L, 17191200L));
		fixtures.get("ORDERS").add(new NestedLoopJoinFixture(24600L, 711L, 17490600L));
		
		TableDescriptor partsuppTable = this.dbInstance.getCatalogue().getTable("LINEITEM");
		BaseTableAccess innerRoot = new BaseTableAccess(partsuppTable);

		for (Map.Entry<String, Collection<NestedLoopJoinFixture>> entry : fixtures.entrySet())
		{
			String tableName = entry.getKey();

			for (NestedLoopJoinFixture fixture : entry.getValue())
			{
				innerRoot.setCumulativeCosts(fixture.innerCumulativeCosts);
				Assert.assertEquals("Bad cost estimate for nested loop join (inner relation scans " + tableName + ");", fixture.expCost, this.estimator.computeNestedLoopJoinCost(fixture.outerCardinality, innerRoot));
			}

//			System.out.println(tableName);
//			TableDescriptor table = dbInstance.getCatalogue().getTable(tableName);
//			for (int i = 0; i < 10; i++)
//			{
//				innerRoot.setCumulativeCosts(random.nextInt(500)+500);
//				long outerCardinality = new Double(((30.0 + random.nextInt(71)) / 100.0) * table.getStatistics().getCardinality()).longValue();
//
//				long expCosts = estimator.computeNestedLoopJoinCost(outerCardinality, innerRoot);
//
//				System.out.println("fixtures.get(\"" + tableName + "\").add(new NestedLoopJoinFixture(" + outerCardinality + "L, " + innerRoot.getCumulativeCosts() + "L, " + expCosts + "L));");
//			}
		}
	}
	
	/* *************************************************************************
	 * additional fixture structures
	 * ************************************************************************/

	private final class IndexLookupFixture
	{
		public final long cardinality;
		public final long expCost;


		public IndexLookupFixture(long cardinality, long expCost)
		{
			this.cardinality = cardinality;
			this.expCost = expCost;
		}
	}

	private final class SortFixture
	{
		public final long colBitmap;
		public final long cardinality;
		public final long expCost;


		public SortFixture(long colBitmap, long cardinality, long expCost)
		{
			this.colBitmap = colBitmap;
			this.cardinality = cardinality;
			this.expCost = expCost;
		}


		public Column[] getColumns(TableDescriptor table)
        {
			Relation relation = new BaseTableAccess(table);
			Collection<Column> columns = new ArrayList<Column>();
			
			for(int i = 0; i < 32; i++)
			{
				if ((0x1L << i & this.colBitmap) != 0)
				{
					ColumnSchema colSchema = table.getSchema().getColumn(i);
					columns.add(new Column(relation, colSchema.getDataType(), i));
				}
			}
			
	        return columns.toArray(new Column[] {});
        }
	}
	
	private final class FetchFixture
	{
		public final long cardinality;
		public final boolean sequential;
		public final long expCost;
		
		public FetchFixture(long outerCardinality, boolean sequential, long expCost)
		{
			this.cardinality = outerCardinality;
			this.sequential = sequential;
			this.expCost = expCost;
		}
	}
	
	private final class NestedLoopJoinFixture
	{
		public final long outerCardinality;
		public final long innerCumulativeCosts;
		public final long expCost;

		public NestedLoopJoinFixture(long outerCardinality, long innerCumulativeCosts, long expCost)
		{
			this.outerCardinality = outerCardinality;
			this.innerCumulativeCosts = innerCumulativeCosts;
			this.expCost = expCost;
		}
	}
}
