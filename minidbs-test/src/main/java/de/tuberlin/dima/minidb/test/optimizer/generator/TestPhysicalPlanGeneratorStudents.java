package de.tuberlin.dima.minidb.test.optimizer.generator;


import de.tuberlin.dima.minidb.semantics.predicate.JoinPredicate;
import de.tuberlin.dima.minidb.semantics.predicate.JoinPredicateAtom;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import de.tuberlin.dima.minidb.Config;
import de.tuberlin.dima.minidb.DBInstance;
import de.tuberlin.dima.minidb.api.AbstractExtensionFactory;
import de.tuberlin.dima.minidb.catalogue.IndexDescriptor;
import de.tuberlin.dima.minidb.core.BasicType;
import de.tuberlin.dima.minidb.core.CharField;
import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataType;
import de.tuberlin.dima.minidb.core.FloatField;
import de.tuberlin.dima.minidb.core.IntField;
import de.tuberlin.dima.minidb.optimizer.FetchPlanOperator;
import de.tuberlin.dima.minidb.optimizer.FilterPlanOperator;
import de.tuberlin.dima.minidb.optimizer.GroupByPlanOperator;
import de.tuberlin.dima.minidb.optimizer.IndexLookupPlanOperator;
import de.tuberlin.dima.minidb.optimizer.MergeJoinPlanOperator;
import de.tuberlin.dima.minidb.optimizer.NestedLoopJoinPlanOperator;
import de.tuberlin.dima.minidb.optimizer.Optimizer;
import de.tuberlin.dima.minidb.optimizer.OptimizerException;
import de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator;
import de.tuberlin.dima.minidb.optimizer.SortPlanOperator;
import de.tuberlin.dima.minidb.optimizer.TableScanPlanOperator;
import de.tuberlin.dima.minidb.optimizer.cardinality.CardinalityEstimator;
import de.tuberlin.dima.minidb.optimizer.cost.CostEstimator;
import de.tuberlin.dima.minidb.optimizer.generator.util.PhysicalPlanCostUpdater;
import de.tuberlin.dima.minidb.parser.OutputColumn;
import de.tuberlin.dima.minidb.parser.ParseException;
import de.tuberlin.dima.minidb.parser.Predicate;
import de.tuberlin.dima.minidb.parser.SQLTokenizer;
import de.tuberlin.dima.minidb.parser.SelectQuery;
import de.tuberlin.dima.minidb.parser.solution.SQLParserImpl;
import de.tuberlin.dima.minidb.semantics.AnalyzedSelectQuery;
import de.tuberlin.dima.minidb.semantics.BaseTableAccess;
import de.tuberlin.dima.minidb.semantics.Column;
import de.tuberlin.dima.minidb.semantics.ProducedColumn;
import de.tuberlin.dima.minidb.semantics.Relation;
import de.tuberlin.dima.minidb.semantics.predicate.LocalPredicateAtom;
import de.tuberlin.dima.minidb.semantics.predicate.LocalPredicateBetween;
import de.tuberlin.dima.minidb.semantics.solution.SelectQueryAnalyzerImpl;
import de.tuberlin.dima.minidb.test.optimizer.generator.util.PhysicalPlanVerifier;
import de.tuberlin.dima.minidb.test.optimizer.generator.util.PhysicalPlanVerifierException;


public class TestPhysicalPlanGeneratorStudents
{
	/**
	 * Location of the config file for the database instance.
	 */
	protected final String configFile = "/config.xml";

	/**
	 * Location of the catalogue file for the database instance.
	 */
	protected final String catalogueFile = "/catalogue.xml";
	
	/**
	 * The cardinality estimator for local an join cardinalities.
	 */
	protected CardinalityEstimator cardEstimator;

	/**
	 * The database instance to perform the tests on.
	 */
	protected DBInstance dbInstance = null;

	/**
	 * The database instance to perform the tests on.
	 */
	protected Config config = null;
	
	/**
	 * The optimizer used by this test.
	 */
	protected Optimizer optimizer;

	// ------------------------------------------------------------------------
	
	@BeforeClass
	public static void initExtensionFactory() throws Exception {
		// initialize the extension factory to have access to the user methods
		AbstractExtensionFactory.initializeDefault();
	}
	
	@Before
	public void setUp() throws Exception {
		// initialize a database instance and start it
		this.dbInstance = new DBInstance(this.configFile, this.catalogueFile);
		int returncode = this.dbInstance.startInstance(); 
		if (returncode != DBInstance.RETURN_CODE_OKAY) {
			throw new Exception("DBInstance could not be started." + returncode);
		}
		
		this.config = this.dbInstance.getConfig();
		this.cardEstimator = AbstractExtensionFactory.getExtensionFactory().createCardinalityEstimator();
		this.optimizer = new Optimizer(this.dbInstance.getCatalogue(),
			this.config.getBlockReadCost(), this.config.getBlockWriteCost(),
			this.config.getBlockRandomReadOverhead(), this.config.getBlockRandomWriteOverhead());
	}

	@After
	public void tearDown() throws Exception {
		// check if instance is running
		if (this.dbInstance != null && this.dbInstance.isRunning()) {
			// stop running instance
			this.dbInstance.shutdownInstance(false);
		}
	}
	
	/**
	 * Simple point query on an indexed column.
	 */
	@Test
	public void testOptimizerSimplePointQuery() throws Exception
	{
		// --------------------------------------------------------------------
		//                         Prepare the query
		// --------------------------------------------------------------------
		String sql =" SELECT " +
					"	li.l_partkey AS l_partkey, li.l_suppkey AS l_suppkey, li.l_quantity AS l_quantity" +
					" FROM " +
					"	lineitem li " +
					" WHERE " +
					"	li.l_partkey = 100 ";
		
		// parse the query
		SQLParserImpl parser = new SQLParserImpl(sql);
		SelectQuery selectQuery = (SelectQuery) parser.parse();		
		SelectQueryAnalyzerImpl semAna = new SelectQueryAnalyzerImpl();
		AnalyzedSelectQuery query = semAna.analyzeQuery(selectQuery, this.dbInstance.getCatalogue());

		// --------------------------------------------------------------------
		//                       Call the optimizer code
		// --------------------------------------------------------------------
		
		OptimizerPlanOperator actualPlanRoot = this.optimizer.createSelectQueryPlan(query);

		// --------------------------------------------------------------------
		//                       Verify the physical plan
		// --------------------------------------------------------------------
		
		try
		{
			PhysicalPlanVerifier planVerifier = new PhysicalPlanVerifier();
			planVerifier.verify(getOptimizerSimplePointQueryReferencePlan(), actualPlanRoot);
		}
		catch(PhysicalPlanVerifierException e)
		{
			e.dumpMessage();
			Assert.fail();
		}
	}
	
	/**
	 * Simple range query on an indexed column, but with high selectivity.
	 */
	@Test
	public void testOptimizerSimpleRangeQuery() throws Exception
	{
		// --------------------------------------------------------------------
		//                         Prepare the query
		// --------------------------------------------------------------------
		String sql =" SELECT " +
					"	li.l_partkey AS l_partkey, li.l_suppkey AS l_suppkey, li.l_quantity AS l_quantity" +
					" FROM " +
					"	lineitem li " +
					" WHERE " +
					"	li.l_suppkey > 100 AND li.l_suppkey < 10000";
		
		// parse the query
		SQLParserImpl parser = new SQLParserImpl(sql);
		SelectQuery selectQuery = (SelectQuery) parser.parse();		
		SelectQueryAnalyzerImpl semAna = new SelectQueryAnalyzerImpl();
		AnalyzedSelectQuery query = semAna.analyzeQuery(selectQuery, this.dbInstance.getCatalogue());

		// --------------------------------------------------------------------
		//                       Call the optimizer code
		// --------------------------------------------------------------------
		
		OptimizerPlanOperator actualPlanRoot = this.optimizer.createSelectQueryPlan(query);

		// --------------------------------------------------------------------
		//                       Verify the physical plan
		// --------------------------------------------------------------------
		
		try
		{
			PhysicalPlanVerifier planVerifier = new PhysicalPlanVerifier();
			planVerifier.verify(getOptimizerRangeQueryReferencePlan(), actualPlanRoot);
		}
		catch(PhysicalPlanVerifierException e)
		{
			e.dumpMessage();
			Assert.fail();
		}
	}
	
	/**
	 * Simple range query on an indexed column, but with high selectivity.
	 */
	@Test
	public void testOptimizerSimpleGroupingQuery() throws Exception
	{
		// --------------------------------------------------------------------
		//                         Prepare the query
		// --------------------------------------------------------------------
		String sql =" SELECT " +
					"	ps.ps_partkey AS ps_partkey, SUM(ps.ps_availqty) as ps_availqty " +
					" FROM " +
					"	partsupplier ps " +
					" GROUP BY ps.ps_partkey";
		
		// parse the query
		SQLParserImpl parser = new SQLParserImpl(sql);
		SelectQuery selectQuery = (SelectQuery) parser.parse();		
		SelectQueryAnalyzerImpl semAna = new SelectQueryAnalyzerImpl();
		AnalyzedSelectQuery query = semAna.analyzeQuery(selectQuery, this.dbInstance.getCatalogue());

		// --------------------------------------------------------------------
		//                       Call the optimizer code
		// --------------------------------------------------------------------
		
		OptimizerPlanOperator actualPlanRoot = this.optimizer.createSelectQueryPlan(query);

		// --------------------------------------------------------------------
		//                       Verify the physical plan
		// --------------------------------------------------------------------
		
		try
		{
			PhysicalPlanVerifier planVerifier = new PhysicalPlanVerifier();
			planVerifier.verify(getOptimizerSimpleGroupingQueryReferencePlan(), actualPlanRoot);
		}
		catch(PhysicalPlanVerifierException e)
		{
			e.dumpMessage();
			Assert.fail();
		}
	}
	
	/**
	 * Simple range query on an indexed column, but with high selectivity.
	 */
	@Test
	public void testOptimizerJoinOf3Query1() throws Exception
	{
		// --------------------------------------------------------------------
		//                         Prepare the query
		// --------------------------------------------------------------------
		String sql =" SELECT " +
					"	p.p_name AS partname, s.s_name as suppname, ps.ps_supplycost as supplycost " +
					" FROM " +
					"	partsupplier ps, part p, supplier s " +
					" WHERE " +
					"   ps.ps_partkey = p.p_partkey AND " +
					"   ps.ps_suppkey = s.s_suppkey AND " +
					"   p.p_partkey >= 1 AND p.p_partkey <= 100";
		
		// parse the query
		SQLParserImpl parser = new SQLParserImpl(sql);
		SelectQuery selectQuery = (SelectQuery) parser.parse();		
		SelectQueryAnalyzerImpl semAna = new SelectQueryAnalyzerImpl();
		AnalyzedSelectQuery query = semAna.analyzeQuery(selectQuery, this.dbInstance.getCatalogue());

		// --------------------------------------------------------------------
		//                       Call the optimizer code
		// --------------------------------------------------------------------
		
		OptimizerPlanOperator actualPlanRoot = this.optimizer.createSelectQueryPlan(query);

		// --------------------------------------------------------------------
		//                       Verify the physical plan
		// --------------------------------------------------------------------
		
		try
		{
			PhysicalPlanVerifier planVerifier = new PhysicalPlanVerifier();
			planVerifier.verify(getOptimizerJoinOf3Query1ReferencePlan(), actualPlanRoot);
		}
		catch(PhysicalPlanVerifierException e)
		{
			e.dumpMessage();
			Assert.fail();
		}
	}
	
	/**
	 * Simple range query on an indexed column, but with high selectivity.
	 */
	@Test
	public void testOptimizerJoinOf3Query2() throws Exception
	{
		// --------------------------------------------------------------------
		//                         Prepare the query
		// --------------------------------------------------------------------
		String sql =" SELECT " +
					"	ps.ps_availqty as ps_availqty, l.l_quantity as l_quantity, o.o_orderdate as o_orderdate " +
					" FROM " +
					"	partsupplier ps, lineitem l, orders o " +
					" WHERE " +
					"   ps.ps_partkey = l.l_partkey AND " +
					"   ps.ps_suppkey = l.l_suppkey AND " +
					"   o.o_orderkey = l.l_orderkey AND " +
					"   o.o_custkey >= 1 AND o.o_custkey <= 5";
		
		// parse the query
		SQLParserImpl parser = new SQLParserImpl(sql);
		SelectQuery selectQuery = (SelectQuery) parser.parse();		
		SelectQueryAnalyzerImpl semAna = new SelectQueryAnalyzerImpl();
		AnalyzedSelectQuery query = semAna.analyzeQuery(selectQuery, this.dbInstance.getCatalogue());

		// --------------------------------------------------------------------
		//                       Call the optimizer code
		// --------------------------------------------------------------------
		
		OptimizerPlanOperator actualPlanRoot = this.optimizer.createSelectQueryPlan(query);

		// --------------------------------------------------------------------
		//                       Verify the physical plan
		// --------------------------------------------------------------------
		
		try
		{
			PhysicalPlanVerifier planVerifier = new PhysicalPlanVerifier();
			planVerifier.verify(getOptimizerJoinOf3Query2ReferencePlan(), actualPlanRoot);
		}
		catch(PhysicalPlanVerifierException e)
		{
			e.dumpMessage();
			Assert.fail();
		}
	}
	
	/**
	 * Simple range query on an indexed column, but with high selectivity.
	 */
	@Test
	public void testOptimizerJoinOf3Query3() throws Exception
	{
		// --------------------------------------------------------------------
		//                         Prepare the query
		// --------------------------------------------------------------------
		String sql =" SELECT " +
					"	ps.ps_availqty as ps_availqty, l.l_quantity as l_quantity, o.o_orderdate as o_orderdate " +
					" FROM " +
					"	partsupplier ps, lineitem l, orders o " +
					" WHERE " +
					"   ps.ps_partkey = l.l_partkey AND " +
					"   ps.ps_suppkey = l.l_suppkey AND " +
					"   o.o_orderkey = l.l_orderkey AND " +
					"   o.o_custkey >= 1 AND o.o_custkey <= 15";
		
		// parse the query
		SQLParserImpl parser = new SQLParserImpl(sql);
		SelectQuery selectQuery = (SelectQuery) parser.parse();		
		SelectQueryAnalyzerImpl semAna = new SelectQueryAnalyzerImpl();
		AnalyzedSelectQuery query = semAna.analyzeQuery(selectQuery, this.dbInstance.getCatalogue());

		// --------------------------------------------------------------------
		//                       Call the optimizer code
		// --------------------------------------------------------------------
		
		OptimizerPlanOperator actualPlanRoot = this.optimizer.createSelectQueryPlan(query);

		// --------------------------------------------------------------------
		//                       Verify the physical plan
		// --------------------------------------------------------------------
		
		try
		{
			PhysicalPlanVerifier planVerifier = new PhysicalPlanVerifier();
			planVerifier.verify(getOptimizerJoinOf3Query3ReferencePlan(), actualPlanRoot);
		}
		catch(PhysicalPlanVerifierException e)
		{
			e.dumpMessage();
			Assert.fail();
		}
	}
	
	/**
	 * Simple range query on an indexed column, but with high selectivity.
	 */
	@Test
	public void testOptimizerJoinOf3Query4() throws Exception
	{
		// --------------------------------------------------------------------
		//                         Prepare the query
		// --------------------------------------------------------------------
		String sql =" SELECT " +
					"	p.p_partkey as p_partkey, p.p_name as p_name, ps.ps_availqty as ps_availqty, l.l_quantity as l_quantity, l.l_tax as l_tax " +
					" FROM " +
					"	partsupplier ps, part p, lineitem l " +
					" WHERE " +
					"   p.p_partkey = ps.ps_partkey AND " +
					"   ps.ps_partkey = l.l_partkey AND " +
					"   ps.ps_suppkey >= 1 AND ps.ps_suppkey <= 200 AND " +
					"   p.p_size > 42 " +
					" ORDER BY p_partkey ASC";
		
		// parse the query
		SQLParserImpl parser = new SQLParserImpl(sql);
		SelectQuery selectQuery = (SelectQuery) parser.parse();		
		SelectQueryAnalyzerImpl semAna = new SelectQueryAnalyzerImpl();
		AnalyzedSelectQuery query = semAna.analyzeQuery(selectQuery, this.dbInstance.getCatalogue());

		// --------------------------------------------------------------------
		//                       Call the optimizer code
		// --------------------------------------------------------------------
		
		OptimizerPlanOperator actualPlanRoot = this.optimizer.createSelectQueryPlan(query);

		// --------------------------------------------------------------------
		//                       Verify the physical plan
		// --------------------------------------------------------------------
		
		try
		{
			PhysicalPlanVerifier planVerifier = new PhysicalPlanVerifier();
			planVerifier.verify(getOptimizerJoinOf3Query4ReferencePlan(), actualPlanRoot);
		}
		catch(PhysicalPlanVerifierException e)
		{
			e.dumpMessage();
			Assert.fail();
		}
	}
	
	/**
	 * Simple range query on an indexed column, but with high selectivity.
	 */
	@Test
	public void testOptimizerJoinOf4Query1() throws Exception
	{
		// --------------------------------------------------------------------
		//                         Prepare the query
		// --------------------------------------------------------------------
		String sql =" SELECT " +
					"	l.l_extendedprice AS l_extendedprice, o.o_totalprice as o_totalprice, c.c_name as c_name, p.p_name as p_name " +
					" FROM " +
					"	lineitem l, orders o, customer c, part p " +
					" WHERE " +
					"   p.p_partkey = l.l_partkey AND " +
					"   o.o_custkey = c.c_custkey AND " +
					"   l.l_orderkey = o.o_orderkey AND " +
					"   l.l_extendedprice < o.o_totalprice AND " +
					"   c.c_custkey < 100 ";
		
		// parse the query
		SQLParserImpl parser = new SQLParserImpl(sql);
		SelectQuery selectQuery = (SelectQuery) parser.parse();		
		SelectQueryAnalyzerImpl semAna = new SelectQueryAnalyzerImpl();
		AnalyzedSelectQuery query = semAna.analyzeQuery(selectQuery, this.dbInstance.getCatalogue());

		// --------------------------------------------------------------------
		//                       Call the optimizer code
		// --------------------------------------------------------------------
		
		OptimizerPlanOperator actualPlanRoot = this.optimizer.createSelectQueryPlan(query);

		// --------------------------------------------------------------------
		//                       Verify the physical plan
		// --------------------------------------------------------------------
		
		try
		{
			PhysicalPlanVerifier planVerifier = new PhysicalPlanVerifier();
			planVerifier.verify(getOptimizerJoinOf4Query1ReferencePlan(), actualPlanRoot);
		}
		catch(PhysicalPlanVerifierException e)
		{
			e.dumpMessage();
			Assert.fail();
		}
	}
	
	/**
	 * Simple range query on an indexed column, but with high selectivity.
	 */
	@Test
	public void testOptimizerJoinOf4WithGroupingQuery() throws Exception
	{
		// --------------------------------------------------------------------
		//                         Prepare the query
		// --------------------------------------------------------------------
		String sql =" SELECT " +
					"	r.r_regionkey AS regionkey, SUM(o.o_totalprice) as sum_totalprice " +
					" FROM " +
					"	orders o, customer c, nation n, region r " +
					" WHERE " +
					"   n.n_regionkey = r.r_regionkey AND " +
					"   c.c_nationkey = n.n_nationkey AND " +
					"   o.o_custkey = c.c_custkey " +
					" GROUP BY r.r_regionkey " +
					" HAVING sum_totalprice > 1000 ";
				
		
		// parse the query
		SQLParserImpl parser = new SQLParserImpl(sql);
		SelectQuery selectQuery = (SelectQuery) parser.parse();		
		SelectQueryAnalyzerImpl semAna = new SelectQueryAnalyzerImpl();
		AnalyzedSelectQuery query = semAna.analyzeQuery(selectQuery, this.dbInstance.getCatalogue());

		// --------------------------------------------------------------------
		//                       Call the optimizer code
		// --------------------------------------------------------------------
		
		OptimizerPlanOperator actualPlanRoot = this.optimizer.createSelectQueryPlan(query);

		// --------------------------------------------------------------------
		//                       Verify the physical plan
		// --------------------------------------------------------------------
		
		try
		{
			PhysicalPlanVerifier planVerifier = new PhysicalPlanVerifier();
			planVerifier.verify(getOptimizerJoinOf4WithGroupingReferencePlan(), actualPlanRoot);
		}
		catch(PhysicalPlanVerifierException e)
		{
			e.dumpMessage();
			Assert.fail();
		}
	}
	
	/**
	 * Simple range query on an indexed column, but with high selectivity.
	 */
	@Test
	public void testOptimizerJoinOf5WithGroupingAndSortQuery1() throws Exception
	{
		// --------------------------------------------------------------------
		//                         Prepare the query
		// --------------------------------------------------------------------
		String sql =" SELECT " +
					"	n.n_nationkey AS n_nationkey, n.n_name AS n_name, SUM(l.l_extendedprice) as sum_extendedprice " +
					" FROM " +
					"	lineitem l, orders o, customer c, nation n, region r " +
					" WHERE " +
					"   n.n_regionkey = r.r_regionkey AND " +
					"   c.c_nationkey = n.n_nationkey AND " +
					"   o.o_custkey = c.c_custkey AND " +
					"   l.l_orderkey = o.o_orderkey AND " +
					"   r.r_name = \"EUROPE\" " +
					" GROUP BY n.n_nationkey, n.n_name " +
					" HAVING sum_extendedprice > 1000 " +
            		" ORDER BY n_nationkey ASC, n_name DESC";
				
		
		// parse the query
		SQLParserImpl parser = new SQLParserImpl(sql);
		SelectQuery selectQuery = (SelectQuery) parser.parse();		
		SelectQueryAnalyzerImpl semAna = new SelectQueryAnalyzerImpl();
		AnalyzedSelectQuery query = semAna.analyzeQuery(selectQuery, this.dbInstance.getCatalogue());

		// --------------------------------------------------------------------
		//                       Call the optimizer code
		// --------------------------------------------------------------------
		
		OptimizerPlanOperator actualPlanRoot = this.optimizer.createSelectQueryPlan(query);

		// --------------------------------------------------------------------
		//                       Verify the physical plan
		// --------------------------------------------------------------------
		
		try
		{
			PhysicalPlanVerifier planVerifier = new PhysicalPlanVerifier();
			planVerifier.verify(getOptimizerJoinOf5WithGroupingAndSortQuery1ReferencePlan(), actualPlanRoot);
		}
		catch(PhysicalPlanVerifierException e)
		{
			e.dumpMessage();
			Assert.fail();
		}
	}
	
	/**
	 * Simple range query on an indexed column, but with high selectivity.
	 */
	@Test
	public void testOptimizerJoinOf5WithGroupingAndSortQuery2() throws Exception
	{
		// --------------------------------------------------------------------
		//                         Prepare the query
		// --------------------------------------------------------------------
		String sql =" SELECT " +
					"	n.n_nationkey AS n_nationkey, n.n_name AS n_name, SUM(l.l_extendedprice) as sum_extendedprice " +
					" FROM " +
					"	lineitem l, orders o, customer c, nation n, region r " +
					" WHERE " +
					"   n.n_regionkey = r.r_regionkey AND " +
					"   c.c_nationkey = n.n_nationkey AND " +
					"   o.o_custkey = c.c_custkey AND " +
					"   l.l_orderkey = o.o_orderkey AND " +
					"   r.r_name = \"EUROPE\" " +
					" GROUP BY n.n_nationkey, n.n_name " +
					" HAVING sum_extendedprice > 1000 " +
            		" ORDER BY n_nationkey ASC, n_name DESC, sum_extendedprice ASC";
				
		
		// parse the query
		SQLParserImpl parser = new SQLParserImpl(sql);
		SelectQuery selectQuery = (SelectQuery) parser.parse();		
		SelectQueryAnalyzerImpl semAna = new SelectQueryAnalyzerImpl();
		AnalyzedSelectQuery query = semAna.analyzeQuery(selectQuery, this.dbInstance.getCatalogue());

		// --------------------------------------------------------------------
		//                       Call the optimizer code
		// --------------------------------------------------------------------
		
		OptimizerPlanOperator actualPlanRoot = this.optimizer.createSelectQueryPlan(query);

		// --------------------------------------------------------------------
		//                       Verify the physical plan
		// --------------------------------------------------------------------
		
		try
		{
			PhysicalPlanVerifier planVerifier = new PhysicalPlanVerifier();
			planVerifier.verify(getOptimizerJoinOf5WithGroupingAndSortQuery2ReferencePlan(), actualPlanRoot);
		}
		catch(PhysicalPlanVerifierException e)
		{
			e.dumpMessage();
			Assert.fail();
		}
	}
	
	// -------------------------------------------------------------------------
	//                       Reference Plans for the Unit Tests
	// -------------------------------------------------------------------------
	
	private OptimizerPlanOperator getOptimizerSimplePointQueryReferencePlan() throws OptimizerException
	{
		// index scan subplan
		IndexDescriptor a_index = this.dbInstance.getCatalogue().getIndex("LINEITEM_FK_PART");
		BaseTableAccess a_table_access = new BaseTableAccess(this.dbInstance.getCatalogue().getTable("LINEITEM"));
		a_table_access.setOutputCardinality(30);
		Predicate a_pred_parsed_pred = createPredicate("li.l_partkey = 100", false);
		Column a_pred_column = new Column(a_table_access, DataType.get(BasicType.INT, 1), 1);
		DataField a_pred_literal = new IntField(100);
		LocalPredicateAtom a_pred = new LocalPredicateAtom(a_pred_parsed_pred, a_pred_column, a_pred_literal);
		IndexLookupPlanOperator a = new IndexLookupPlanOperator(a_index, a_table_access, a_pred, 30);

		// fetch subplan
		BaseTableAccess b_table_access = new BaseTableAccess(this.dbInstance.getCatalogue().getTable("LINEITEM"));
		b_table_access.setOutputCardinality(30);
		Column[] b_columns = new Column[3];
		b_columns[0] = new Column(b_table_access, DataType.get(BasicType.INT, 1), 1);
		b_columns[1] = new Column(b_table_access, DataType.get(BasicType.INT, 1), 2);
		b_columns[2] = new Column(b_table_access, DataType.get(BasicType.FLOAT, 1), 4);
		FetchPlanOperator b = new FetchPlanOperator(a, b_table_access, b_columns);
		
		CostEstimator costEstimator = AbstractExtensionFactory.getExtensionFactory().createCostEstimator(this.config.getBlockReadCost(), this.config.getBlockWriteCost(), this.config.getBlockRandomReadOverhead(), this.config.getBlockRandomWriteOverhead());
		PhysicalPlanCostUpdater costUpdater = new PhysicalPlanCostUpdater(costEstimator);
		costUpdater.costGenericOperator(b);
		
		return b;
	}
	
	private OptimizerPlanOperator getOptimizerRangeQueryReferencePlan() throws OptimizerException
	{
		// table scan (LINEITEM)
		BaseTableAccess relation_0 = new BaseTableAccess(this.dbInstance.getCatalogue().getTable("LINEITEM"));
		relation_0.setOutputCardinality(120515);
		BaseTableAccess a_table_access = relation_0;
		Column[] a_columns = new Column[3];
		a_columns[0] = new Column(a_table_access, DataType.get(BasicType.INT, 1), 1);
		a_columns[1] = new Column(a_table_access, DataType.get(BasicType.INT, 1), 2);
		a_columns[2] = new Column(a_table_access, DataType.get(BasicType.FLOAT, 1), 4);
		TableScanPlanOperator a = new TableScanPlanOperator(a_table_access, a_columns);
		Predicate a_pred_lb_pred = createPredicate("li.l_suppkey > 100", false);
		Predicate a_pred_ub_pred = createPredicate("li.l_suppkey < 10000", false);
		Column a_pred_column = new Column(a_table_access, DataType.get(BasicType.INT, 1), 2);
		DataField a_pred_lb_literal = new IntField(100);
		DataField a_pred_up_literal = new IntField(10000);
		LocalPredicateBetween a_pred = new LocalPredicateBetween(a_pred_column, a_pred_lb_pred, a_pred_ub_pred, a_pred_lb_literal, a_pred_up_literal);
		a.setPredicate(a_pred);
		
		CostEstimator costEstimator = AbstractExtensionFactory.getExtensionFactory().createCostEstimator(this.config.getBlockReadCost(), this.config.getBlockWriteCost(), this.config.getBlockRandomReadOverhead(), this.config.getBlockRandomWriteOverhead());
		PhysicalPlanCostUpdater costUpdater = new PhysicalPlanCostUpdater(costEstimator);
		costUpdater.costGenericOperator(a);
		
		return a;
	}
	
	private OptimizerPlanOperator getOptimizerSimpleGroupingQueryReferencePlan() throws OptimizerException
	{
		// table scan (PARTSUPPLIER)
		BaseTableAccess relation_0 = new BaseTableAccess(this.dbInstance.getCatalogue().getTable("PARTSUPPLIER"));
		relation_0.setOutputCardinality(16000);
		BaseTableAccess a_table_access = relation_0;
		Column[] a_columns = new Column[2];
		a_columns[0] = new Column(a_table_access, DataType.get(BasicType.INT, 1), 0);
		a_columns[1] = new Column(a_table_access, DataType.get(BasicType.INT, 1), 2);
		TableScanPlanOperator a = new TableScanPlanOperator(a_table_access, a_columns);

		// sort subplan
		int[] b_col_indices = new int[1];
		b_col_indices[0] = 0;
		boolean[] b_sort_asc = new boolean[1];
		b_sort_asc[0] = true;
		SortPlanOperator b = new SortPlanOperator(a, b_col_indices, b_sort_asc);

		// group by subplan
		ProducedColumn[] c_prod_columns = new ProducedColumn[2];
		Relation c_prod_column_0_relation = relation_0;
		DataType c_prod_column_0_data_type = DataType.get(BasicType.INT, 1);
		int c_prod_column_0_col_index = 0;
		String c_prod_column_0_col_alias = "ps_partkey";
		OutputColumn c_prod_column_0_parsed_col = null; //TODO
		OutputColumn.AggregationType c_prod_column_0_agg_fun = OutputColumn.AggregationType.NONE;
		c_prod_columns[0] = new ProducedColumn(c_prod_column_0_relation, c_prod_column_0_data_type, c_prod_column_0_col_index, c_prod_column_0_col_alias, c_prod_column_0_parsed_col, c_prod_column_0_agg_fun);
		Relation c_prod_column_1_relation = relation_0;
		DataType c_prod_column_1_data_type = DataType.get(BasicType.INT, 1);
		int c_prod_column_1_col_index = 2;
		String c_prod_column_1_col_alias = "ps_availqty";
		OutputColumn c_prod_column_1_parsed_col = null; //TODO
		OutputColumn.AggregationType c_prod_column_1_agg_fun = OutputColumn.AggregationType.SUM;
		c_prod_columns[1] = new ProducedColumn(c_prod_column_1_relation, c_prod_column_1_data_type, c_prod_column_1_col_index, c_prod_column_1_col_alias, c_prod_column_1_parsed_col, c_prod_column_1_agg_fun);

		int[] c_group_col_indices = new int[1];
		c_group_col_indices[0] = 0;

		int[] c_agg_col_indices = new int[1];
		c_agg_col_indices[0] = 1;

		GroupByPlanOperator c = new GroupByPlanOperator(b, c_prod_columns, c_group_col_indices, c_agg_col_indices, 4000);
		
		CostEstimator costEstimator = AbstractExtensionFactory.getExtensionFactory().createCostEstimator(this.config.getBlockReadCost(), this.config.getBlockWriteCost(), this.config.getBlockRandomReadOverhead(), this.config.getBlockRandomWriteOverhead());
		PhysicalPlanCostUpdater costUpdater = new PhysicalPlanCostUpdater(costEstimator);
		costUpdater.costGenericOperator(c);
		
		return c;
	}
	
	private OptimizerPlanOperator getOptimizerJoinOf3Query1ReferencePlan() throws OptimizerException
	{
		// table scan (PARTSUPPLIER)
		BaseTableAccess relation_0 = new BaseTableAccess(this.dbInstance.getCatalogue().getTable("PARTSUPPLIER"));
		relation_0.setID(0);
		relation_0.setOutputCardinality(16000);
		BaseTableAccess a_table_access = relation_0;
		Column[] a_columns = new Column[3];
		a_columns[0] = new Column(a_table_access, DataType.get(BasicType.FLOAT, 1), 3);
		a_columns[1] = new Column(a_table_access, DataType.get(BasicType.INT, 1), 1);
		a_columns[2] = new Column(a_table_access, DataType.get(BasicType.INT, 1), 0);
		TableScanPlanOperator a = new TableScanPlanOperator(a_table_access, a_columns);

		// sort subplan
		int[] b_col_indices = new int[1];
		b_col_indices[0] = 2;
		boolean[] b_sort_asc = new boolean[1];
		b_sort_asc[0] = true;
		SortPlanOperator b = new SortPlanOperator(a, b_col_indices, b_sort_asc);

		// table scan (PART)
		BaseTableAccess relation_1 = new BaseTableAccess(this.dbInstance.getCatalogue().getTable("PART"));
		relation_1.setID(1);
		relation_1.setOutputCardinality(99);
		BaseTableAccess c_table_access = relation_1;
		Column[] c_columns = new Column[2];
		c_columns[0] = new Column(c_table_access, DataType.get(BasicType.VAR_CHAR, 55), 1);
		c_columns[1] = new Column(c_table_access, DataType.get(BasicType.INT, 1), 0);
		TableScanPlanOperator c = new TableScanPlanOperator(c_table_access, c_columns);
		Predicate c_pred_lb_pred = createPredicate("p.p_partkey >= 1", false);
		Predicate c_pred_ub_pred = createPredicate("p.p_partkey <= 100", false);
		Column c_pred_column = new Column(c_table_access, DataType.get(BasicType.INT, 1), 0);
		DataField c_pred_lb_literal = new IntField(1);
		DataField c_pred_up_literal = new IntField(100);
		LocalPredicateBetween c_pred = new LocalPredicateBetween(c_pred_column, c_pred_lb_pred, c_pred_ub_pred, c_pred_lb_literal, c_pred_up_literal);
		c.setPredicate(c_pred);

		// sort subplan
		int[] d_col_indices = new int[1];
		d_col_indices[0] = 1;
		boolean[] d_sort_asc = new boolean[1];
		d_sort_asc[0] = true;
		SortPlanOperator d = new SortPlanOperator(c, d_col_indices, d_sort_asc);

		// merge join subplan
		int[] e_lj_cols = new int[1];
		e_lj_cols[0] = 2;
		int[] e_rj_cols = new int[1];
		e_rj_cols[0] = 1;
		int[] e_lc_map= new int[3];
		e_lc_map[0] = -1;
		e_lc_map[1] = 0;
		e_lc_map[2] = 1;
		int[] e_rc_map= new int[3];
		e_rc_map[0] = 0;
		e_rc_map[1] = -1;
		e_rc_map[2] = -1;
		MergeJoinPlanOperator e = new MergeJoinPlanOperator(b, d, null, e_lj_cols, e_rj_cols, e_lc_map, e_rc_map, 396);

		// sort subplan
		int[] f_col_indices = new int[1];
		f_col_indices[0] = 2;
		boolean[] f_sort_asc = new boolean[1];
		f_sort_asc[0] = true;
		SortPlanOperator f = new SortPlanOperator(e, f_col_indices, f_sort_asc);

		// table scan (SUPPLIER)
		BaseTableAccess relation_2 = new BaseTableAccess(this.dbInstance.getCatalogue().getTable("SUPPLIER"));
		relation_2.setID(2);
		relation_2.setOutputCardinality(200);
		BaseTableAccess g_table_access = relation_2;
		Column[] g_columns = new Column[2];
		g_columns[0] = new Column(g_table_access, DataType.get(BasicType.CHAR, 25), 1);
		g_columns[1] = new Column(g_table_access, DataType.get(BasicType.INT, 1), 0);
		TableScanPlanOperator g = new TableScanPlanOperator(g_table_access, g_columns);

		// sort subplan
		int[] h_col_indices = new int[1];
		h_col_indices[0] = 1;
		boolean[] h_sort_asc = new boolean[1];
		h_sort_asc[0] = true;
		SortPlanOperator h = new SortPlanOperator(g, h_col_indices, h_sort_asc);

		// merge join subplan
		int[] i_lj_cols = new int[1];
		i_lj_cols[0] = 2;
		int[] i_rj_cols = new int[1];
		i_rj_cols[0] = 1;
		int[] i_lc_map= new int[3];
		i_lc_map[0] = 0;
		i_lc_map[1] = -1;
		i_lc_map[2] = 1;
		int[] i_rc_map= new int[3];
		i_rc_map[0] = -1;
		i_rc_map[1] = 0;
		i_rc_map[2] = -1;
		MergeJoinPlanOperator i = new MergeJoinPlanOperator(f, h, null, i_lj_cols, i_rj_cols, i_lc_map, i_rc_map, 396);
		
		CostEstimator costEstimator = AbstractExtensionFactory.getExtensionFactory().createCostEstimator(this.config.getBlockReadCost(), this.config.getBlockWriteCost(), this.config.getBlockRandomReadOverhead(), this.config.getBlockRandomWriteOverhead());
		PhysicalPlanCostUpdater costUpdater = new PhysicalPlanCostUpdater(costEstimator);
		costUpdater.costGenericOperator(i);
		
		return i;
	}
	
	private OptimizerPlanOperator getOptimizerJoinOf3Query2ReferencePlan() throws OptimizerException
	{
		// index scan subplan
		IndexDescriptor a_index = this.dbInstance.getCatalogue().getIndex("ORDER_FK_CUSTOMER");
		BaseTableAccess relation_2 = new BaseTableAccess(this.dbInstance.getCatalogue().getTable("ORDERS"));
		relation_2.setID(2);
		relation_2.setOutputCardinality(40);
		BaseTableAccess a_table_access = relation_2;
		Predicate a_pred_lb_pred = createPredicate("o.o_custkey >= 1", false);
		Predicate a_pred_ub_pred = createPredicate("o.o_custkey <= 5", false);
		Column a_pred_column = new Column(a_table_access, DataType.get(BasicType.INT, 1), 1);
		DataField a_pred_lb_literal = new IntField(1);
		DataField a_pred_up_literal = new IntField(5);
		LocalPredicateBetween a_pred = new LocalPredicateBetween(a_pred_column, a_pred_lb_pred, a_pred_ub_pred, a_pred_lb_literal, a_pred_up_literal);
		IndexLookupPlanOperator a = new IndexLookupPlanOperator(a_index, a_table_access, a_pred, 40);

		// fetch subplan
		BaseTableAccess b_table_access = relation_2;
		Column[] b_columns = new Column[2];
		b_columns[0] = new Column(b_table_access, DataType.get(BasicType.DATE, 1), 4);
		b_columns[1] = new Column(b_table_access, DataType.get(BasicType.INT, 1), 0);
		FetchPlanOperator b = new FetchPlanOperator(a, b_table_access, b_columns);

		// index scan subplan
		IndexDescriptor c_index = this.dbInstance.getCatalogue().getIndex("LINEITEM_FK_ORDER");
		BaseTableAccess relation_1 = new BaseTableAccess(this.dbInstance.getCatalogue().getTable("LINEITEM"));
		relation_1.setID(1);
		relation_1.setOutputCardinality(120515);
		BaseTableAccess c_table_access = relation_1;
		int c_corr_index = 1;
		IndexLookupPlanOperator c = new IndexLookupPlanOperator(c_index, c_table_access, c_corr_index, 4);

		// fetch subplan
		BaseTableAccess d_table_access = relation_1;
		Column[] d_columns = new Column[4];
		d_columns[0] = new Column(d_table_access, DataType.get(BasicType.FLOAT, 1), 4);
		d_columns[1] = new Column(d_table_access, DataType.get(BasicType.INT, 1), 1);
		d_columns[2] = new Column(d_table_access, DataType.get(BasicType.INT, 1), 2);
		d_columns[3] = new Column(d_table_access, DataType.get(BasicType.INT, 1), 0);
		FetchPlanOperator d = new FetchPlanOperator(c, d_table_access, d_columns);

		// nested loop join subplan
		int[] e_oc_map= new int[4];
		e_oc_map[0] = -1;
		e_oc_map[1] = 0;
		e_oc_map[2] = -1;
		e_oc_map[3] = -1;
		int[] e_ic_map= new int[4];
		e_ic_map[0] = 0;
		e_ic_map[1] = -1;
		e_ic_map[2] = 1;
		e_ic_map[3] = 2;
		NestedLoopJoinPlanOperator e = new NestedLoopJoinPlanOperator(b, d, null, e_oc_map, e_ic_map, 160);

		// sort subplan
		int[] f_col_indices = new int[2];
		f_col_indices[0] = 2;
		f_col_indices[1] = 3;
		boolean[] f_sort_asc = new boolean[2];
		f_sort_asc[0] = true;
		f_sort_asc[1] = true;
		SortPlanOperator f = new SortPlanOperator(e, f_col_indices, f_sort_asc);

		// table scan (PARTSUPPLIER)
		BaseTableAccess relation_0 = new BaseTableAccess(this.dbInstance.getCatalogue().getTable("PARTSUPPLIER"));
		relation_0.setID(0);
		relation_0.setOutputCardinality(16000);
		BaseTableAccess g_table_access = relation_0;
		Column[] g_columns = new Column[3];
		g_columns[0] = new Column(g_table_access, DataType.get(BasicType.INT, 1), 2);
		g_columns[1] = new Column(g_table_access, DataType.get(BasicType.INT, 1), 0);
		g_columns[2] = new Column(g_table_access, DataType.get(BasicType.INT, 1), 1);
		TableScanPlanOperator g = new TableScanPlanOperator(g_table_access, g_columns);

		// sort subplan
		int[] h_col_indices = new int[2];
		h_col_indices[0] = 1;
		h_col_indices[1] = 2;
		boolean[] h_sort_asc = new boolean[2];
		h_sort_asc[0] = true;
		h_sort_asc[1] = true;
		SortPlanOperator h = new SortPlanOperator(g, h_col_indices, h_sort_asc);

		// merge join subplan
		int[] i_lj_cols = new int[2];
		i_lj_cols[0] = 2;
		i_lj_cols[1] = 3;
		int[] i_rj_cols = new int[2];
		i_rj_cols[0] = 1;
		i_rj_cols[1] = 2;
		int[] i_lc_map= new int[3];
		i_lc_map[0] = -1;
		i_lc_map[1] = 0;
		i_lc_map[2] = 1;
		int[] i_rc_map= new int[3];
		i_rc_map[0] = 0;
		i_rc_map[1] = -1;
		i_rc_map[2] = -1;
		MergeJoinPlanOperator i = new MergeJoinPlanOperator(f, h, null, i_lj_cols, i_rj_cols, i_lc_map, i_rc_map, 640);
		
		CostEstimator costEstimator = AbstractExtensionFactory.getExtensionFactory().createCostEstimator(this.config.getBlockReadCost(), this.config.getBlockWriteCost(), this.config.getBlockRandomReadOverhead(), this.config.getBlockRandomWriteOverhead());
		PhysicalPlanCostUpdater costUpdater = new PhysicalPlanCostUpdater(costEstimator);
		costUpdater.costGenericOperator(i);

		return i;
	}
	
	private OptimizerPlanOperator getOptimizerJoinOf3Query3ReferencePlan() throws OptimizerException
	{
		// table scan (LINEITEM)
		BaseTableAccess relation_1 = new BaseTableAccess(this.dbInstance.getCatalogue().getTable("LINEITEM"));
		relation_1.setID(1);
		relation_1.setOutputCardinality(120515);
		BaseTableAccess a_table_access = relation_1;
		Column[] a_columns = new Column[4];
		a_columns[0] = new Column(a_table_access, DataType.get(BasicType.FLOAT, 1), 4);
		a_columns[1] = new Column(a_table_access, DataType.get(BasicType.INT, 1), 1);
		a_columns[2] = new Column(a_table_access, DataType.get(BasicType.INT, 1), 2);
		a_columns[3] = new Column(a_table_access, DataType.get(BasicType.INT, 1), 0);
		TableScanPlanOperator a = new TableScanPlanOperator(a_table_access, a_columns);

		// sort subplan
		int[] b_col_indices = new int[1];
		b_col_indices[0] = 3;
		boolean[] b_sort_asc = new boolean[1];
		b_sort_asc[0] = true;
		SortPlanOperator b = new SortPlanOperator(a, b_col_indices, b_sort_asc);

		// table scan (ORDERS)
		BaseTableAccess relation_2 = new BaseTableAccess(this.dbInstance.getCatalogue().getTable("ORDERS"));
		relation_2.setID(2);
		relation_2.setOutputCardinality(140);
		BaseTableAccess c_table_access = relation_2;
		Column[] c_columns = new Column[2];
		c_columns[0] = new Column(c_table_access, DataType.get(BasicType.DATE, 1), 4);
		c_columns[1] = new Column(c_table_access, DataType.get(BasicType.INT, 1), 0);
		TableScanPlanOperator c = new TableScanPlanOperator(c_table_access, c_columns);
		Predicate c_pred_lb_pred = createPredicate("o.o_custkey >= 1", false);
		Predicate c_pred_ub_pred = createPredicate("o.o_custkey <= 15", false);
		Column c_pred_column = new Column(c_table_access, DataType.get(BasicType.INT, 1), 1);
		DataField c_pred_lb_literal = new IntField(1);
		DataField c_pred_up_literal = new IntField(15);
		LocalPredicateBetween c_pred = new LocalPredicateBetween(c_pred_column, c_pred_lb_pred, c_pred_ub_pred, c_pred_lb_literal, c_pred_up_literal);
		c.setPredicate(c_pred);

		// sort subplan
		int[] d_col_indices = new int[1];
		d_col_indices[0] = 1;
		boolean[] d_sort_asc = new boolean[1];
		d_sort_asc[0] = true;
		SortPlanOperator d = new SortPlanOperator(c, d_col_indices, d_sort_asc);

		// merge join subplan
		int[] e_lj_cols = new int[1];
		e_lj_cols[0] = 3;
		int[] e_rj_cols = new int[1];
		e_rj_cols[0] = 1;
		int[] e_lc_map= new int[4];
		e_lc_map[0] = 0;
		e_lc_map[1] = -1;
		e_lc_map[2] = 1;
		e_lc_map[3] = 2;
		int[] e_rc_map= new int[4];
		e_rc_map[0] = -1;
		e_rc_map[1] = 0;
		e_rc_map[2] = -1;
		e_rc_map[3] = -1;
		MergeJoinPlanOperator e = new MergeJoinPlanOperator(b, d, null, e_lj_cols, e_rj_cols, e_lc_map, e_rc_map, 562);

		// sort subplan
		int[] f_col_indices = new int[2];
		f_col_indices[0] = 2;
		f_col_indices[1] = 3;
		boolean[] f_sort_asc = new boolean[2];
		f_sort_asc[0] = true;
		f_sort_asc[1] = true;
		SortPlanOperator f = new SortPlanOperator(e, f_col_indices, f_sort_asc);

		// table scan (PARTSUPPLIER)
		BaseTableAccess relation_0 = new BaseTableAccess(this.dbInstance.getCatalogue().getTable("PARTSUPPLIER"));
		relation_0.setID(0);
		relation_0.setOutputCardinality(16000);
		BaseTableAccess g_table_access = relation_0;
		Column[] g_columns = new Column[3];
		g_columns[0] = new Column(g_table_access, DataType.get(BasicType.INT, 1), 2);
		g_columns[1] = new Column(g_table_access, DataType.get(BasicType.INT, 1), 0);
		g_columns[2] = new Column(g_table_access, DataType.get(BasicType.INT, 1), 1);
		TableScanPlanOperator g = new TableScanPlanOperator(g_table_access, g_columns);

		// sort subplan
		int[] h_col_indices = new int[2];
		h_col_indices[0] = 1;
		h_col_indices[1] = 2;
		boolean[] h_sort_asc = new boolean[2];
		h_sort_asc[0] = true;
		h_sort_asc[1] = true;
		SortPlanOperator h = new SortPlanOperator(g, h_col_indices, h_sort_asc);

		// merge join subplan
		int[] i_lj_cols = new int[2];
		i_lj_cols[0] = 2;
		i_lj_cols[1] = 3;
		int[] i_rj_cols = new int[2];
		i_rj_cols[0] = 1;
		i_rj_cols[1] = 2;
		int[] i_lc_map= new int[3];
		i_lc_map[0] = -1;
		i_lc_map[1] = 0;
		i_lc_map[2] = 1;
		int[] i_rc_map= new int[3];
		i_rc_map[0] = 0;
		i_rc_map[1] = -1;
		i_rc_map[2] = -1;
		MergeJoinPlanOperator i = new MergeJoinPlanOperator(f, h, null, i_lj_cols, i_rj_cols, i_lc_map, i_rc_map, 2248);
		
		CostEstimator costEstimator = AbstractExtensionFactory.getExtensionFactory().createCostEstimator(this.config.getBlockReadCost(), this.config.getBlockWriteCost(), this.config.getBlockRandomReadOverhead(), this.config.getBlockRandomWriteOverhead());
		PhysicalPlanCostUpdater costUpdater = new PhysicalPlanCostUpdater(costEstimator);
		costUpdater.costGenericOperator(i);

		return i;
	}
	
	private OptimizerPlanOperator getOptimizerJoinOf3Query4ReferencePlan() throws OptimizerException
	{
		// table scan (PARTSUPPLIER)
		BaseTableAccess relation_0 = new BaseTableAccess(this.dbInstance.getCatalogue().getTable("PARTSUPPLIER"));
		relation_0.setID(0);
		relation_0.setOutputCardinality(16000);
		BaseTableAccess a_table_access = relation_0;
		Column[] a_columns = new Column[2];
		a_columns[0] = new Column(a_table_access, DataType.get(BasicType.INT, 1), 2);
		a_columns[1] = new Column(a_table_access, DataType.get(BasicType.INT, 1), 0);
		TableScanPlanOperator a = new TableScanPlanOperator(a_table_access, a_columns);
		Predicate a_pred_lb_pred = createPredicate("ps.ps_suppkey >= 1", false);
		Predicate a_pred_ub_pred = createPredicate("ps.ps_suppkey <= 200", false);
		Column a_pred_column = new Column(a_table_access, DataType.get(BasicType.INT, 1), 1);
		DataField a_pred_lb_literal = new IntField(1);
		DataField a_pred_up_literal = new IntField(200);
		LocalPredicateBetween a_pred = new LocalPredicateBetween(a_pred_column, a_pred_lb_pred, a_pred_ub_pred, a_pred_lb_literal, a_pred_up_literal);
		a.setPredicate(a_pred);

		// sort subplan
		int[] b_col_indices = new int[1];
		b_col_indices[0] = 1;
		boolean[] b_sort_asc = new boolean[1];
		b_sort_asc[0] = true;
		SortPlanOperator b = new SortPlanOperator(a, b_col_indices, b_sort_asc);

		// table scan (PART)
		BaseTableAccess relation_1 = new BaseTableAccess(this.dbInstance.getCatalogue().getTable("PART"));
		relation_1.setID(1);
		relation_1.setOutputCardinality(653);
		BaseTableAccess c_table_access = relation_1;
		Column[] c_columns = new Column[2];
		c_columns[0] = new Column(c_table_access, DataType.get(BasicType.INT, 1), 0);
		c_columns[1] = new Column(c_table_access, DataType.get(BasicType.VAR_CHAR, 55), 1);
		TableScanPlanOperator c = new TableScanPlanOperator(c_table_access, c_columns);
		Predicate c_pred_parsed_pred = createPredicate("p.p_size > 42", false);
		Column c_pred_column = new Column(c_table_access, DataType.get(BasicType.INT, 1), 5);
		DataField c_pred_literal = new IntField(42);
		LocalPredicateAtom c_pred = new LocalPredicateAtom(c_pred_parsed_pred, c_pred_column, c_pred_literal);
		c.setPredicate(c_pred);

		// sort subplan
		int[] d_col_indices = new int[1];
		d_col_indices[0] = 0;
		boolean[] d_sort_asc = new boolean[1];
		d_sort_asc[0] = true;
		SortPlanOperator d = new SortPlanOperator(c, d_col_indices, d_sort_asc);

		// merge join subplan
		int[] e_lj_cols = new int[1];
		e_lj_cols[0] = 1;
		int[] e_rj_cols = new int[1];
		e_rj_cols[0] = 0;
		int[] e_lc_map = new int[3];
		e_lc_map[0] = -1;
		e_lc_map[1] = -1;
		e_lc_map[2] = 0;
		int[] e_rc_map = new int[3];
		e_rc_map[0] = 0;
		e_rc_map[1] = 1;
		e_rc_map[2] = -1;
		MergeJoinPlanOperator e = new MergeJoinPlanOperator(b, d, null, e_lj_cols, e_rj_cols, e_lc_map, e_rc_map, 2612);

		// table scan (LINEITEM)
		BaseTableAccess relation_2 = new BaseTableAccess(this.dbInstance.getCatalogue().getTable("LINEITEM"));
		relation_2.setID(2);
		relation_2.setOutputCardinality(120515);
		BaseTableAccess f_table_access = relation_2;
		Column[] f_columns = new Column[3];
		f_columns[0] = new Column(f_table_access, DataType.get(BasicType.FLOAT, 1), 4);
		f_columns[1] = new Column(f_table_access, DataType.get(BasicType.FLOAT, 1), 7);
		f_columns[2] = new Column(f_table_access, DataType.get(BasicType.INT, 1), 1);
		TableScanPlanOperator f = new TableScanPlanOperator(f_table_access, f_columns);

		// sort subplan
		int[] g_col_indices = new int[1];
		g_col_indices[0] = 2;
		boolean[] g_sort_asc = new boolean[1];
		g_sort_asc[0] = true;
		SortPlanOperator g = new SortPlanOperator(f, g_col_indices, g_sort_asc);

		// merge join subplan
		int[] h_lj_cols = new int[1];
		h_lj_cols[0] = 0;
		int[] h_rj_cols = new int[1];
		h_rj_cols[0] = 2;
		int[] h_lc_map = new int[5];
		h_lc_map[0] = 0;
		h_lc_map[1] = 1;
		h_lc_map[2] = 2;
		h_lc_map[3] = -1;
		h_lc_map[4] = -1;
		int[] h_rc_map = new int[5];
		h_rc_map[0] = -1;
		h_rc_map[1] = -1;
		h_rc_map[2] = -1;
		h_rc_map[3] = 0;
		h_rc_map[4] = 1;
		MergeJoinPlanOperator h = new MergeJoinPlanOperator(e, g, null, h_lj_cols, h_rj_cols, h_lc_map, h_rc_map, 78696);
		
		CostEstimator costEstimator = AbstractExtensionFactory.getExtensionFactory().createCostEstimator(this.config.getBlockReadCost(), this.config.getBlockWriteCost(), this.config.getBlockRandomReadOverhead(), this.config.getBlockRandomWriteOverhead());
		PhysicalPlanCostUpdater costUpdater = new PhysicalPlanCostUpdater(costEstimator);
		costUpdater.costGenericOperator(h);
		
		return h;
	}
	
	private OptimizerPlanOperator getOptimizerJoinOf4Query1ReferencePlan() throws OptimizerException
	{
		// table scan (ORDERS)
		BaseTableAccess relation_1 = new BaseTableAccess(this.dbInstance.getCatalogue().getTable("ORDERS"));
		relation_1.setID(1);
		relation_1.setOutputCardinality(30000);
		BaseTableAccess a_table_access = relation_1;
		Column[] a_columns = new Column[3];
		a_columns[0] = new Column(a_table_access, DataType.get(BasicType.FLOAT, 1), 3);
		a_columns[1] = new Column(a_table_access, DataType.get(BasicType.INT, 1), 0);
		a_columns[2] = new Column(a_table_access, DataType.get(BasicType.INT, 1), 1);
		TableScanPlanOperator a = new TableScanPlanOperator(a_table_access, a_columns);

		// sort subplan
		int[] b_col_indices = new int[1];
		b_col_indices[0] = 2;
		boolean[] b_sort_asc = new boolean[1];
		b_sort_asc[0] = true;
		SortPlanOperator b = new SortPlanOperator(a, b_col_indices, b_sort_asc);

		// table scan (CUSTOMER)
		BaseTableAccess relation_2 = new BaseTableAccess(this.dbInstance.getCatalogue().getTable("CUSTOMER"));
		relation_2.setID(2);
		relation_2.setOutputCardinality(99);
		BaseTableAccess c_table_access = relation_2;
		Column[] c_columns = new Column[2];
		c_columns[0] = new Column(c_table_access, DataType.get(BasicType.VAR_CHAR, 25), 1);
		c_columns[1] = new Column(c_table_access, DataType.get(BasicType.INT, 1), 0);
		TableScanPlanOperator c = new TableScanPlanOperator(c_table_access, c_columns);
		Predicate c_pred_parsed_pred = createPredicate("c.c_custkey < 100", false);
		Column c_pred_column = new Column(c_table_access, DataType.get(BasicType.INT, 1), 0);
		DataField c_pred_literal = new IntField(100);
		LocalPredicateAtom c_pred = new LocalPredicateAtom(c_pred_parsed_pred, c_pred_column, c_pred_literal);
		c.setPredicate(c_pred);

		// sort subplan
		int[] d_col_indices = new int[1];
		d_col_indices[0] = 1;
		boolean[] d_sort_asc = new boolean[1];
		d_sort_asc[0] = true;
		SortPlanOperator d = new SortPlanOperator(c, d_col_indices, d_sort_asc);

		// merge subplan
		int[] e_lj_cols = new int[1];
		e_lj_cols[0] = 2;
		int[] e_rj_cols = new int[1];
		e_rj_cols[0] = 1;
		int[] e_lc_map= new int[3];
		e_lc_map[0] = 0;
		e_lc_map[1] = -1;
		e_lc_map[2] = 1;
		int[] e_rc_map= new int[3];
		e_rc_map[0] = -1;
		e_rc_map[1] = 0;
		e_rc_map[2] = -1;
		MergeJoinPlanOperator e = new MergeJoinPlanOperator(b, d, null, e_lj_cols, e_rj_cols, e_lc_map, e_rc_map, 990);

		// table scan (LINEITEM)
		BaseTableAccess relation_0 = new BaseTableAccess(this.dbInstance.getCatalogue().getTable("LINEITEM"));
		relation_0.setID(0);
		relation_0.setOutputCardinality(120515);
		BaseTableAccess f_table_access = relation_0;
		Column[] f_columns = new Column[3];
		f_columns[0] = new Column(f_table_access, DataType.get(BasicType.FLOAT, 1), 5);
		f_columns[1] = new Column(f_table_access, DataType.get(BasicType.INT, 1), 1);
		f_columns[2] = new Column(f_table_access, DataType.get(BasicType.INT, 1), 0);
		TableScanPlanOperator f = new TableScanPlanOperator(f_table_access, f_columns);

		// nested loop join subplan
		int[] g_oc_map= new int[4];
		g_oc_map[0] = -1;
		g_oc_map[1] = 0;
		g_oc_map[2] = 1;
		g_oc_map[3] = -1;
		int[] g_ic_map= new int[4];
		g_ic_map[0] = 0;
		g_ic_map[1] = -1;
		g_ic_map[2] = -1;
		g_ic_map[3] = 1;
		NestedLoopJoinPlanOperator g = new NestedLoopJoinPlanOperator(e, f, null, g_oc_map, g_ic_map, 3976);

		// sort subplan
		int[] h_col_indices = new int[1];
		h_col_indices[0] = 3;
		boolean[] h_sort_asc = new boolean[1];
		h_sort_asc[0] = true;
		SortPlanOperator h = new SortPlanOperator(g, h_col_indices, h_sort_asc);

		// table scan (PART)
		BaseTableAccess relation_3 = new BaseTableAccess(this.dbInstance.getCatalogue().getTable("PART"));
		relation_3.setID(3);
		relation_3.setOutputCardinality(4000);
		BaseTableAccess i_table_access = relation_3;
		Column[] i_columns = new Column[2];
		i_columns[0] = new Column(i_table_access, DataType.get(BasicType.VAR_CHAR, 55), 1);
		i_columns[1] = new Column(i_table_access, DataType.get(BasicType.INT, 1), 0);
		TableScanPlanOperator i = new TableScanPlanOperator(i_table_access, i_columns);

		// sort subplan
		int[] j_col_indices = new int[1];
		j_col_indices[0] = 1;
		boolean[] j_sort_asc = new boolean[1];
		j_sort_asc[0] = true;
		SortPlanOperator j = new SortPlanOperator(i, j_col_indices, j_sort_asc);

		// merge subplan
		int[] k_lj_cols = new int[1];
		k_lj_cols[0] = 3;
		int[] k_rj_cols = new int[1];
		k_rj_cols[0] = 1;
		int[] k_lc_map= new int[4];
		k_lc_map[0] = 0;
		k_lc_map[1] = 1;
		k_lc_map[2] = 2;
		k_lc_map[3] = -1;
		int[] k_rc_map= new int[4];
		k_rc_map[0] = -1;
		k_rc_map[1] = -1;
		k_rc_map[2] = -1;
		k_rc_map[3] = 0;
		MergeJoinPlanOperator k = new MergeJoinPlanOperator(h, j, null, k_lj_cols, k_rj_cols, k_lc_map, k_rc_map, 3976);
		
		CostEstimator costEstimator = AbstractExtensionFactory.getExtensionFactory().createCostEstimator(this.config.getBlockReadCost(), this.config.getBlockWriteCost(), this.config.getBlockRandomReadOverhead(), this.config.getBlockRandomWriteOverhead());
		PhysicalPlanCostUpdater costUpdater = new PhysicalPlanCostUpdater(costEstimator);
		costUpdater.costGenericOperator(k);
		
		return k;
	}
	
	private OptimizerPlanOperator getOptimizerJoinOf4WithGroupingReferencePlan() throws OptimizerException
	{
		// table scan (NATION)
		BaseTableAccess relation_2 = new BaseTableAccess(this.dbInstance.getCatalogue().getTable("NATION"));
		relation_2.setID(2);
		relation_2.setOutputCardinality(25);
		BaseTableAccess a_table_access = relation_2;
		Column[] a_columns = new Column[2];
		a_columns[0] = new Column(a_table_access, DataType.get(BasicType.INT, 1), 0);
		a_columns[1] = new Column(a_table_access, DataType.get(BasicType.INT, 1), 2);
		TableScanPlanOperator a = new TableScanPlanOperator(a_table_access, a_columns);

		// sort subplan
		int[] b_col_indices = new int[1];
		b_col_indices[0] = 1;
		boolean[] b_sort_asc = new boolean[1];
		b_sort_asc[0] = true;
		SortPlanOperator b = new SortPlanOperator(a, b_col_indices, b_sort_asc);

		// table scan (REGION)
		BaseTableAccess relation_3 = new BaseTableAccess(this.dbInstance.getCatalogue().getTable("REGION"));
		relation_3.setID(3);
		relation_3.setOutputCardinality(5);
		BaseTableAccess c_table_access = relation_3;
		Column[] c_columns = new Column[1];
		c_columns[0] = new Column(c_table_access, DataType.get(BasicType.INT, 1), 0);
		TableScanPlanOperator c = new TableScanPlanOperator(c_table_access, c_columns);

		// sort subplan
		int[] d_col_indices = new int[1];
		d_col_indices[0] = 0;
		boolean[] d_sort_asc = new boolean[1];
		d_sort_asc[0] = true;
		SortPlanOperator d = new SortPlanOperator(c, d_col_indices, d_sort_asc);

		// merge subplan
		int[] e_lj_cols = new int[1];
		e_lj_cols[0] = 1;
		int[] e_rj_cols = new int[1];
		e_rj_cols[0] = 0;
		int[] e_lc_map= new int[2];
		e_lc_map[0] = -1;
		e_lc_map[1] = 0;
		int[] e_rc_map= new int[2];
		e_rc_map[0] = 0;
		e_rc_map[1] = -1;
		MergeJoinPlanOperator e = new MergeJoinPlanOperator(b, d, null, e_lj_cols, e_rj_cols, e_lc_map, e_rc_map, 25);

		// sort subplan
		int[] f_col_indices = new int[1];
		f_col_indices[0] = 1;
		boolean[] f_sort_asc = new boolean[1];
		f_sort_asc[0] = true;
		SortPlanOperator f = new SortPlanOperator(e, f_col_indices, f_sort_asc);

		// table scan (CUSTOMER)
		BaseTableAccess relation_1 = new BaseTableAccess(this.dbInstance.getCatalogue().getTable("CUSTOMER"));
		relation_1.setID(1);
		relation_1.setOutputCardinality(3000);
		BaseTableAccess g_table_access = relation_1;
		Column[] g_columns = new Column[2];
		g_columns[0] = new Column(g_table_access, DataType.get(BasicType.INT, 1), 0);
		g_columns[1] = new Column(g_table_access, DataType.get(BasicType.INT, 1), 3);
		TableScanPlanOperator g = new TableScanPlanOperator(g_table_access, g_columns);

		// sort subplan
		int[] h_col_indices = new int[1];
		h_col_indices[0] = 1;
		boolean[] h_sort_asc = new boolean[1];
		h_sort_asc[0] = true;
		SortPlanOperator h = new SortPlanOperator(g, h_col_indices, h_sort_asc);

		// merge subplan
		int[] i_lj_cols = new int[1];
		i_lj_cols[0] = 1;
		int[] i_rj_cols = new int[1];
		i_rj_cols[0] = 1;
		int[] i_lc_map= new int[2];
		i_lc_map[0] = 0;
		i_lc_map[1] = -1;
		int[] i_rc_map= new int[2];
		i_rc_map[0] = -1;
		i_rc_map[1] = 0;
		MergeJoinPlanOperator i = new MergeJoinPlanOperator(f, h, null, i_lj_cols, i_rj_cols, i_lc_map, i_rc_map, 3000);

		// sort subplan
		int[] j_col_indices = new int[1];
		j_col_indices[0] = 1;
		boolean[] j_sort_asc = new boolean[1];
		j_sort_asc[0] = true;
		SortPlanOperator j = new SortPlanOperator(i, j_col_indices, j_sort_asc);

		// table scan (ORDERS)
		BaseTableAccess relation_0 = new BaseTableAccess(this.dbInstance.getCatalogue().getTable("ORDERS"));
		relation_0.setID(0);
		relation_0.setOutputCardinality(30000);
		BaseTableAccess k_table_access = relation_0;
		Column[] k_columns = new Column[2];
		k_columns[0] = new Column(k_table_access, DataType.get(BasicType.FLOAT, 1), 3);
		k_columns[1] = new Column(k_table_access, DataType.get(BasicType.INT, 1), 1);
		TableScanPlanOperator k = new TableScanPlanOperator(k_table_access, k_columns);

		// sort subplan
		int[] l_col_indices = new int[1];
		l_col_indices[0] = 1;
		boolean[] l_sort_asc = new boolean[1];
		l_sort_asc[0] = true;
		SortPlanOperator l = new SortPlanOperator(k, l_col_indices, l_sort_asc);

		// merge subplan
		int[] m_lj_cols = new int[1];
		m_lj_cols[0] = 1;
		int[] m_rj_cols = new int[1];
		m_rj_cols[0] = 1;
		int[] m_lc_map= new int[2];
		m_lc_map[0] = 0;
		m_lc_map[1] = -1;
		int[] m_rc_map= new int[2];
		m_rc_map[0] = -1;
		m_rc_map[1] = 0;
		MergeJoinPlanOperator m = new MergeJoinPlanOperator(j, l, null, m_lj_cols, m_rj_cols, m_lc_map, m_rc_map, 30000);

		// sort subplan
		int[] n_col_indices = new int[1];
		n_col_indices[0] = 0;
		boolean[] n_sort_asc = new boolean[1];
		n_sort_asc[0] = true;
		SortPlanOperator n = new SortPlanOperator(m, n_col_indices, n_sort_asc);

		// group by subplan
		ProducedColumn[] o_prod_columns = new ProducedColumn[2];
		Relation o_prod_column_0_relation = relation_3;
		DataType o_prod_column_0_data_type = DataType.get(BasicType.INT, 1);
		int o_prod_column_0_col_index = 0;
		String o_prod_column_0_col_alias = "regionkey";
		OutputColumn o_prod_column_0_parsed_col = null; //TODO
		OutputColumn.AggregationType o_prod_column_0_agg_fun = OutputColumn.AggregationType.NONE;
		o_prod_columns[0] = new ProducedColumn(o_prod_column_0_relation, o_prod_column_0_data_type, o_prod_column_0_col_index, o_prod_column_0_col_alias, o_prod_column_0_parsed_col, o_prod_column_0_agg_fun);
		Relation o_prod_column_1_relation = relation_0;
		DataType o_prod_column_1_data_type = DataType.get(BasicType.FLOAT, 1);
		int o_prod_column_1_col_index = 3;
		String o_prod_column_1_col_alias = "sum_totalprice";
		OutputColumn o_prod_column_1_parsed_col = null; //TODO
		OutputColumn.AggregationType o_prod_column_1_agg_fun = OutputColumn.AggregationType.SUM;
		o_prod_columns[1] = new ProducedColumn(o_prod_column_1_relation, o_prod_column_1_data_type, o_prod_column_1_col_index, o_prod_column_1_col_alias, o_prod_column_1_parsed_col, o_prod_column_1_agg_fun);

		int[] o_group_col_indices = new int[1];
		o_group_col_indices[0] = 0;

		int[] o_agg_col_indices = new int[1];
		o_agg_col_indices[0] = 1;

		GroupByPlanOperator o = new GroupByPlanOperator(n, o_prod_columns, o_group_col_indices, o_agg_col_indices, 5);

		// filter subplan
		Predicate p_pred_parsed_pred = createPredicate("sum_totalprice > 1000", true);
		Column p_pred_column = new Column(relation_0, DataType.get(BasicType.FLOAT, 1), 3);
		DataField p_pred_literal = new FloatField(1000.0f);
		LocalPredicateAtom p_pred = new LocalPredicateAtom(p_pred_parsed_pred, p_pred_column, p_pred_literal);
		FilterPlanOperator p = new FilterPlanOperator(o, p_pred);
		
		CostEstimator costEstimator = AbstractExtensionFactory.getExtensionFactory().createCostEstimator(this.config.getBlockReadCost(), this.config.getBlockWriteCost(), this.config.getBlockRandomReadOverhead(), this.config.getBlockRandomWriteOverhead());
		PhysicalPlanCostUpdater costUpdater = new PhysicalPlanCostUpdater(costEstimator);
		costUpdater.costGenericOperator(p);
		
		return p;
	}
	
	private OptimizerPlanOperator getOptimizerJoinOf5WithGroupingAndSortQuery1ReferencePlan() throws OptimizerException
	{
		// table scan (NATION)
		BaseTableAccess relation_3 = new BaseTableAccess(this.dbInstance.getCatalogue().getTable("NATION"));
		relation_3.setID(3);
		relation_3.setOutputCardinality(25);
		BaseTableAccess a_table_access = relation_3;
		Column[] a_columns = new Column[3];
		a_columns[0] = new Column(a_table_access, DataType.get(BasicType.INT, 1), 0);
		a_columns[1] = new Column(a_table_access, DataType.get(BasicType.CHAR, 25), 1);
		a_columns[2] = new Column(a_table_access, DataType.get(BasicType.INT, 1), 2);
		TableScanPlanOperator a = new TableScanPlanOperator(a_table_access, a_columns);

		// table scan (REGION)
		BaseTableAccess relation_4 = new BaseTableAccess(this.dbInstance.getCatalogue().getTable("REGION"));
		relation_4.setID(4);
		relation_4.setOutputCardinality(1);
		BaseTableAccess c_table_access = relation_4;
		Column[] c_columns = new Column[1];
		c_columns[0] = new Column(c_table_access, DataType.get(BasicType.INT, 1), 0);
		TableScanPlanOperator c = new TableScanPlanOperator(c_table_access, c_columns);
		Predicate c_pred_parsed_pred = createPredicate("r.r_name = \"EUROPE\"", false);
		Column c_pred_column = new Column(c_table_access, DataType.get(BasicType.CHAR, 25), 1);
		DataField c_pred_literal = new CharField("EUROPE                   ");
		LocalPredicateAtom c_pred = new LocalPredicateAtom(c_pred_parsed_pred, c_pred_column, c_pred_literal);
		c.setPredicate(c_pred);

		// NL subplan
		int[] e_lc_map= new int[2];
		e_lc_map[0] = 0;
		e_lc_map[1] = 1;
		int[] e_rc_map= new int[2];
		e_rc_map[0] = -1;
		e_rc_map[1] = -1;
		JoinPredicate pred = new JoinPredicateAtom(null, c, a,
				new Column(c, null, 2),
				new Column(a,null, 0));
		NestedLoopJoinPlanOperator e = new NestedLoopJoinPlanOperator(c, a, pred, e_rc_map,
				e_lc_map,	5);

		// sort subplan
		int[] f_col_indices = new int[1];
		f_col_indices[0] = 0;
		boolean[] f_sort_asc = new boolean[1];
		f_sort_asc[0] = true;
		SortPlanOperator f = new SortPlanOperator(e, f_col_indices, f_sort_asc);

		// table scan (CUSTOMER)
		BaseTableAccess relation_2 = new BaseTableAccess(this.dbInstance.getCatalogue().getTable("CUSTOMER"));
		relation_2.setID(2);
		relation_2.setOutputCardinality(3000);
		BaseTableAccess g_table_access = relation_2;
		Column[] g_columns = new Column[2];
		g_columns[0] = new Column(g_table_access, DataType.get(BasicType.INT, 1), 0);
		g_columns[1] = new Column(g_table_access, DataType.get(BasicType.INT, 1), 3);
		TableScanPlanOperator g = new TableScanPlanOperator(g_table_access, g_columns);

		// sort subplan
		int[] h_col_indices = new int[1];
		h_col_indices[0] = 1;
		boolean[] h_sort_asc = new boolean[1];
		h_sort_asc[0] = true;
		SortPlanOperator h = new SortPlanOperator(g, h_col_indices, h_sort_asc);

		// merge subplan
		int[] i_lj_cols = new int[1];
		i_lj_cols[0] = 0;
		int[] i_rj_cols = new int[1];
		i_rj_cols[0] = 1;
		int[] i_lc_map= new int[3];
		i_lc_map[0] = 0;
		i_lc_map[1] = 1;
		i_lc_map[2] = -1;
		int[] i_rc_map= new int[3];
		i_rc_map[0] = -1;
		i_rc_map[1] = -1;
		i_rc_map[2] = 0;
		MergeJoinPlanOperator i = new MergeJoinPlanOperator(f, h, null, i_lj_cols, i_rj_cols, i_lc_map, i_rc_map, 600);

		// sort subplan
		int[] j_col_indices = new int[1];
		j_col_indices[0] = 2;
		boolean[] j_sort_asc = new boolean[1];
		j_sort_asc[0] = true;
		SortPlanOperator j = new SortPlanOperator(i, j_col_indices, j_sort_asc);

		// table scan (ORDERS)
		BaseTableAccess relation_1 = new BaseTableAccess(this.dbInstance.getCatalogue().getTable("ORDERS"));
		relation_1.setID(1);
		relation_1.setOutputCardinality(30000);
		BaseTableAccess k_table_access = relation_1;
		Column[] k_columns = new Column[2];
		k_columns[0] = new Column(k_table_access, DataType.get(BasicType.INT, 1), 0);
		k_columns[1] = new Column(k_table_access, DataType.get(BasicType.INT, 1), 1);
		TableScanPlanOperator k = new TableScanPlanOperator(k_table_access, k_columns);

		// sort subplan
		int[] l_col_indices = new int[1];
		l_col_indices[0] = 1;
		boolean[] l_sort_asc = new boolean[1];
		l_sort_asc[0] = true;
		SortPlanOperator l = new SortPlanOperator(k, l_col_indices, l_sort_asc);

		// merge subplan
		int[] m_lj_cols = new int[1];
		m_lj_cols[0] = 2;
		int[] m_rj_cols = new int[1];
		m_rj_cols[0] = 1;
		int[] m_lc_map= new int[3];
		m_lc_map[0] = 0;
		m_lc_map[1] = 1;
		m_lc_map[2] = -1;
		int[] m_rc_map= new int[3];
		m_rc_map[0] = -1;
		m_rc_map[1] = -1;
		m_rc_map[2] = 0;
		MergeJoinPlanOperator m = new MergeJoinPlanOperator(j, l, null, m_lj_cols, m_rj_cols, m_lc_map, m_rc_map, 6000);

		// sort subplan
		int[] n_col_indices = new int[1];
		n_col_indices[0] = 2;
		boolean[] n_sort_asc = new boolean[1];
		n_sort_asc[0] = true;
		SortPlanOperator n = new SortPlanOperator(m, n_col_indices, n_sort_asc);

		// table scan (LINEITEM)
		BaseTableAccess relation_0 = new BaseTableAccess(this.dbInstance.getCatalogue().getTable("LINEITEM"));
		relation_0.setID(0);
		relation_0.setOutputCardinality(120515);
		BaseTableAccess o_table_access = relation_0;
		Column[] o_columns = new Column[2];
		o_columns[0] = new Column(o_table_access, DataType.get(BasicType.FLOAT, 1), 5);
		o_columns[1] = new Column(o_table_access, DataType.get(BasicType.INT, 1), 0);
		TableScanPlanOperator o = new TableScanPlanOperator(o_table_access, o_columns);

		// sort subplan
		int[] p_col_indices = new int[1];
		p_col_indices[0] = 1;
		boolean[] p_sort_asc = new boolean[1];
		p_sort_asc[0] = true;
		SortPlanOperator p = new SortPlanOperator(o, p_col_indices, p_sort_asc);

		// merge subplan
		int[] q_lj_cols = new int[1];
		q_lj_cols[0] = 2;
		int[] q_rj_cols = new int[1];
		q_rj_cols[0] = 1;
		int[] q_lc_map= new int[3];
		q_lc_map[0] = 0;
		q_lc_map[1] = 1;
		q_lc_map[2] = -1;
		int[] q_rc_map= new int[3];
		q_rc_map[0] = -1;
		q_rc_map[1] = -1;
		q_rc_map[2] = 0;
		MergeJoinPlanOperator q = new MergeJoinPlanOperator(n, p, null, q_lj_cols, q_rj_cols, q_lc_map, q_rc_map, 24103);

		// sort subplan
		int[] r_col_indices = new int[2];
		r_col_indices[0] = 0;
		r_col_indices[1] = 1;
		boolean[] r_sort_asc = new boolean[2];
		r_sort_asc[0] = true;
		r_sort_asc[1] = false;
		SortPlanOperator r = new SortPlanOperator(q, r_col_indices, r_sort_asc);

		// group by subplan
		ProducedColumn[] s_prod_columns = new ProducedColumn[3];
		Relation s_prod_column_0_relation = relation_3;
		DataType s_prod_column_0_data_type = DataType.get(BasicType.INT, 1);
		int s_prod_column_0_col_index = 0;
		String s_prod_column_0_col_alias = "n_nationkey";
		OutputColumn s_prod_column_0_parsed_col = null; //TODO
		OutputColumn.AggregationType s_prod_column_0_agg_fun = OutputColumn.AggregationType.NONE;
		s_prod_columns[0] = new ProducedColumn(s_prod_column_0_relation, s_prod_column_0_data_type, s_prod_column_0_col_index, s_prod_column_0_col_alias, s_prod_column_0_parsed_col, s_prod_column_0_agg_fun);
		Relation s_prod_column_1_relation = relation_3;
		DataType s_prod_column_1_data_type = DataType.get(BasicType.CHAR, 25);
		int s_prod_column_1_col_index = 1;
		String s_prod_column_1_col_alias = "n_name";
		OutputColumn s_prod_column_1_parsed_col = null; //TODO
		OutputColumn.AggregationType s_prod_column_1_agg_fun = OutputColumn.AggregationType.NONE;
		s_prod_columns[1] = new ProducedColumn(s_prod_column_1_relation, s_prod_column_1_data_type, s_prod_column_1_col_index, s_prod_column_1_col_alias, s_prod_column_1_parsed_col, s_prod_column_1_agg_fun);
		Relation s_prod_column_2_relation = relation_0;
		DataType s_prod_column_2_data_type = DataType.get(BasicType.FLOAT, 1);
		int s_prod_column_2_col_index = 5;
		String s_prod_column_2_col_alias = "sum_extendedprice";
		OutputColumn s_prod_column_2_parsed_col = null; //TODO
		OutputColumn.AggregationType s_prod_column_2_agg_fun = OutputColumn.AggregationType.SUM;
		s_prod_columns[2] = new ProducedColumn(s_prod_column_2_relation, s_prod_column_2_data_type, s_prod_column_2_col_index, s_prod_column_2_col_alias, s_prod_column_2_parsed_col, s_prod_column_2_agg_fun);

		int[] s_group_col_indices = new int[2];
		s_group_col_indices[0] = 0;
		s_group_col_indices[1] = 1;

		int[] s_agg_col_indices = new int[1];
		s_agg_col_indices[0] = 2;

		GroupByPlanOperator s = new GroupByPlanOperator(r, s_prod_columns, s_group_col_indices, s_agg_col_indices, 625);

		// fetch subplan
		Predicate t_pred_parsed_pred = createPredicate("sum_extendedprice > 1000", true);
		Column t_pred_column = new Column(relation_0, DataType.get(BasicType.FLOAT, 1), 5);
		DataField t_pred_literal = new FloatField(1000.0f);
		LocalPredicateAtom t_pred = new LocalPredicateAtom(t_pred_parsed_pred, t_pred_column, t_pred_literal);
		FilterPlanOperator t = new FilterPlanOperator(s, t_pred);
		
		CostEstimator costEstimator = AbstractExtensionFactory.getExtensionFactory().createCostEstimator(this.config.getBlockReadCost(), this.config.getBlockWriteCost(), this.config.getBlockRandomReadOverhead(), this.config.getBlockRandomWriteOverhead());
		PhysicalPlanCostUpdater costUpdater = new PhysicalPlanCostUpdater(costEstimator);
		costUpdater.costGenericOperator(t);
		
		return t;
	}
	
	private OptimizerPlanOperator getOptimizerJoinOf5WithGroupingAndSortQuery2ReferencePlan() throws OptimizerException
	{
		// table scan (NATION)
		BaseTableAccess relation_3 = new BaseTableAccess(this.dbInstance.getCatalogue().getTable("NATION"));
		relation_3.setID(3);
		relation_3.setOutputCardinality(25);
		BaseTableAccess a_table_access = relation_3;
		Column[] a_columns = new Column[3];
		a_columns[0] = new Column(a_table_access, DataType.get(BasicType.INT, 1), 0);
		a_columns[1] = new Column(a_table_access, DataType.get(BasicType.CHAR, 25), 1);
		a_columns[2] = new Column(a_table_access, DataType.get(BasicType.INT, 1), 2);
		TableScanPlanOperator a = new TableScanPlanOperator(a_table_access, a_columns);

		// table scan (REGION)
		BaseTableAccess relation_4 = new BaseTableAccess(this.dbInstance.getCatalogue().getTable("REGION"));
		relation_4.setID(4);
		relation_4.setOutputCardinality(1);
		BaseTableAccess c_table_access = relation_4;
		Column[] c_columns = new Column[1];
		c_columns[0] = new Column(c_table_access, DataType.get(BasicType.INT, 1), 0);
		TableScanPlanOperator c = new TableScanPlanOperator(c_table_access, c_columns);
		Predicate c_pred_parsed_pred = createPredicate("r.r_name = \"EUROPE\"", false);
		Column c_pred_column = new Column(c_table_access, DataType.get(BasicType.CHAR, 25), 1);
		DataField c_pred_literal = new CharField("EUROPE                   ");
		LocalPredicateAtom c_pred = new LocalPredicateAtom(c_pred_parsed_pred, c_pred_column, c_pred_literal);
		c.setPredicate(c_pred);

		// NL subplan
		int[] e_lc_map= new int[2];
		e_lc_map[0] = 0;
		e_lc_map[1] = 1;
		int[] e_rc_map= new int[2];
		e_rc_map[0] = -1;
		e_rc_map[1] = -1;
		JoinPredicate pred = new JoinPredicateAtom(null, c, a,
				new Column(c, null, 2),
				new Column(a,null, 0));
		NestedLoopJoinPlanOperator e = new NestedLoopJoinPlanOperator(c, a, pred, e_rc_map,
				e_lc_map,	5);

		// sort subplan
		int[] f_col_indices = new int[1];
		f_col_indices[0] = 0;
		boolean[] f_sort_asc = new boolean[1];
		f_sort_asc[0] = true;
		SortPlanOperator f = new SortPlanOperator(e, f_col_indices, f_sort_asc);

		// table scan (CUSTOMER)
		BaseTableAccess relation_2 = new BaseTableAccess(this.dbInstance.getCatalogue().getTable("CUSTOMER"));
		relation_2.setID(2);
		relation_2.setOutputCardinality(3000);
		BaseTableAccess g_table_access = relation_2;
		Column[] g_columns = new Column[2];
		g_columns[0] = new Column(g_table_access, DataType.get(BasicType.INT, 1), 0);
		g_columns[1] = new Column(g_table_access, DataType.get(BasicType.INT, 1), 3);
		TableScanPlanOperator g = new TableScanPlanOperator(g_table_access, g_columns);

		// sort subplan
		int[] h_col_indices = new int[1];
		h_col_indices[0] = 1;
		boolean[] h_sort_asc = new boolean[1];
		h_sort_asc[0] = true;
		SortPlanOperator h = new SortPlanOperator(g, h_col_indices, h_sort_asc);

		// merge join subplan
		int[] i_lj_cols = new int[1];
		i_lj_cols[0] = 0;
		int[] i_rj_cols = new int[1];
		i_rj_cols[0] = 1;
		int[] i_lc_map = new int[3];
		i_lc_map[0] = 0;
		i_lc_map[1] = 1;
		i_lc_map[2] = -1;
		int[] i_rc_map = new int[3];
		i_rc_map[0] = -1;
		i_rc_map[1] = -1;
		i_rc_map[2] = 0;
		MergeJoinPlanOperator i = new MergeJoinPlanOperator(f, h, null, i_lj_cols, i_rj_cols, i_lc_map, i_rc_map, 600);

		// sort subplan
		int[] j_col_indices = new int[1];
		j_col_indices[0] = 2;
		boolean[] j_sort_asc = new boolean[1];
		j_sort_asc[0] = true;
		SortPlanOperator j = new SortPlanOperator(i, j_col_indices, j_sort_asc);

		// table scan (ORDERS)
		BaseTableAccess relation_1 = new BaseTableAccess(this.dbInstance.getCatalogue().getTable("ORDERS"));
		relation_1.setID(1);
		relation_1.setOutputCardinality(30000);
		BaseTableAccess k_table_access = relation_1;
		Column[] k_columns = new Column[2];
		k_columns[0] = new Column(k_table_access, DataType.get(BasicType.INT, 1), 0);
		k_columns[1] = new Column(k_table_access, DataType.get(BasicType.INT, 1), 1);
		TableScanPlanOperator k = new TableScanPlanOperator(k_table_access, k_columns);

		// sort subplan
		int[] l_col_indices = new int[1];
		l_col_indices[0] = 1;
		boolean[] l_sort_asc = new boolean[1];
		l_sort_asc[0] = true;
		SortPlanOperator l = new SortPlanOperator(k, l_col_indices, l_sort_asc);

		// merge join subplan
		int[] m_lj_cols = new int[1];
		m_lj_cols[0] = 2;
		int[] m_rj_cols = new int[1];
		m_rj_cols[0] = 1;
		int[] m_lc_map = new int[3];
		m_lc_map[0] = 0;
		m_lc_map[1] = 1;
		m_lc_map[2] = -1;
		int[] m_rc_map = new int[3];
		m_rc_map[0] = -1;
		m_rc_map[1] = -1;
		m_rc_map[2] = 0;
		MergeJoinPlanOperator m = new MergeJoinPlanOperator(j, l, null, m_lj_cols, m_rj_cols, m_lc_map, m_rc_map, 6000);

		// sort subplan
		int[] n_col_indices = new int[1];
		n_col_indices[0] = 2;
		boolean[] n_sort_asc = new boolean[1];
		n_sort_asc[0] = true;
		SortPlanOperator n = new SortPlanOperator(m, n_col_indices, n_sort_asc);

		// table scan (LINEITEM)
		BaseTableAccess relation_0 = new BaseTableAccess(this.dbInstance.getCatalogue().getTable("LINEITEM"));
		relation_0.setID(0);
		relation_0.setOutputCardinality(120515);
		BaseTableAccess o_table_access = relation_0;
		Column[] o_columns = new Column[2];
		o_columns[0] = new Column(o_table_access, DataType.get(BasicType.FLOAT, 1), 5);
		o_columns[1] = new Column(o_table_access, DataType.get(BasicType.INT, 1), 0);
		TableScanPlanOperator o = new TableScanPlanOperator(o_table_access, o_columns);

		// sort subplan
		int[] p_col_indices = new int[1];
		p_col_indices[0] = 1;
		boolean[] p_sort_asc = new boolean[1];
		p_sort_asc[0] = true;
		SortPlanOperator p = new SortPlanOperator(o, p_col_indices, p_sort_asc);

		// merge join subplan
		int[] q_lj_cols = new int[1];
		q_lj_cols[0] = 2;
		int[] q_rj_cols = new int[1];
		q_rj_cols[0] = 1;
		int[] q_lc_map = new int[3];
		q_lc_map[0] = 0;
		q_lc_map[1] = 1;
		q_lc_map[2] = -1;
		int[] q_rc_map = new int[3];
		q_rc_map[0] = -1;
		q_rc_map[1] = -1;
		q_rc_map[2] = 0;
		MergeJoinPlanOperator q = new MergeJoinPlanOperator(n, p, null, q_lj_cols, q_rj_cols, q_lc_map, q_rc_map, 24103);

		// sort subplan
		int[] r_col_indices = new int[2];
		r_col_indices[0] = 0;
		r_col_indices[1] = 1;
		boolean[] r_sort_asc = new boolean[2];
		r_sort_asc[0] = true;
		r_sort_asc[1] = true;
		SortPlanOperator r = new SortPlanOperator(q, r_col_indices, r_sort_asc);

		// group by subplan
		ProducedColumn[] s_prod_columns = new ProducedColumn[3];
		Relation s_prod_column_0_relation = relation_3;
		DataType s_prod_column_0_data_type = DataType.get(BasicType.INT, 1);
		int s_prod_column_0_col_index = 0;
		String s_prod_column_0_col_alias = "n_nationkey";
		OutputColumn s_prod_column_0_parsed_col = null; //TODO
		OutputColumn.AggregationType s_prod_column_0_agg_fun = OutputColumn.AggregationType.NONE;
		s_prod_columns[0] = new ProducedColumn(s_prod_column_0_relation, s_prod_column_0_data_type, s_prod_column_0_col_index, s_prod_column_0_col_alias, s_prod_column_0_parsed_col, s_prod_column_0_agg_fun);
		Relation s_prod_column_1_relation = relation_3;
		DataType s_prod_column_1_data_type = DataType.get(BasicType.CHAR, 25);
		int s_prod_column_1_col_index = 1;
		String s_prod_column_1_col_alias = "n_name";
		OutputColumn s_prod_column_1_parsed_col = null; //TODO
		OutputColumn.AggregationType s_prod_column_1_agg_fun = OutputColumn.AggregationType.NONE;
		s_prod_columns[1] = new ProducedColumn(s_prod_column_1_relation, s_prod_column_1_data_type, s_prod_column_1_col_index, s_prod_column_1_col_alias, s_prod_column_1_parsed_col, s_prod_column_1_agg_fun);
		Relation s_prod_column_2_relation = relation_0;
		DataType s_prod_column_2_data_type = DataType.get(BasicType.FLOAT, 1);
		int s_prod_column_2_col_index = 5;
		String s_prod_column_2_col_alias = "sum_extendedprice";
		OutputColumn s_prod_column_2_parsed_col = null; //TODO
		OutputColumn.AggregationType s_prod_column_2_agg_fun = OutputColumn.AggregationType.SUM;
		s_prod_columns[2] = new ProducedColumn(s_prod_column_2_relation, s_prod_column_2_data_type, s_prod_column_2_col_index, s_prod_column_2_col_alias, s_prod_column_2_parsed_col, s_prod_column_2_agg_fun);

		int[] s_group_col_indices = new int[2];
		s_group_col_indices[0] = 0;
		s_group_col_indices[1] = 1;

		int[] s_agg_col_indices = new int[1];
		s_agg_col_indices[0] = 2;

		GroupByPlanOperator s = new GroupByPlanOperator(r, s_prod_columns, s_group_col_indices, s_agg_col_indices, 625);

		// filter subplan
		Predicate t_pred_parsed_pred = createPredicate("sum_extendedprice > 1000", true);
		Column t_pred_column = new Column(relation_0, DataType.get(BasicType.FLOAT, 1), 5);
		DataField t_pred_literal = new FloatField(1000.0f);
		LocalPredicateAtom t_pred = new LocalPredicateAtom(t_pred_parsed_pred, t_pred_column, t_pred_literal);
		FilterPlanOperator t = new FilterPlanOperator(s, t_pred);

		// sort subplan
		int[] u_col_indices = new int[3];
		u_col_indices[0] = 0;
		u_col_indices[1] = 1;
		u_col_indices[2] = 2;
		boolean[] u_sort_asc = new boolean[3];
		u_sort_asc[0] = true;
		u_sort_asc[1] = false;
		u_sort_asc[2] = true;
		SortPlanOperator u = new SortPlanOperator(t, u_col_indices, u_sort_asc);
		
		CostEstimator costEstimator = AbstractExtensionFactory.getExtensionFactory().createCostEstimator(this.config.getBlockReadCost(), this.config.getBlockWriteCost(), this.config.getBlockRandomReadOverhead(), this.config.getBlockRandomWriteOverhead());
		PhysicalPlanCostUpdater costUpdater = new PhysicalPlanCostUpdater(costEstimator);
		costUpdater.costGenericOperator(u);

		return u;
	}
	
	// -------------------------------------------------------------------------
	//                       Helper functions
	// -------------------------------------------------------------------------

	private static Predicate createPredicate(String text, boolean inHaving)
	{
		try {
			SQLTokenizer tokenizer = new SQLTokenizer(text);
			tokenizer.nextToken();
			
			SQLParserImpl parser = new SQLParserImpl(null);
			return parser.parsePredicate(tokenizer, inHaving);
		}
		catch (ParseException e) {
			System.out.println(e);
			return null;
		}
	}
}
