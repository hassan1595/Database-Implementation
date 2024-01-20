package de.tuberlin.dima.minidb.test.optimizer;


import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import de.tuberlin.dima.minidb.Config;
import de.tuberlin.dima.minidb.DBInstance;
import de.tuberlin.dima.minidb.api.AbstractExtensionFactory;
import de.tuberlin.dima.minidb.core.CharField;
import de.tuberlin.dima.minidb.core.DataType;
import de.tuberlin.dima.minidb.core.IntField;
import de.tuberlin.dima.minidb.optimizer.AbstractJoinPlanOperator;
import de.tuberlin.dima.minidb.optimizer.JoinOrderVerifier;
import de.tuberlin.dima.minidb.optimizer.Optimizer;
import de.tuberlin.dima.minidb.optimizer.OptimizerException;
import de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator;
import de.tuberlin.dima.minidb.optimizer.cardinality.CardinalityEstimator;
import de.tuberlin.dima.minidb.optimizer.joins.util.JoinOrderOptimizerUtils;
import de.tuberlin.dima.minidb.optimizer.joins.util.LogicalPlanPrinter;
import de.tuberlin.dima.minidb.parser.ParseException;
import de.tuberlin.dima.minidb.parser.Predicate;
import de.tuberlin.dima.minidb.parser.SQLTokenizer;
import de.tuberlin.dima.minidb.parser.SelectQuery;
import de.tuberlin.dima.minidb.semantics.AnalyzedSelectQuery;
import de.tuberlin.dima.minidb.semantics.BaseTableAccess;
import de.tuberlin.dima.minidb.semantics.Column;
import de.tuberlin.dima.minidb.semantics.JoinGraphEdge;
import de.tuberlin.dima.minidb.semantics.Relation;
import de.tuberlin.dima.minidb.semantics.predicate.JoinPredicate;
import de.tuberlin.dima.minidb.semantics.predicate.JoinPredicateAtom;
import de.tuberlin.dima.minidb.semantics.predicate.JoinPredicateConjunct;
import de.tuberlin.dima.minidb.semantics.predicate.LocalPredicate;
import de.tuberlin.dima.minidb.semantics.predicate.LocalPredicateAtom;
import de.tuberlin.dima.minidb.semantics.predicate.LocalPredicateBetween;

//Note: The following imports require the reference solutions of all previous tasks.
import de.tuberlin.dima.minidb.semantics.solution.SelectQueryAnalyzerImpl;
import de.tuberlin.dima.minidb.parser.solution.SQLParserImpl;

public class
TestJoinOrderOptimizerStudents
{
	/**
	 * Location of the configuration file for the database instance.
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
	 * The cardinality estimator for local an join cardinalities.
	 */
	protected CardinalityEstimator cardEstimator;

	/**
	 * The database instance to perform the tests on.
	 */
	protected DBInstance dbInstance = null;
	
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
		// get the class name of the extension factory class
		AbstractExtensionFactory.initializeDefault();

		// initialize a database instance and start it
		this.dbInstance = new DBInstance(OUT_LOGGER, CONFIG_FILE_NAME, CATALOGUE_FILE_NAME);
		int returncode = this.dbInstance.startInstance(); 
		if (returncode != DBInstance.RETURN_CODE_OKAY) {
			throw new Exception("DBInstance could not be started." + returncode);
		}
		
		Config cfg = this.dbInstance.getConfig();
		this.cardEstimator = AbstractExtensionFactory.getExtensionFactory().createCardinalityEstimator();
		this.optimizer = new Optimizer(this.dbInstance.getCatalogue(),
				cfg.getBlockReadCost(), cfg.getBlockWriteCost(),
				cfg.getBlockRandomReadOverhead(), cfg.getBlockRandomWriteOverhead());
	}

	@After
	public void tearDown() throws Exception {
		// check if instance is running
		if (this.dbInstance != null && this.dbInstance.isRunning()) {
			// stop running instance
			this.dbInstance.shutdownInstance(false);
		}
	}
	
	// ------------------------------------------------------------------------
	
	/**
	 * Simple join of Nation, Region, ParSupp and Supplier. Predicate on Region.
	 */
	@Test
	public void testOptimizerJoinOf4WithPreds() throws Exception
	{
		// --------------------------------------------------------------------
		//                         Prepare the query
		// --------------------------------------------------------------------
		
		// create the base relations
		BaseTableAccess suppTable = new BaseTableAccess(this.dbInstance.getCatalogue().getTable("SUPPLIER"));
		BaseTableAccess partSuppTable = new BaseTableAccess(this.dbInstance.getCatalogue().getTable("PARTSUPPLIER"));
		BaseTableAccess nationTable = new BaseTableAccess(this.dbInstance.getCatalogue().getTable("NATION"));
		BaseTableAccess regionTable = new BaseTableAccess(this.dbInstance.getCatalogue().getTable("REGION"));

		LocalPredicate partSuppPred = new LocalPredicateBetween(new Column(partSuppTable, DataType.intType(), 0), createPredicate("PS.PARTKEY >= 1000"), createPredicate("PS.PARTKEY < 1005"), new IntField(1000), new IntField(1005));
		partSuppTable.setPredicate(partSuppPred);
		
		LocalPredicate regionPred = new LocalPredicateAtom(createPredicate("R.NAME = \"AFRICA\""), new Column(regionTable, DataType.charType(25), 1), new CharField("AFRICA                   "));
		regionTable.setPredicate(regionPred);
		
		BaseTableAccess[] tables = new BaseTableAccess[] { suppTable, partSuppTable, nationTable, regionTable };
		
		JoinPredicate region2nationPred = new JoinPredicateAtom(createPredicate("r.regionkey = n.regionkey"),
				regionTable, nationTable, new Column(regionTable, DataType.intType(), 0),
				new Column(nationTable, DataType.intType(), 2));
		
		JoinPredicate nation2suppPred = new JoinPredicateAtom(createPredicate("n.nationkey = s.nationkey"),
				nationTable, suppTable, new Column(nationTable, DataType.intType(), 0),
				new Column(suppTable, DataType.intType(), 3));
		
		JoinPredicate partsupp2suppPred = new JoinPredicateAtom(createPredicate("ps.suppkey = s.suppkey"),
				partSuppTable, suppTable, new Column(partSuppTable, DataType.intType(), 1),
				new Column(suppTable, DataType.intType(), 0));
		
		JoinGraphEdge[] edges = new JoinGraphEdge[] {
				new JoinGraphEdge(regionTable, nationTable, region2nationPred),
				new JoinGraphEdge(nationTable, suppTable, nation2suppPred), 
				new JoinGraphEdge(partSuppTable, suppTable, partsupp2suppPred) };
		
		AnalyzedSelectQuery query = new AnalyzedSelectQuery(tables, edges);
		
		// --------------------------------------------------------------------
		//                         Prepare the query
		// --------------------------------------------------------------------
		
		TestingJoinOrderVerifyer ver = new TestingJoinOrderVerifyer();
		ver.setExpectedJoinOrder(query, new AbstractJoinPlanOperator(
											new AbstractJoinPlanOperator(
												regionTable, 
												nationTable, 
												JoinOrderOptimizerUtils.filterTwinPredicates(region2nationPred)),
											new AbstractJoinPlanOperator(
												partSuppTable, 
												suppTable, 
												JoinOrderOptimizerUtils.filterTwinPredicates(partsupp2suppPred)), 
											JoinOrderOptimizerUtils.filterTwinPredicates(nation2suppPred)));
		
		// --------------------------------------------------------------------
		//                       Call the optimizer code
		// --------------------------------------------------------------------
		
		this.optimizer.createSelectQueryPlan(query, ver, true);
	}
	
	
	private static boolean containsRelation(OptimizerPlanOperator op, Relation rel) {
		if(op instanceof AbstractJoinPlanOperator) {
			return containsRelation(((AbstractJoinPlanOperator) op).getLeftChild(), rel) || containsRelation(((AbstractJoinPlanOperator) op).getRightChild(), rel);
		}
		if(op instanceof Relation) {
			return (op.equals(rel));
		}
		return false;
	}
	
	/*
	 * Switch predicate to retain the right order
	 */
	private static JoinPredicate getRightSwitchedPredicate(OptimizerPlanOperator l, OptimizerPlanOperator r, JoinPredicate joinPredicate) {
		if(joinPredicate instanceof JoinPredicateAtom){
			JoinPredicateAtom atom = (JoinPredicateAtom) joinPredicate;
			if(containsRelation(r,atom.getRightHandOriginatingTable()) || containsRelation(l, atom.getLeftHandOriginatingTable())){
				return atom;
			} else {
				return atom.createSideSwitchedCopy();
			}
		}
		if(joinPredicate instanceof JoinPredicateConjunct){
			JoinPredicateConjunct conj = (JoinPredicateConjunct) joinPredicate;
			
			JoinPredicateConjunct newConj = new JoinPredicateConjunct();
			
			for(JoinPredicateAtom atom : conj.getConjunctiveFactors()){
				newConj.addJoinPredicate(getRightSwitchedPredicate(l, r, atom));
			}
			return newConj;
		}
		return null;
	}
	
	
	/**
	 * Simple join of ParSupp, Supplier, Lineitem and Orders. No predicates.
	 */
	@Test
	public void testOptimizerJoinOf4() throws Exception
	{
		// --------------------------------------------------------------------
		//                         Prepare the query
		// --------------------------------------------------------------------
		
		// create the base relations
		BaseTableAccess ordersTable = new BaseTableAccess(this.dbInstance.getCatalogue().getTable("ORDERS"));
		BaseTableAccess lineitemTable = new BaseTableAccess(this.dbInstance.getCatalogue().getTable("LINEITEM"));
		BaseTableAccess suppTable = new BaseTableAccess(this.dbInstance.getCatalogue().getTable("SUPPLIER"));
		BaseTableAccess partSuppTable = new BaseTableAccess(this.dbInstance.getCatalogue().getTable("PARTSUPPLIER"));
		BaseTableAccess[] tables = new BaseTableAccess[] { ordersTable, lineitemTable, suppTable, partSuppTable };
		
		JoinPredicate partsupp2suppPred = new JoinPredicateAtom(createPredicate("ps.suppkey = s.suppkey"),
				partSuppTable, suppTable, new Column(partSuppTable, DataType.intType(), 1),
				new Column(lineitemTable, DataType.intType(), 0));
		
		JoinPredicate partsupp2lisuppPred1 = new JoinPredicateAtom(createPredicate("ps.suppkey = l.suppkey"),
				partSuppTable, lineitemTable, new Column(partSuppTable, DataType.intType(), 1),
				new Column(lineitemTable, DataType.intType(), 2)); 
		JoinPredicate partsupp2lisuppPred2 = new JoinPredicateAtom(createPredicate("ps.partkey = l.partkey"),
				partSuppTable, lineitemTable, new Column(partSuppTable, DataType.intType(), 0),
				new Column(lineitemTable, DataType.intType(), 1)); 
		JoinPredicateConjunct partsupp2lisuppPred = new JoinPredicateConjunct();
		partsupp2lisuppPred.addJoinPredicate(partsupp2lisuppPred1);
		partsupp2lisuppPred.addJoinPredicate(partsupp2lisuppPred2);
		
		JoinPredicate supp2lisuppPred = new JoinPredicateAtom(createPredicate("s.suppkey = l.suppkey"),
				suppTable, lineitemTable, new Column(suppTable, DataType.intType(), 1),
				new Column(lineitemTable, DataType.intType(), 2));
		
		JoinPredicate order2liorderPred = new JoinPredicateAtom(createPredicate("o.orderkey = l.orderkey"),
				ordersTable, lineitemTable, new Column(ordersTable, DataType.intType(), 1),
				new Column(lineitemTable, DataType.intType(), 0));
		
		JoinGraphEdge[] edges = new JoinGraphEdge[] {
				new JoinGraphEdge(ordersTable, lineitemTable, order2liorderPred),
				new JoinGraphEdge(partSuppTable, lineitemTable, partsupp2lisuppPred),
				new JoinGraphEdge(suppTable, lineitemTable, supp2lisuppPred),
				new JoinGraphEdge(partSuppTable, suppTable, partsupp2suppPred) };
	
		AnalyzedSelectQuery query = new AnalyzedSelectQuery(tables, edges);
		
		// --------------------------------------------------------------------
		//                         Prepare the query
		// --------------------------------------------------------------------
		JoinPredicateConjunct joinVerPred = new JoinPredicateConjunct();
		joinVerPred.addJoinPredicate(supp2lisuppPred);
		joinVerPred.addJoinPredicate(partsupp2lisuppPred1);
		joinVerPred.addJoinPredicate(partsupp2lisuppPred2);		
		TestingJoinOrderVerifyer ver = new TestingJoinOrderVerifyer();
		ver.setExpectedJoinOrder(query, new AbstractJoinPlanOperator(
											new AbstractJoinPlanOperator(
												new AbstractJoinPlanOperator(
														partSuppTable,
														suppTable, 
														JoinOrderOptimizerUtils.filterTwinPredicates(partsupp2suppPred)), 
												lineitemTable, 
												JoinOrderOptimizerUtils.filterTwinPredicates(joinVerPred)), 
											ordersTable, 
											JoinOrderOptimizerUtils.filterTwinPredicates(order2liorderPred.createSideSwitchedCopy())));

		// --------------------------------------------------------------------
		//                       Call the optimizer code
		// --------------------------------------------------------------------
		
		this.optimizer.createSelectQueryPlan(query, ver, true);
	}	

	
	/**
	 * Simple join of ParSupp, Supplier, Lineitem and Orders. No predicates.
	 */
	@Test
	public void testOptimizerJoinOf8() throws Exception
	{
		// --------------------------------------------------------------------
		//                         Prepare the query
		// --------------------------------------------------------------------
		
		// create the base relations
		BaseTableAccess regionTable = new BaseTableAccess(this.dbInstance.getCatalogue().getTable("REGION"));
		BaseTableAccess nationTable = new BaseTableAccess(this.dbInstance.getCatalogue().getTable("NATION"));
		BaseTableAccess customerTable = new BaseTableAccess(this.dbInstance.getCatalogue().getTable("CUSTOMER"));
		BaseTableAccess ordersTable = new BaseTableAccess(this.dbInstance.getCatalogue().getTable("ORDERS"));
		BaseTableAccess lineitemTable = new BaseTableAccess(this.dbInstance.getCatalogue().getTable("LINEITEM"));
		BaseTableAccess partSuppTable = new BaseTableAccess(this.dbInstance.getCatalogue().getTable("PARTSUPPLIER"));
		BaseTableAccess suppTable = new BaseTableAccess(this.dbInstance.getCatalogue().getTable("SUPPLIER"));
		BaseTableAccess partTable = new BaseTableAccess(this.dbInstance.getCatalogue().getTable("PART"));
		BaseTableAccess[] tables = new BaseTableAccess[] { 
				regionTable, nationTable, customerTable, ordersTable, 
				lineitemTable, partSuppTable, suppTable, partTable };
		
		// part supplier join predicates
		JoinPredicate partsupp2suppPred = new JoinPredicateAtom(createPredicate("ps.suppkey = s.suppkey"),
				partSuppTable, suppTable, new Column(partSuppTable, DataType.intType(), 1),
				new Column(suppTable, DataType.intType(), 0));
		JoinPredicate partsupp2partPred = new JoinPredicateAtom(createPredicate("ps.partkey = p.partkey"),
				partSuppTable, partTable, new Column(partSuppTable, DataType.intType(), 0),
				new Column(partTable, DataType.intType(), 0));
		JoinPredicate partsupp2lisuppPred1 = new JoinPredicateAtom(createPredicate("ps.suppkey = l.suppkey"),
				partSuppTable, lineitemTable, new Column(partSuppTable, DataType.intType(), 1),
				new Column(lineitemTable, DataType.intType(), 2)); 
		JoinPredicate partsupp2lisuppPred2 = new JoinPredicateAtom(createPredicate("ps.partkey = l.partkey"),
				partSuppTable, lineitemTable, new Column(partSuppTable, DataType.intType(), 0),
				new Column(lineitemTable, DataType.intType(), 1)); 
		JoinPredicateConjunct partsupp2lisuppPred = new JoinPredicateConjunct();
		partsupp2lisuppPred.addJoinPredicate(partsupp2lisuppPred1);
		partsupp2lisuppPred.addJoinPredicate(partsupp2lisuppPred2);
		
		// additional lineitem join predicates
		JoinPredicate supp2lisuppPred = new JoinPredicateAtom(createPredicate("s.suppkey = l.suppkey"),
				suppTable, lineitemTable, new Column(suppTable, DataType.intType(), 0),
				new Column(lineitemTable, DataType.intType(), 2));
		JoinPredicate part2lipartPred = new JoinPredicateAtom(createPredicate("p.partkey = l.partkey"),
				partTable, lineitemTable, new Column(partTable, DataType.intType(), 0),
				new Column(lineitemTable, DataType.intType(), 1));
		JoinPredicate order2liorderPred = new JoinPredicateAtom(createPredicate("o.orderkey = l.orderkey"),
				ordersTable, lineitemTable, new Column(ordersTable, DataType.intType(), 0),
				new Column(lineitemTable, DataType.intType(), 0));
		
		// customer join predicates
		JoinPredicate cust2ordercustPred = new JoinPredicateAtom(createPredicate("c.custkey = o.custkey"),
				customerTable, ordersTable, new Column(customerTable, DataType.intType(), 0),
				new Column(ordersTable, DataType.intType(), 1));
		JoinPredicate cust2nationPred = new JoinPredicateAtom(createPredicate("c.nationkey = n.nationkey"),
				customerTable, nationTable, new Column(customerTable, DataType.intType(), 3),
				new Column(nationTable, DataType.intType(), 0));
		
		// region join predicates
		JoinPredicate region2nationPred = new JoinPredicateAtom(createPredicate("r.regionkey = n.regionkey"),
				regionTable, nationTable, new Column(regionTable, DataType.intType(), 0),
				new Column(nationTable, DataType.intType(), 2));
		
		JoinGraphEdge[] edges = new JoinGraphEdge[] {
				new JoinGraphEdge(regionTable, nationTable, region2nationPred),
				new JoinGraphEdge(customerTable, nationTable, cust2nationPred),
				new JoinGraphEdge(customerTable, ordersTable, cust2ordercustPred),
				new JoinGraphEdge(ordersTable, lineitemTable, order2liorderPred),
				new JoinGraphEdge(suppTable, lineitemTable, supp2lisuppPred),
				new JoinGraphEdge(partTable, lineitemTable, part2lipartPred),
				new JoinGraphEdge(partSuppTable, lineitemTable, partsupp2lisuppPred),
				new JoinGraphEdge(partSuppTable, partTable, partsupp2partPred),
				new JoinGraphEdge(partSuppTable, suppTable, partsupp2suppPred) };
	
		AnalyzedSelectQuery query = new AnalyzedSelectQuery(tables, edges);
		
		// --------------------------------------------------------------------
		//                         Prepare the query
		// --------------------------------------------------------------------
		JoinPredicateConjunct joinVerPred = new JoinPredicateConjunct();
		joinVerPred.addJoinPredicate(partsupp2lisuppPred1.createSideSwitchedCopy());
		joinVerPred.addJoinPredicate(partsupp2lisuppPred2.createSideSwitchedCopy());
		joinVerPred.addJoinPredicate(supp2lisuppPred.createSideSwitchedCopy());
		joinVerPred.addJoinPredicate(part2lipartPred.createSideSwitchedCopy());
		
		
		ArrayList<JoinOrderVerifier> verifiers = new ArrayList<JoinOrderVerifier>();
		
		TestingJoinOrderVerifyer solution1 = new TestingJoinOrderVerifyer();
		
		
		solution1.setExpectedJoinOrder(query,	
				new AbstractJoinPlanOperator(
					new AbstractJoinPlanOperator(
						new AbstractJoinPlanOperator(
							new AbstractJoinPlanOperator(
								new AbstractJoinPlanOperator(
									regionTable, 
									nationTable, 
									JoinOrderOptimizerUtils.filterTwinPredicates(region2nationPred)),
								customerTable, 
								JoinOrderOptimizerUtils.filterTwinPredicates(cust2nationPred.createSideSwitchedCopy())), 
							ordersTable, 
							JoinOrderOptimizerUtils.filterTwinPredicates(cust2ordercustPred)),
						lineitemTable, 
						JoinOrderOptimizerUtils.filterTwinPredicates(order2liorderPred)),
					new AbstractJoinPlanOperator(
						new AbstractJoinPlanOperator(
							partSuppTable, 
							partTable, 
							JoinOrderOptimizerUtils.filterTwinPredicates(partsupp2partPred)),
						suppTable, 
						JoinOrderOptimizerUtils.filterTwinPredicates(partsupp2suppPred)), 
					JoinOrderOptimizerUtils.filterTwinPredicates(joinVerPred)));
		
		verifiers.add(solution1);
		
		//second solution
		JoinPredicateConjunct joinVerPred2 = new JoinPredicateConjunct();
		joinVerPred2.addJoinPredicate(part2lipartPred);
		joinVerPred2.addJoinPredicate(partsupp2lisuppPred1);
		
		TestingJoinOrderVerifyer solution2 = new TestingJoinOrderVerifyer();
		solution2.setExpectedJoinOrder(query,
				new AbstractJoinPlanOperator(
						new AbstractJoinPlanOperator(
								partTable,
								new AbstractJoinPlanOperator(
										partSuppTable,
										suppTable,
										JoinOrderOptimizerUtils.filterTwinPredicates(partsupp2suppPred)),
								JoinOrderOptimizerUtils.filterTwinPredicates(partsupp2partPred).createSideSwitchedCopy()),
						new AbstractJoinPlanOperator(
								lineitemTable,
								new AbstractJoinPlanOperator(
										ordersTable,
										new AbstractJoinPlanOperator(
												customerTable,
												new AbstractJoinPlanOperator(regionTable, nationTable, JoinOrderOptimizerUtils.filterTwinPredicates(region2nationPred)),
												JoinOrderOptimizerUtils.filterTwinPredicates(cust2nationPred)),
										JoinOrderOptimizerUtils.filterTwinPredicates(cust2ordercustPred).createSideSwitchedCopy()),
								JoinOrderOptimizerUtils.filterTwinPredicates(order2liorderPred).createSideSwitchedCopy()),
						JoinOrderOptimizerUtils.filterTwinPredicates(joinVerPred2))
				);
		
		verifiers.add(solution2);
		
		// --------------------------------------------------------------------
		//                       Call the optimizer code
		// --------------------------------------------------------------------
		
		ArrayList<Exception> eList = new ArrayList<Exception>();
		
		for(JoinOrderVerifier verifier : verifiers) {
			try {
				this.optimizer.createSelectQueryPlan(query, verifier, true);
			} catch(Exception e) {
				eList.add(e);
			}
		}
		
		if(verifiers.size() == eList.size()) {
			for(Exception e: eList) {
				System.out.println(e.getMessage());
			}
			throw new OptimizerException("Your plan is not within the solutions! (See test output.)");
		}
	}	

	/**
	 * Simple join of ParSupp, Supplier, Lineitem and Orders. No predicates.
	 */
	@Test
	public void testOptimizerJoinWithSubquery() throws Exception
	{
		// --------------------------------------------------------------------
		//                         Prepare the query
		// --------------------------------------------------------------------
		String sql =" SELECT " +
					"	r.r_name AS r_name, p.p_brand AS p_brand, s.s_name AS s_name, p.p_retailprice AS p_retailprice " +
					" FROM " +
					"	supplier s, nation n, region r, partsupplier ps, part p, " +
					"	( " +
					"		SELECT " +
					"			MIN(p.p_retailprice) AS min_retail, r.r_name AS r_name, p.p_brand AS p_brand " +
					"		FROM " +
					"			supplier s, nation n, region r, partsupplier ps, part p " +
					"		WHERE " +
					"			r.r_regionkey = n.n_regionkey AND " +
					"			n.n_nationkey = s.s_nationkey AND " +
					"			s.s_suppkey = ps.ps_suppkey AND " +
					"			ps.ps_partkey = p.p_partkey " +
					"		GROUP BY " +
					"			r.r_name, p.p_brand" +
					"	) pCalc " +
					" WHERE " +
					"	r.r_regionkey = n.n_regionkey AND " +
					"	n.n_nationkey = s.s_nationkey AND " +
					"	s.s_suppkey = ps.ps_suppkey AND " +
					"	ps.ps_partkey = p.p_partkey AND " +
					"	pCalc.min_retail = p.p_retailprice AND " +
					"	pCalc.p_brand = p.p_brand AND " +
					"	pCalc.r_name = r.r_name";
		// parse the query
		SQLParserImpl parser = new SQLParserImpl(sql);
		SelectQuery selectQuery = (SelectQuery) parser.parse();		
		SelectQueryAnalyzerImpl semAna = new SelectQueryAnalyzerImpl();
		AnalyzedSelectQuery query = semAna.analyzeQuery(selectQuery, this.dbInstance.getCatalogue());
		AnalyzedSelectQuery subquery = null;
		
		BaseTableAccess regionTable = null;
		BaseTableAccess nationTable = null;
		BaseTableAccess partSuppTable = null;
		BaseTableAccess suppTable = null;
		BaseTableAccess partTable = null;
		
		for(Relation rel : query.getTableAccesses())
		{
			if(!(rel instanceof BaseTableAccess))
			{
				subquery = (AnalyzedSelectQuery) rel;
			}
			else
			{
				BaseTableAccess tmp = (BaseTableAccess) rel;
				String name = tmp.getTable().getTableName();
				if(name.equals("SUPPLIER"))
				{
					suppTable = tmp;
				}
				else if(name.equals("PARTSUPPLIER"))
				{
					partSuppTable = tmp;					
				}
				else if(name.equals("PART"))
				{
					partTable = tmp;
				}
				else if(name.equals("REGION"))
				{
					regionTable = tmp;
				}
				else if(name.equals("NATION"))
				{
					nationTable = tmp;
				}
			}
		}
		
		
		// create the base relations
		
		BaseTableAccess subRegionTable = null;
		BaseTableAccess subNationTable = null;
		BaseTableAccess subPartSuppTable = null;
		BaseTableAccess subSuppTable = null;
		BaseTableAccess subPartTable = null;
		
		for(Relation rel : subquery.getTableAccesses())
		{
			BaseTableAccess tmp = (BaseTableAccess) rel;
			String name = tmp.getTable().getTableName();
			if(name.equals("SUPPLIER"))
			{
				subSuppTable = tmp;
			}
			else if(name.equals("PARTSUPPLIER"))
			{
				subPartSuppTable = tmp;					
			}
			else if(name.equals("PART"))
			{
				subPartTable = tmp;
			}
			else if(name.equals("REGION"))
			{
				subRegionTable = tmp;
			}
			else if(name.equals("NATION"))
			{
				subNationTable = tmp;
			}
		}
				
		//////////////////////////////////
		// Main query predicates
		//////////////////////////////////
		
		// part supplier join predicates
		JoinPredicate partsupp2suppPred = new JoinPredicateAtom(createPredicate("ps.ps_suppkey = s.s_suppkey"),
				partSuppTable, suppTable, new Column(partSuppTable, DataType.intType(), 1),
				new Column(suppTable, DataType.intType(), 0));
		JoinPredicate partsupp2partPred = new JoinPredicateAtom(createPredicate("ps.ps_partkey = p.p_partkey"),
				partSuppTable, partTable, new Column(partSuppTable, DataType.intType(), 0),
				new Column(partTable, DataType.intType(), 0));
		
		// region join predicates
		JoinPredicate region2nationPred = new JoinPredicateAtom(createPredicate("r.r_regionkey = n.n_regionkey"),
				regionTable, nationTable, new Column(regionTable, DataType.intType(), 0),
				new Column(nationTable, DataType.intType(), 2));
		
		// nation join predicates
		JoinPredicate nation2suppPred = new JoinPredicateAtom(createPredicate("n.n_nationkey = s.s_nationkey"),
				nationTable, suppTable, new Column(nationTable, DataType.intType(), 0),
				new Column(suppTable, DataType.intType(), 3));
		
		// subquery predicates
		JoinPredicate part2retsubqueryPred = new JoinPredicateAtom(createPredicate("p.p_retailprice = pCalc.min_retail"),
				partTable, subquery, new Column(partTable, DataType.floatType(), 7),
				new Column(subquery, DataType.floatType(), 0));
		JoinPredicate part2brandsubqueryPred = new JoinPredicateAtom(createPredicate("p.p_brand = pCalc.p_brand"),
				partTable, subquery, new Column(partTable, partTable.getTable().getSchema().getColumn(3).getDataType(), 3),
				new Column(subquery, DataType.floatType(), 2));
		JoinPredicateConjunct part2subqueryPred = new JoinPredicateConjunct();
		part2subqueryPred.addJoinPredicate(part2retsubqueryPred);
		part2subqueryPred.addJoinPredicate(part2brandsubqueryPred);
		
		JoinPredicate subquery2regionPred = new JoinPredicateAtom(createPredicate("pCalc.r_name = r.r_name"),
				subquery, regionTable, new Column(subquery, regionTable.getTable().getSchema().getColumn(1).getDataType(), 1),
				new Column(regionTable, regionTable.getTable().getSchema().getColumn(1).getDataType(), 1));
		
		JoinPredicateConjunct topJoinPred = new JoinPredicateConjunct();
		topJoinPred.addJoinPredicate(partsupp2suppPred);
		topJoinPred.addJoinPredicate(subquery2regionPred);
		
		//////////////////////////////////
		// Subquery predicates
		//////////////////////////////////
		
		// part supplier join predicates
		JoinPredicate subPartsupp2suppPred = new JoinPredicateAtom(createPredicate("ps.ps_suppkey = s.s_suppkey"),
				subPartSuppTable, subSuppTable, new Column(subPartSuppTable, DataType.intType(), 1),
				new Column(subSuppTable, DataType.intType(), 0));
		JoinPredicate subPartsupp2partPred = new JoinPredicateAtom(createPredicate("ps.ps_partkey = p.p_partkey"),
				subPartSuppTable, subPartTable, new Column(subPartSuppTable, DataType.intType(), 0),
				new Column(subPartTable, DataType.intType(), 0));
			
		// region join predicates
		JoinPredicate subRegion2nationPred = new JoinPredicateAtom(createPredicate("r.r_regionkey = n.n_regionkey"),
				subRegionTable, subNationTable, new Column(subRegionTable, DataType.intType(), 0),
				new Column(subNationTable, DataType.intType(), 2));

		// nation join predicates
		JoinPredicate subNation2supplierPred = new JoinPredicateAtom(createPredicate("n.n_nationkey = s.s_nationkey"), 
				subNationTable, subSuppTable, new Column(subNationTable, DataType.intType(), 0), 
				new Column(subSuppTable, DataType.intType(), 3));
				
		// --------------------------------------------------------------------
		//                         Prepare the query
		// --------------------------------------------------------------------
		ArrayList<JoinOrderVerifier> verifiers = new ArrayList<JoinOrderVerifier>();
		
		TestingJoinOrderVerifyer solution1 = new TestingJoinOrderVerifyer();
		
		solution1.setExpectedJoinOrder(query,	
				new AbstractJoinPlanOperator(
					new AbstractJoinPlanOperator(
						new AbstractJoinPlanOperator(
							partTable, 
							subquery, 
							JoinOrderOptimizerUtils.filterTwinPredicates(part2subqueryPred)),
						partSuppTable, 
						JoinOrderOptimizerUtils.filterTwinPredicates(partsupp2partPred.createSideSwitchedCopy())),
					new AbstractJoinPlanOperator(
						new AbstractJoinPlanOperator(
							nationTable, 
							regionTable, 
							JoinOrderOptimizerUtils.filterTwinPredicates(region2nationPred.createSideSwitchedCopy())), 
						suppTable, 
						JoinOrderOptimizerUtils.filterTwinPredicates(nation2suppPred)), 
					JoinOrderOptimizerUtils.filterTwinPredicates(topJoinPred)));
		
		solution1.setExpectedJoinOrder(subquery, 
				new AbstractJoinPlanOperator(
					new AbstractJoinPlanOperator(
						new AbstractJoinPlanOperator(
							new AbstractJoinPlanOperator(
								subNationTable,
								subRegionTable, 
								JoinOrderOptimizerUtils.filterTwinPredicates(subRegion2nationPred.createSideSwitchedCopy())), 
							subSuppTable, 
							JoinOrderOptimizerUtils.filterTwinPredicates(subNation2supplierPred)), 
						subPartSuppTable, 
						JoinOrderOptimizerUtils.filterTwinPredicates(subPartsupp2suppPred.createSideSwitchedCopy())), 
					subPartTable, 
					JoinOrderOptimizerUtils.filterTwinPredicates(subPartsupp2partPred))
				);
		
		verifiers.add(solution1);
		
		
		TestingJoinOrderVerifyer solution2 = new TestingJoinOrderVerifyer();
		
		solution2.setExpectedJoinOrder(query,	
				new AbstractJoinPlanOperator(
					new AbstractJoinPlanOperator(
						new AbstractJoinPlanOperator(
							partTable, 
							subquery, 
							JoinOrderOptimizerUtils.filterTwinPredicates(part2subqueryPred)),
						partSuppTable, 
						JoinOrderOptimizerUtils.filterTwinPredicates(partsupp2partPred.createSideSwitchedCopy())),
					new AbstractJoinPlanOperator(
						new AbstractJoinPlanOperator(
							nationTable, 
							regionTable, 
							JoinOrderOptimizerUtils.filterTwinPredicates(region2nationPred.createSideSwitchedCopy())), 
						suppTable, 
						JoinOrderOptimizerUtils.filterTwinPredicates(nation2suppPred)), 
					JoinOrderOptimizerUtils.filterTwinPredicates(topJoinPred)));
		
		
		AbstractJoinPlanOperator regionJnation_1 = new AbstractJoinPlanOperator(subRegionTable, subNationTable, JoinOrderOptimizerUtils.filterTwinPredicates(subRegion2nationPred));
		AbstractJoinPlanOperator supplierJ1_2 = new AbstractJoinPlanOperator(subSuppTable, regionJnation_1, JoinOrderOptimizerUtils.filterTwinPredicates(subNation2supplierPred.createSideSwitchedCopy()));
		
		AbstractJoinPlanOperator partJpartsupplier_3 = new AbstractJoinPlanOperator(subPartTable, subPartSuppTable, JoinOrderOptimizerUtils.filterTwinPredicates(subPartsupp2partPred.createSideSwitchedCopy()));
		AbstractJoinPlanOperator final3J2 = new AbstractJoinPlanOperator(partJpartsupplier_3, supplierJ1_2, JoinOrderOptimizerUtils.filterTwinPredicates(subPartsupp2suppPred));
		
		solution2.setExpectedJoinOrder(subquery, final3J2);
		
		verifiers.add(solution2);
		
		
		// --------------------------------------------------------------------
		//                       Call the optimizer code
		// --------------------------------------------------------------------
		
		ArrayList<Exception> eList = new ArrayList<Exception>();
		
		for(JoinOrderVerifier verifier : verifiers) {
			try {
				this.optimizer.createSelectQueryPlan(query, verifier, true);
			} catch(Exception e) {
				eList.add(e);
			}
		}
		
		if(verifiers.size() == eList.size()) {
			for(Exception e: eList) {
				System.out.println(e.getMessage());
			}
			throw new OptimizerException("Your plan is not within the solutions! (See test output.)");
		}
	}		

	private static Predicate createPredicate(String text)
	{
		try {
			SQLTokenizer tokenizer = new SQLTokenizer(text);
			tokenizer.nextToken();
			
			SQLParserImpl parser = new SQLParserImpl(null);
			return parser.parsePredicate(tokenizer, false);
		}
		catch (ParseException e) {
			return null;
		}
	}
	
	private static class TestingJoinOrderVerifyer implements JoinOrderVerifier
	{
		private final Map<AnalyzedSelectQuery, OptimizerPlanOperator> queryMap;
		
		
		private TestingJoinOrderVerifyer() {
			this.queryMap = new HashMap<AnalyzedSelectQuery, OptimizerPlanOperator>();
		}
		
		public void setExpectedJoinOrder(AnalyzedSelectQuery query, OptimizerPlanOperator order) {
			this.queryMap.put(query, order);
		}
		
		@Override
		public void verifyJoinOrder(AnalyzedSelectQuery query, OptimizerPlanOperator pop) throws OptimizerException
		{
			OptimizerPlanOperator should = this.queryMap.get(query);
			if (should == null) {
				throw new OptimizerException("Verifyer cannot verify this query plan, because no expected version exists for this query.");
			}
			
			if (!recursiveCompare(pop, should)) {
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				PrintStream ps = new PrintStream(baos);
				LogicalPlanPrinter planPrinter = new LogicalPlanPrinter(ps, true);
				
				ps.println("~~~~~~~~~~~~~~~~~~~~~~~ Actual Plan ~~~~~~~~~~~~~~~~~~~~~~~");
				planPrinter.print(pop);
				System.out.println("actual: " + ((AbstractJoinPlanOperator)pop).toString());
				ps.println("~~~~~~~~~~~~~~~~~~~~~~ Expected Plan ~~~~~~~~~~~~~~~~~~~~~~");
				planPrinter.print(should);
				System.out.println("should: " + ((AbstractJoinPlanOperator)should).toString());
				ps.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
				ps.flush();
				
				throw new OptimizerException("The given plan does not match the expected plan.\n" + baos.toString());
			}
		}
		
		private boolean recursiveCompare(OptimizerPlanOperator plan, OptimizerPlanOperator should)
		{
			if (plan == null || should == null) {
				return false;
			}
			
			// final case
			if (plan instanceof Relation && should instanceof Relation) {
				return compareRelations((Relation) plan, (Relation) should);
			}
			
			// intermediate case
			if (plan instanceof AbstractJoinPlanOperator && should instanceof AbstractJoinPlanOperator) {
				// both are join. make sure that children match, no matter in which order
				
				AbstractJoinPlanOperator planJoin = (AbstractJoinPlanOperator) plan;
				AbstractJoinPlanOperator shouldJoin = (AbstractJoinPlanOperator) should;
				
				if (recursiveCompare(planJoin.getLeftChild(), shouldJoin.getLeftChild()) &&
					recursiveCompare(planJoin.getRightChild(), shouldJoin.getRightChild()))
				{
					// verify that the join predicate matches
					JoinPredicate shouldJoinPredFiltered = JoinOrderOptimizerUtils.filterTwinPredicates(shouldJoin.getJoinPredicate());
					JoinPredicate planJoinPredFiltered = JoinOrderOptimizerUtils.filterTwinPredicates(planJoin.getJoinPredicate());
					
					// get predicate in the right oder
					planJoinPredFiltered = getRightSwitchedPredicate(planJoin.getLeftChild(),planJoin.getRightChild(),planJoinPredFiltered);
					
					if (!shouldJoin.getJoinPredicate().equals(planJoinPredFiltered)) {
						System.out.println("WARNING: original predicate does not match expected predicate, maybe missing JoinOrderOptimizerUtils#filterTwinPredicates() call?");
						System.out.println("         original predicate: " + planJoinPredFiltered);
						System.out.println("         expected predicate: " + shouldJoin.getJoinPredicate());
					}
					
					boolean predicatesAreEqual = shouldJoinPredFiltered.equals(planJoinPredFiltered);
					if (!predicatesAreEqual) {
						System.out.println("Non-equal predicates:");
						System.out.println("  exp: " + shouldJoinPredFiltered);
						System.out.println("  act: " + planJoinPredFiltered);
					}
					return predicatesAreEqual;
				}
				
				if (recursiveCompare(planJoin.getLeftChild(), shouldJoin.getRightChild()) &&
					recursiveCompare(planJoin.getRightChild(), shouldJoin.getLeftChild()))
				{
					// verify that the join predicate matches
					JoinPredicate shouldJoinPredFiltered = JoinOrderOptimizerUtils.filterTwinPredicates(shouldJoin.getJoinPredicate().createSideSwitchedCopy());
					JoinPredicate planJoinPredFiltered = JoinOrderOptimizerUtils.filterTwinPredicates(planJoin.getJoinPredicate());
					
					planJoinPredFiltered = getRightSwitchedPredicate(planJoin.getLeftChild(),planJoin.getRightChild(),planJoinPredFiltered);
					
					if (!shouldJoin.getJoinPredicate().createSideSwitchedCopy().equals(planJoinPredFiltered)) {
						System.out.println("WARNING: original (switched) predicate does not match expected predicate, maybe missing JoinOrderOptimizerUtils#filterTwinPredicates() call?");
					}
					
					boolean predicatesAreEqual = shouldJoinPredFiltered.equals(planJoinPredFiltered);
					if (!predicatesAreEqual) {
						System.out.println("Non-equal predicates: exp: " + shouldJoinPredFiltered + ", act: " + planJoinPredFiltered);
					}
					return predicatesAreEqual;
				}
				
				return false;
			}
			
			return false;
		}
		
		private boolean compareRelations(Relation plan, Relation should)
		{
			if (plan == null || should == null) {
				return false;
			}
			
			if (plan instanceof BaseTableAccess && should instanceof BaseTableAccess) {
				BaseTableAccess planAccess = (BaseTableAccess) plan;
				BaseTableAccess shouldAccess = (BaseTableAccess) should;
				
				return shouldAccess.getTable().equals(planAccess.getTable());
			}
			else if (plan instanceof AnalyzedSelectQuery && should instanceof AnalyzedSelectQuery) {
				return plan == should;
			}
			else {
				return false;
			}
		}
	}
}
