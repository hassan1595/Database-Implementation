package de.tuberlin.dima.minidb;


import java.util.concurrent.atomic.AtomicInteger;

import de.tuberlin.dima.minidb.api.AbstractExtensionFactory;
import de.tuberlin.dima.minidb.catalogue.Catalogue;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.io.manager.BufferPoolManager;
import de.tuberlin.dima.minidb.optimizer.Optimizer;
import de.tuberlin.dima.minidb.optimizer.OptimizerException;
import de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator;
import de.tuberlin.dima.minidb.parser.DeleteQuery;
import de.tuberlin.dima.minidb.parser.InsertQuery;
import de.tuberlin.dima.minidb.parser.ParseException;
import de.tuberlin.dima.minidb.parser.ParsedQuery;
import de.tuberlin.dima.minidb.parser.SQLParser;
import de.tuberlin.dima.minidb.parser.SelectQuery;
import de.tuberlin.dima.minidb.qexec.PhysicalPlanOperator;
import de.tuberlin.dima.minidb.qexec.QueryExecutionException;
import de.tuberlin.dima.minidb.qexec.heap.QueryHeap;
import de.tuberlin.dima.minidb.semantics.AnalyzedSelectQuery;
import de.tuberlin.dima.minidb.semantics.QuerySemanticsInvalidException;
import de.tuberlin.dima.minidb.semantics.SelectQueryAnalyzer;


/**
 * The query processor that drives the evaluation of a SQL query through all phases.
 * Since the system is currently single-threaded, it uses only one thread.
 * 
 * STUB ONLY. 
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class QueryProcessor
{
	/**
	 * The catalogue to be used by the query processor.
	 */
	private final Catalogue catalogue;
	
	/**
	 * The configuration object for this instance.
	 */
	private final Config config;
	
	/**
	 * The buffer pool manager used for queries executed by this query executor.
	 */
	private final BufferPoolManager bufferPool;
	
	/**
	 * The heap used by queries executed by this query executor.
	 */
	private final QueryHeap heap;
	
	/**
	 * The current data epoch for reading. This property and the associated 
	 * logic are maintained for historical reasons. The current physical  
	 * operator implementation does not use epochs.  
	 */
	@SuppressWarnings("unused")
	private final AtomicInteger currentReadEpoch;
		
	/**
	 * Creates a new query processor that will use the given catalogue and configuration.
	 * 
	 * @param cat The catalogue to be used by the query processor.
	 * @param config The configuration object for this instance.
	 * @param buffer The buffer pool manager used for queries executed by this query executor.
	 * @param heap The heap used by queries executed by this query executor.
	 */
	public QueryProcessor(Catalogue cat, Config config, BufferPoolManager buffer, QueryHeap heap, int startingEpoch)
	{
		// fail fast
		if (cat == null || config == null || buffer == null || heap == null) {
			throw new NullPointerException();
		}
		
		this.catalogue = cat;
		this.config = config;
		this.bufferPool = buffer;
		this.heap = heap;
		this.currentReadEpoch = new AtomicInteger(startingEpoch);
	}

	
	
	/**
	 * Main function to execute a query as given by the query string. The results of the query,
	 * are written to the given result set.
	 * 
	 * @param queryString The query to process.
	 * @param resultHandler The result hander to accept the result tuples.
	 */
	public void processQuery(String queryString, ResultHandler resultHandler)
	{
		/* *******************************************************
		 *                  1) Query Parsing
		 *                  
		 * This step involves purely syntactical checking. It is
		 * not checked whether tables or columns exist or the
		 * combination of clauses is valid, beyond pure syntax.
		 * ******************************************************/
		
		ParsedQuery parsedQuery = null;
		try {
			SQLParser parser = AbstractExtensionFactory.getExtensionFactory().getParser(queryString);
			parsedQuery = parser.parse();
		}
		catch (ParseException pex) {
			resultHandler.handleException(pex);
			return;
		}
	    
	    /* *******************************************************
		 *                2) Semantic Checking
		 *                
		 * This step checks the query semantically and performs
		 * some other semantic steps, like normalizing predicates,
		 * reasoning about predicates, addition of transitive
		 * predicates.
		 * 
		 *                    3) Optimization
		 * 
		 * This step finds the best plan to optimize the query,
		 * based on a representation as an optimizer plan.
		 * ******************************************************/
		
		long rc = this.config.getBlockReadCost();
    	long wc = this.config.getBlockWriteCost();
    	long rro = this.config.getBlockRandomReadOverhead();
    	long rwo = this.config.getBlockRandomWriteOverhead();
    	final Optimizer opt = new Optimizer(this.catalogue, rc, wc, rro, rwo);
    	
    	OptimizerPlanOperator bestPlan = null;
    	
	    // we need to distinguish the between the types of query we have
		if (parsedQuery instanceof SelectQuery)
		{
			SelectQueryAnalyzer analyzer = AbstractExtensionFactory.getExtensionFactory().createSelectQueryAnalyzer();
	    	try {
	    		// analyze the query and rewrite it logically
	    		AnalyzedSelectQuery analyzedQuery = analyzer.analyzeQuery((SelectQuery) parsedQuery, this.catalogue);
	    		
	    		 // open the result set with the schema information
	        	resultHandler.openResultSet(analyzedQuery.getOutputColumns());
	        				    
	    		// call the optimizer to determine the query execution plan
	    	    bestPlan = opt.createSelectQueryPlan(analyzedQuery);
	       	}
	    	catch (QuerySemanticsInvalidException qsiex) {
	    		resultHandler.handleException(qsiex);
	    		return;
	    	}
    	    catch (OptimizerException oex) {
    	    	resultHandler.handleException(oex);
    	    	return;
    	    }
	    	parsedQuery = null;
		}
		else if (parsedQuery instanceof InsertQuery)
		{
			throw new UnsupportedOperationException("Query Processor can currently only handle SELECT queries.");
		}
		else if (parsedQuery instanceof DeleteQuery)
		{
			throw new UnsupportedOperationException("Query Processor can currently only handle SELECT queries.");
		}
		else {
			throw new UnsupportedOperationException("Query Processor can currently only handle SELECT queries.");
		}	    
	    
	    
	    /* *******************************************************
		 *                  4) Physical Plan Translation
		 *
		 * Here, the optimizer plan is translated into an
		 * executable plan.
		 * ******************************************************/
	    
	    PhysicalPlanOperator executablePlan = bestPlan.createPhysicalPlan(this.bufferPool, this.heap);
	    bestPlan = null;
	    
	    /* *******************************************************
		 *                  5) Plan Execution
		 *
		 * Here, the best plan is executed.
		 * ******************************************************/
	    
	    try {
	    	executablePlan.open(null);
	    	DataTuple tuple = null;
	    	while ((tuple = executablePlan.next()) != null) {
	    		resultHandler.addResultTuple(tuple);
	    	}
	    	resultHandler.closeResultSet();
	    }
	    catch (QueryExecutionException qeex) {
	    	resultHandler.handleException(qeex);
	    	return;
	    }
	    finally {
	    	try {
				executablePlan.close();
			}
			catch (QueryExecutionException qex) {
				resultHandler.handleException(qex);
			}
	    }
	}
}
