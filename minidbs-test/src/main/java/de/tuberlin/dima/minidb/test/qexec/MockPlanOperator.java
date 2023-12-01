package de.tuberlin.dima.minidb.test.qexec;

import java.util.Iterator;
import java.util.List;

import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.qexec.PhysicalPlanOperator;
import de.tuberlin.dima.minidb.qexec.QueryExecutionException;

/**
 * Mock class generating the input to test the insert & delete operator.
 * Returns the given data tuples using the physical plan operator interface methods.
 * 
 * @author Michael Saecker
 */
public class MockPlanOperator implements PhysicalPlanOperator {

	/**
	 * The set of tuples this operator returns.
	 */
	private List<DataTuple> tuples;
	
	/**
	 * The iterator holding the information which tuple to return next. 
	 */
	private Iterator<DataTuple> iter;
	
	/**
	 * Creates an operator that will return the given tuples using the physical plan operator interface.
	 * 
	 * @param tuples The tuples this operator shall return.
	 */
	public MockPlanOperator(List<DataTuple> tuples)
	{
		this.tuples = tuples;
	}
	
	/*
	 * (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.qexec.PhysicalPlanOperator#open(int, de.tuberlin.dima.minidb.core.DataTuple)
	 */
	@Override
	public void open(DataTuple correlatedTuple)
			throws QueryExecutionException 
	{
		this.iter = this.tuples.iterator();
	}

	/*
	 * (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.qexec.PhysicalPlanOperator#next()
	 */
	@Override
	public DataTuple next() throws QueryExecutionException 
	{
		if(this.iter.hasNext())
		{
			return this.iter.next();
		}
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.qexec.PhysicalPlanOperator#close()
	 */
	@Override
	public void close() throws QueryExecutionException 
	{
		// clear resources
		this.iter = null;
	}

}
