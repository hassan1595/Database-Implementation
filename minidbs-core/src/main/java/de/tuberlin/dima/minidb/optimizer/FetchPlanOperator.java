package de.tuberlin.dima.minidb.optimizer;


import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import de.tuberlin.dima.minidb.catalogue.TableDescriptor;
import de.tuberlin.dima.minidb.io.manager.BufferPoolManager;
import de.tuberlin.dima.minidb.qexec.OperatorFactory;
import de.tuberlin.dima.minidb.qexec.PhysicalPlanOperator;
import de.tuberlin.dima.minidb.qexec.heap.QueryHeap;
import de.tuberlin.dima.minidb.semantics.BaseTableAccess;
import de.tuberlin.dima.minidb.semantics.Column;
import de.tuberlin.dima.minidb.semantics.Relation;


/**
 * The optimizer representation of a FETCH operator that accesses a table based on a RID.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class FetchPlanOperator extends OptimizerPlanOperator
{
	/**
	 * The child of this FETCH operator.
	 */
	private OptimizerPlanOperator childOperator;
	
	/**
	 * The base table access node for the fetched table.
	 */
	private BaseTableAccess tableAccess;
	
	/**
	 * The table from which we fetch the tuples.
	 */
	private TableDescriptor accessedTable; 
	
	/**
	 * The columns that we should produce.
	 */
	private Column[] outputCols;
	
	/**
	 * The set of involved relations.
	 */
	private Set<Relation> involvedRelations;
	
	
	/**
	 * Creates a new Fetch operator that retrieves the given columns from a table, 
	 * based on the RIDs from the child.
	 * 
	 * @param child The child of this FETCH operator.
	 * @param colsToFetch The columns that should be put out.
	 */
	public FetchPlanOperator(OptimizerPlanOperator child, BaseTableAccess accessedTable, Column[] colsToFetch)
	{
		this.childOperator = child;
		this.tableAccess = accessedTable;
		this.accessedTable = accessedTable.getTable();
		this.outputCols = colsToFetch;
		
		// do some sanity checks
		Column[] childCols = child.getReturnedColumns();
		if (childCols == null || childCols.length != 1 || !childCols[0].isRID()) {
			throw new IllegalArgumentException("Child does not produce tuples with RID column.");
		}
			
		for (Column col : colsToFetch) {
			Relation rel = col.getRelation();
			if (!(rel instanceof BaseTableAccess && rel == accessedTable)) {
				throw new IllegalArgumentException("Produced columns involve columns from another table.");
			}
		}
		
		this.involvedRelations = new HashSet<Relation>(2);
		this.involvedRelations.add(accessedTable);
	}
	
	/**
	 * Gets the child of this fetch operator.
	 * 
	 * @return The child of this FETCH.
	 */
	public OptimizerPlanOperator getChild()
	{
		return this.childOperator;
	}
	
	/**
	 * Gets the table from which this operator fetches the tuples.
	 * 
	 * @return The table from which this operator fetches the tuples.
	 */
	public BaseTableAccess getTableAccess()
	{
		return this.tableAccess;
	}
	
	/**
	 * Gets the table from which this operator fetches the tuples.
	 * 
	 * @return The table from which this operator fetches the tuples.
	 */
	public TableDescriptor getAccessedTable()
	{
		return this.accessedTable;
	}
	
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator#getName()
	 */
	@Override
	public String getName()
	{
		return "Fetch";
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator#getChildren()
	 */
	@Override
	public Iterator<OptimizerPlanOperator> getChildren()
	{
		return Collections.singleton(this.childOperator).iterator();
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator#getOutputCardinality()
	 */
	@Override
	public long getOutputCardinality()
	{
		// fetch neither adds nor drops rows
		return this.childOperator.getOutputCardinality();
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator#getInvolvedTables()
	 */
	@Override
	public Set<Relation> getInvolvedRelations()
	{
		return this.involvedRelations;
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator#getOutputColumns()
	 */
	@Override
	public Column[] getReturnedColumns()
	{
		return this.outputCols;
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator#createPhysicalPlan(de.tuberlin.dima.minidb.io.BufferPoolManager, de.tuberlin.dima.minidb.qexec.heap.QueryHeap)
	 */
	@Override
	public PhysicalPlanOperator createPhysicalPlan(BufferPoolManager buffer, QueryHeap heap)
	{
		PhysicalPlanOperator childPlan = this.childOperator.createPhysicalPlan(buffer, heap);
		
		int[] colIndices = new int[this.outputCols.length];
		for (int i = 0; i < this.outputCols.length; i++) {
			colIndices[i] = this.outputCols[i].getColumnIndex();
		}
		return OperatorFactory.createFetchOperator(childPlan, buffer, 
				this.accessedTable.getResourceId(), colIndices);
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator#getColumnOrder()
	 */
	@Override
	public OrderedColumn[] getColumnOrder()
	{
		OrderedColumn[] oc = this.childOperator.getColumnOrder();
		
		// this operator preserves the order from the child
		// however drop it, if it is the RID order, which corresponds
		// to none after the fetch
		if (oc != null && oc.length == 1 && oc[0].isSingleColumn() && oc[0].getColumns()[0].isRID()) {
			return null;
		}
		else {
			return oc;
		}
	}
	
	/**
	 * Checks if this FETCH operator is performing a sequential access on the table
	 * because it receives the RIDs in a sorted way.
	 * 
	 * @return True, if the FETCH accesses the table in a sequential way.
	 */
	public boolean isSequentialFetch()
	{
		OrderedColumn[] oc = this.childOperator.getColumnOrder();
		return oc != null && oc.length == 1 && oc[0].isSingleColumn() && oc[0].getColumns()[0].isRID();
	}

}
