package de.tuberlin.dima.minidb.test.optimizer.generator.util;

import de.tuberlin.dima.minidb.optimizer.FetchPlanOperator;
import de.tuberlin.dima.minidb.optimizer.FilterPlanOperator;
import de.tuberlin.dima.minidb.optimizer.GroupByPlanOperator;
import de.tuberlin.dima.minidb.optimizer.IndexLookupPlanOperator;
import de.tuberlin.dima.minidb.optimizer.MergeJoinPlanOperator;
import de.tuberlin.dima.minidb.optimizer.NestedLoopJoinPlanOperator;
import de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator;
import de.tuberlin.dima.minidb.optimizer.SortPlanOperator;
import de.tuberlin.dima.minidb.optimizer.TableScanPlanOperator;
import de.tuberlin.dima.minidb.semantics.Column;
import de.tuberlin.dima.minidb.semantics.predicate.LocalPredicate;

public class PhysicalPlanVerifier
{
	public void verify(OptimizerPlanOperator expSubplan, OptimizerPlanOperator actSubplan)
	{
		if (expSubplan instanceof FetchPlanOperator)
		{
			verifyConcrete((FetchPlanOperator) expSubplan, actSubplan);
		}
		else if (expSubplan instanceof FilterPlanOperator)
		{
			verifyConcrete((FilterPlanOperator) expSubplan, actSubplan);
		}
		else if (expSubplan instanceof GroupByPlanOperator)
		{
			verifyConcrete((GroupByPlanOperator) expSubplan, actSubplan);
		}
		else if (expSubplan instanceof IndexLookupPlanOperator)
		{
			verifyConcrete((IndexLookupPlanOperator) expSubplan, actSubplan);
		}
		else if (expSubplan instanceof MergeJoinPlanOperator)
		{
			verifyConcrete((MergeJoinPlanOperator) expSubplan, actSubplan);
		}
		else if (expSubplan instanceof NestedLoopJoinPlanOperator)
		{
			verifyConcrete((NestedLoopJoinPlanOperator) expSubplan, actSubplan);
		}
		else if (expSubplan instanceof SortPlanOperator)
		{
			verifyConcrete((SortPlanOperator) expSubplan, actSubplan);
		}
		else if (expSubplan instanceof TableScanPlanOperator)
		{
			verifyConcrete((TableScanPlanOperator) expSubplan, actSubplan);
		}
		else
		{
			System.out.println("unsupported verification of subplan with root " + expSubplan.getName());
		}
	}
	
	private void verifyConcrete(FetchPlanOperator expSubplan, OptimizerPlanOperator actSubplan)
	{
		if (!(actSubplan instanceof FetchPlanOperator))
		{
			throw new PhysicalPlanVerifierException(expSubplan, actSubplan);
		}
		
		FetchPlanOperator actSubplanCast = (FetchPlanOperator) actSubplan;
		
		verify(expSubplan.getChild(), actSubplanCast.getChild());
	}
	
	private void verifyConcrete(FilterPlanOperator expSubplan, OptimizerPlanOperator actSubplan)
	{
		if (!(actSubplan instanceof FilterPlanOperator))
		{
			throw new PhysicalPlanVerifierException(expSubplan, actSubplan);
		}
		
		FilterPlanOperator actSubplanCast = (FilterPlanOperator) actSubplan;
		
		verifyLocalPredicate(expSubplan, actSubplanCast, expSubplan.getSimplePredicate(), actSubplanCast.getSimplePredicate());
		
		verify(expSubplan.getChild(), actSubplanCast.getChild());
	}

	private void verifyConcrete(GroupByPlanOperator expSubplan, OptimizerPlanOperator actSubplan)
	{
		if (!(actSubplan instanceof GroupByPlanOperator))
		{
			throw new PhysicalPlanVerifierException(expSubplan, actSubplan);
		}

		GroupByPlanOperator actSubplanCast = (GroupByPlanOperator) actSubplan;
		
		verifyOutputColumns(expSubplan, actSubplanCast);
		
		verify(expSubplan.getChild(), actSubplanCast.getChild());
	}

	private void verifyConcrete(IndexLookupPlanOperator expSubplan, OptimizerPlanOperator actSubplan)
	{
		if (!(actSubplan instanceof IndexLookupPlanOperator))
		{
			throw new PhysicalPlanVerifierException(expSubplan, actSubplan);
		}

		IndexLookupPlanOperator actSubplanCast = (IndexLookupPlanOperator) actSubplan;
		
		verifyOutputColumns(expSubplan, actSubplanCast);
		
		if (!actSubplanCast.isCorrelated())
		{
    		if (!actSubplanCast.getLocalPredicate().equals(actSubplanCast.getLocalPredicate()))
    		{
    			throw new PhysicalPlanVerifierException("IndexLookupPlanOperator local predicates differ", expSubplan, actSubplan);
    		}
		}
	}
	
	private void verifyConcrete(MergeJoinPlanOperator expSubplan, OptimizerPlanOperator actSubplan)
	{
		if (!(actSubplan instanceof MergeJoinPlanOperator))
		{
			throw new PhysicalPlanVerifierException(expSubplan, actSubplan);
		}

		MergeJoinPlanOperator actSubplanCast = (MergeJoinPlanOperator) actSubplan;
		
		verifyOutputColumns(expSubplan, actSubplanCast);
		
		verify(expSubplan.getLeftChild(), actSubplanCast.getLeftChild());
		verify(expSubplan.getRightChild(), actSubplanCast.getRightChild());
	}
	
	private void verifyConcrete(NestedLoopJoinPlanOperator expSubplan, OptimizerPlanOperator actSubplan)
	{
		if (!(actSubplan instanceof NestedLoopJoinPlanOperator))
		{
			throw new PhysicalPlanVerifierException(expSubplan, actSubplan);
		}

		NestedLoopJoinPlanOperator actSubplanCast = (NestedLoopJoinPlanOperator) actSubplan;
		
		verifyOutputColumns(expSubplan, actSubplanCast);
		
		verify(expSubplan.getInnerChild(), actSubplanCast.getInnerChild());
		verify(expSubplan.getOuterChild(), actSubplanCast.getOuterChild());
	}
	
	private void verifyConcrete(SortPlanOperator expSubplan, OptimizerPlanOperator actSubplan)
	{
		if (!(actSubplan instanceof SortPlanOperator))
		{
			throw new PhysicalPlanVerifierException(expSubplan, actSubplan);
		}

		SortPlanOperator actSubplanCast = (SortPlanOperator) actSubplan;
		
		verify(expSubplan.getChild(), actSubplanCast.getChild());
	}
	
	private void verifyConcrete(TableScanPlanOperator expSubplan, OptimizerPlanOperator actSubplan)
	{
		if (!(actSubplan instanceof TableScanPlanOperator))
		{
			throw new PhysicalPlanVerifierException(expSubplan, actSubplan);
		}

		TableScanPlanOperator actSubplanCast = (TableScanPlanOperator) actSubplan;
		
		verifyOutputColumns(expSubplan, actSubplanCast);
		
		if (expSubplan.getID() != actSubplanCast.getID())
		{
			throw new PhysicalPlanVerifierException("TableScanPlanOperator is defined on different relations", expSubplan, actSubplan);
		}
		
		if (expSubplan.getPredicate() != null)
		{
			if (actSubplanCast.getPredicate() == null)
			{
				throw new PhysicalPlanVerifierException("TableScanPlanOperator must define a local predicate", expSubplan, actSubplan);
			}
			
			if (!expSubplan.getPredicate().toString().equals(actSubplanCast.getPredicate().toString()))
			{
    			throw new PhysicalPlanVerifierException("TableScanPlanOperator local predicates differ", expSubplan, actSubplan);
			}
		}
	}
	
	private void verifyOutputColumns(OptimizerPlanOperator expSubplan, OptimizerPlanOperator actSubplan)
    {
		Column[] expColumns = expSubplan.getReturnedColumns();
		Column[] actColumns = actSubplan.getReturnedColumns();
		
	    if (expColumns.length != actColumns.length)
	    {
	    	throw new PhysicalPlanVerifierException("Different output columns count at root operator (expected: " + columnsToString(expColumns) + ", actual: " + columnsToString(actColumns) + ")", expSubplan, actSubplan);
	    }
	    
	    for (int i = 0; i < expColumns.length; i++)
	    {
	    	Column expColumn = expColumns[i];
	    	Column actColumn = actColumns[i];
	    	
	    	if (expColumn.getColumnIndex() != actColumn.getColumnIndex())
	    	{
		    	throw new PhysicalPlanVerifierException("Output column #" + i + " have different column indices (expected = " + expColumn.toString() + "[" + expColumn.getRelation().getID() + "], actual = " + actColumn.toString() + ") [" + actColumn.getRelation().getID() + "]", expSubplan, actSubplan);
	    	}
	    	
	    	if (expColumn.getRelation().getID() != actColumn.getRelation().getID())
	    	{
		    	throw new PhysicalPlanVerifierException("Output column #" + i + " references different relations (expected = " + expColumn.toString() + "[" + expColumn.getRelation().getID() + "], actual = " + actColumn.toString() + ") [" + actColumn.getRelation().getID() + "]", expSubplan, actSubplan);
	    	}
	    	
	    	if (!expColumn.getDataType().toString().equals(actColumn.getDataType().toString()))
	    	{
	    		String msg = String.format("Output column #" + i + " have different data types (expected %s, actual %s)", expColumn.getDataType().toString(), actColumn.getDataType().toString());
		    	throw new PhysicalPlanVerifierException(msg, expSubplan, actSubplan);
	    	}
	    }
    }
	
	private void verifyLocalPredicate(OptimizerPlanOperator expSubplan, OptimizerPlanOperator actSubplan, LocalPredicate expPred, LocalPredicate actPred)
	{
		if (!expPred.toString().equals(actPred.toString()))
		{
			throw new PhysicalPlanVerifierException("Root operators have different local predicates", expSubplan, actSubplan);
		}
	}
	
	@SuppressWarnings("unused")
	private void verifyJoinPredicate(OptimizerPlanOperator expSubplan, OptimizerPlanOperator actSubplan, LocalPredicate expPred, LocalPredicate actPred)
	{
		if (!expPred.toString().equals(actPred.toString()))
		{
			throw new PhysicalPlanVerifierException("Root operators have different local predicates", expSubplan, actSubplan);
		}
	}
	
	private String columnsToString(Column[] columns)
	{
		StringBuilder sb = new StringBuilder();
		
		if (columns.length <= 0) {
			return "[]";
		}
		
		sb.append("[");
	    for (int i = 0; i < columns.length; i++)
	    {
	    	sb.append(columns[i].getRelation());
	    	sb.append(":");
	    	sb.append(columns[i].getColumnIndex());
	    	sb.append(", ");
	    }
	    sb.replace(sb.length()-2, sb.length(), "");
	    sb.append("]");
		
		return sb.toString();
	}
}
