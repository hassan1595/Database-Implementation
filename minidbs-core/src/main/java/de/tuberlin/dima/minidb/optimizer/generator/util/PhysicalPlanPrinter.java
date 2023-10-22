package de.tuberlin.dima.minidb.optimizer.generator.util;

import de.tuberlin.dima.minidb.optimizer.AbstractJoinPlanOperator;
import de.tuberlin.dima.minidb.optimizer.FetchPlanOperator;
import de.tuberlin.dima.minidb.optimizer.FilterPlanOperator;
import de.tuberlin.dima.minidb.optimizer.IndexLookupPlanOperator;
import de.tuberlin.dima.minidb.optimizer.MergeJoinPlanOperator;
import de.tuberlin.dima.minidb.optimizer.NestedLoopJoinPlanOperator;
import de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator;
import de.tuberlin.dima.minidb.optimizer.OptimizerPlanVisitor;
import de.tuberlin.dima.minidb.optimizer.SortPlanOperator;
import de.tuberlin.dima.minidb.optimizer.TableScanPlanOperator;
import de.tuberlin.dima.minidb.semantics.BaseTableAccess;

public class PhysicalPlanPrinter implements OptimizerPlanVisitor
{
	private final int identStep = 2;

	private final boolean printExtended;

	private int ident = 0;


	public PhysicalPlanPrinter()
	{
		this(false);
	}


	public PhysicalPlanPrinter(boolean printExtended)
	{
		this.printExtended = printExtended;
	}


	public void print(OptimizerPlanOperator operator)
	{
		operator.accept(this);
	}


	@Override
	public void preVisit(OptimizerPlanOperator operator)
	{
		if (operator instanceof BaseTableAccess)
		{
			preVisit((BaseTableAccess) operator);
		}
		else if (operator instanceof FetchPlanOperator)
		{
			preVisit((FetchPlanOperator) operator);
		}
		else if (operator instanceof FilterPlanOperator)
		{
			preVisit((FilterPlanOperator) operator);
		}
		else if (operator instanceof IndexLookupPlanOperator)
		{
			preVisit((IndexLookupPlanOperator) operator);
		}
		else if (operator instanceof MergeJoinPlanOperator)
		{
			preVisit((MergeJoinPlanOperator) operator);
		}
		else if (operator instanceof NestedLoopJoinPlanOperator)
		{
			preVisit((NestedLoopJoinPlanOperator) operator);
		}
		else if (operator instanceof AbstractJoinPlanOperator)
		{
			preVisit((AbstractJoinPlanOperator) operator);
		}
		else if (operator instanceof SortPlanOperator)
		{
			preVisit((SortPlanOperator) operator);
		}
		else if (operator instanceof TableScanPlanOperator)
		{
			preVisit((TableScanPlanOperator) operator);
		}
		else
		{
			System.out.println(repeatString(" ", this.ident) + operator.getName());
		}

		this.ident += this.identStep;
	}


	@Override
	public void postVisit(OptimizerPlanOperator operator)
	{
		this.ident -= this.identStep;
	}

	private void preVisit(AbstractJoinPlanOperator operator)
	{
		if (this.printExtended)
		{
			System.out.println(String.format("%s%s [%s] (op.cost = %d, cum.cost = %d, card. = %d)", repeatString(" ", this.ident), operator.getName(), operator.getJoinPredicate().toString(), operator.getOperatorCosts(), operator.getCumulativeCosts(), operator.getOutputCardinality()));
		}
		else
		{
			System.out.println(String.format("%s%s [%s] ", repeatString(" ", this.ident), operator.getName(), operator.getJoinPredicate().toString()));
		}
	}

	private void preVisit(BaseTableAccess operator)
	{
		if (this.printExtended)
		{
			System.out.println(String.format("%s%s [%s] (op.cost = %d, cum.cost = %d, card. = %d)", repeatString(" ", this.ident), operator.getName(), operator.getTable().getTableName(), operator.getOperatorCosts(), operator.getCumulativeCosts(), operator.getOutputCardinality()));
		}
		else
		{
			System.out.println(String.format("%s%s [%s]", repeatString(" ", this.ident), operator.getName(), operator.getTable().getTableName()));
		}
	}
	
	private void preVisit(FetchPlanOperator operator)
	{
		if (this.printExtended)
		{
			System.out.println(String.format("%s%s [%s] (op.cost = %d, cum.cost = %d, card. = %d)", repeatString(" ", this.ident), operator.getName(), operator.getAccessedTable().getTableName(), operator.getOperatorCosts(), operator.getCumulativeCosts(), operator.getOutputCardinality()));
		}
		else
		{
			System.out.println(String.format("%s%s [%s]", repeatString(" ", this.ident), operator.getName(), operator.getAccessedTable().getTableName()));
		}
	}

	private void preVisit(FilterPlanOperator operator)
	{
		if (this.printExtended)
		{
			System.out.println(String.format("%s%s [%s] (op.cost = %d, cum.cost = %d, card. = %d)", repeatString(" ", this.ident), operator.getName(), operator.getSimplePredicate().toString(), operator.getOperatorCosts(), operator.getCumulativeCosts(), operator.getOutputCardinality()));
		}
		else
		{
			System.out.println(String.format("%s%s [%s]", repeatString(" ", this.ident), operator.getName(), operator.getSimplePredicate().toString()));
		}
	}


	private void preVisit(IndexLookupPlanOperator operator)
	{
		if (this.printExtended)
		{
			System.out.println(String.format("%s%s [%s] (op.cost = %d, cum.cost = %d, card. = %d)", repeatString(" ", this.ident), operator.getName(), operator.getIndex().getName(), operator.getOperatorCosts(), operator.getCumulativeCosts(), operator.getOutputCardinality()));
		}
		else
		{
			System.out.println(String.format("%s%s [%s]", repeatString(" ", this.ident), operator.getName(), operator.getIndex().getName()));
		}
	}


	private void preVisit(NestedLoopJoinPlanOperator operator)
	{
		if (this.printExtended)
		{
			System.out.println(String.format("%s%s (op.cost = %d, cum.cost = %d, card. = %d)", repeatString(" ", this.ident), operator.getName(), operator.getOperatorCosts(), operator.getCumulativeCosts(), operator.getOutputCardinality()));
		}
		else
		{
			System.out.println(String.format("%s%s", repeatString(" ", this.ident), operator.getName()));
		}
	}


	private void preVisit(MergeJoinPlanOperator operator)
	{
		if (this.printExtended)
		{
			System.out.println(String.format("%s%s (op.cost = %d, cum.cost = %d, card. = %d)", repeatString(" ", this.ident), operator.getName(), operator.getOperatorCosts(), operator.getCumulativeCosts(), operator.getOutputCardinality()));
		}
		else
		{
			System.out.println(String.format("%s%s", repeatString(" ", this.ident), operator.getName()));
		}
	}


	private void preVisit(SortPlanOperator operator)
	{
		if (this.printExtended)
		{
			System.out.println(String.format("%s%s (op.cost = %d, cum.cost = %d, card. = %d)", repeatString(" ", this.ident), operator.getName(), operator.getOperatorCosts(), operator.getCumulativeCosts(), operator.getOutputCardinality()));
		}
		else
		{
			System.out.println(String.format("%s%s", repeatString(" ", this.ident), operator.getName()));
		}
	}


	private void preVisit(TableScanPlanOperator operator)
	{
		if (this.printExtended)
		{
			System.out.println(String.format("%s%s [%s] (op.cost = %d, cum.cost = %d, card. = %d)", repeatString(" ", this.ident), operator.getName(), operator.getTable().getTableName(), operator.getOperatorCosts(), operator.getCumulativeCosts(), operator.getOutputCardinality(), operator.getOutputCardinality()));
		}
		else
		{
			System.out.println(String.format("%s%s [%s]", repeatString(" ", this.ident), operator.getName(), operator.getTable().getTableName()));
		}
	}


	private static String repeatString(String s, int n)
	{
		return new String(new char[n]).replace("\0", s);
	}
}
