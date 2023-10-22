package de.tuberlin.dima.minidb.optimizer.joins.util;

import java.io.PrintStream;

import de.tuberlin.dima.minidb.optimizer.AbstractJoinPlanOperator;
import de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator;
import de.tuberlin.dima.minidb.optimizer.OptimizerPlanVisitor;
import de.tuberlin.dima.minidb.semantics.BaseTableAccess;

public class LogicalPlanPrinter implements OptimizerPlanVisitor {

	private final PrintStream out;
	
	private final int identStep = 4;

	private final boolean printExtended;

	private int ident = 0;

	public LogicalPlanPrinter() {
		this(System.out, false);
	}

	public LogicalPlanPrinter(PrintStream out) {
		this(out, false);
	}

	public LogicalPlanPrinter(PrintStream out, boolean printExtended) {
		this.printExtended = printExtended;
		this.out = out;
	}

	public void print(OptimizerPlanOperator operator) {
		operator.accept(this);
	}

	@Override
	public void preVisit(OptimizerPlanOperator operator) {
		if (operator instanceof BaseTableAccess) {
			preVisit((BaseTableAccess) operator);
		} else if (operator instanceof AbstractJoinPlanOperator) {
			preVisit((AbstractJoinPlanOperator) operator);
		} else {
			this.out.println(repeatString(" ", this.ident) + operator.getName());
		}

		this.ident += this.identStep;
	}

	@Override
	public void postVisit(OptimizerPlanOperator operator) {
		this.ident -= this.identStep;
	}

	private void preVisit(AbstractJoinPlanOperator operator) {
		if (this.printExtended) {
			this.out.println(String.format("%s%s [%s] (op.cost = %d, cum.cost = %d, card. = %d)",
				repeatString(" ", this.ident), operator.getName(), operator.getJoinPredicate().toString(),
				operator.getOperatorCosts(), operator.getCumulativeCosts(), operator.getOutputCardinality()));
		} else {
			this.out.println(String.format("%s%s [%s] ", repeatString(" ", this.ident), operator.getName(), operator
				.getJoinPredicate().toString()));
		}
	}

	private void preVisit(BaseTableAccess operator) {
		if (this.printExtended) {
			this.out.println(String.format("%s%s [%s] (op.cost = %d, cum.cost = %d, card. = %d)",
				repeatString(" ", this.ident), operator.getName(), operator.getTable().getTableName(),
				operator.getOperatorCosts(), operator.getCumulativeCosts(), operator.getOutputCardinality()));
		} else {
			this.out.println(String.format("%s%s [%s]", repeatString(" ", this.ident), operator.getName(), operator
				.getTable().getTableName()));
		}
	}

	private static String repeatString(String s, int n) {
		return new String(new char[n]).replace("\0", s);
	}
}
