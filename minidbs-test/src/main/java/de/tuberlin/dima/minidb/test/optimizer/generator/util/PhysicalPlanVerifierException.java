package de.tuberlin.dima.minidb.test.optimizer.generator.util;

import de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator;
import de.tuberlin.dima.minidb.optimizer.generator.util.PhysicalPlanPrinter;

public class PhysicalPlanVerifierException extends RuntimeException
{
	/**
     * 
     */
	private static final long serialVersionUID = 4476088191775595004L;
	
	
	private final OptimizerPlanOperator expSubplan;
	private final OptimizerPlanOperator actSubplan;
	private final PhysicalPlanPrinter planPrinter;

	PhysicalPlanVerifierException(String message, OptimizerPlanOperator expSubplan, OptimizerPlanOperator actSubplan)
	{
		super(message);
		
		this.expSubplan = expSubplan;
		this.actSubplan = actSubplan;
		
		this.planPrinter = new PhysicalPlanPrinter(true);
	}

	PhysicalPlanVerifierException(OptimizerPlanOperator expSubplan, OptimizerPlanOperator actSubplan)
	{
		super("Subplans differ");
		
		this.expSubplan = expSubplan;
		this.actSubplan = actSubplan;
		
		this.planPrinter = new PhysicalPlanPrinter(true);
	}
	
	
	public void dumpMessage()
	{
		System.out.println("Subplan mismatch: " + this.getMessage());
		System.out.println("");
		
		System.out.println("Expected subplan:");
		this.planPrinter.print(this.expSubplan);
		System.out.println("");
		
		System.out.println("Actual subplan:");
		this.planPrinter.print(this.actSubplan);
	}
}
