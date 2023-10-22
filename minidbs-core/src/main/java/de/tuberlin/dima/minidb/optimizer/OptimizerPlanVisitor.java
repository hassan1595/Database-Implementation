package de.tuberlin.dima.minidb.optimizer;

public interface OptimizerPlanVisitor
{
        void preVisit(OptimizerPlanOperator operator);
        
        void postVisit(OptimizerPlanOperator operator);
}