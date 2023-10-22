package de.tuberlin.dima.minidb.optimizer;


import java.util.Iterator;
import java.util.Set;

import de.tuberlin.dima.minidb.io.manager.BufferPoolManager;
import de.tuberlin.dima.minidb.qexec.PhysicalPlanOperator;
import de.tuberlin.dima.minidb.qexec.heap.QueryHeap;
import de.tuberlin.dima.minidb.semantics.Column;
import de.tuberlin.dima.minidb.semantics.Relation;


/**
 * Base class that all classes representing optimizer plan operators have to
 * extend. Contains basic getter and setter methods for plan costs.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public abstract class OptimizerPlanOperator
{
        // --------------------------------------------------------------------------------------------
        //                                         Costs
        // --------------------------------------------------------------------------------------------
        
        /**
         * The costs produced by this operator.
         */
        private long operatorCosts = -1;
        
        /**
         * The costs produced in total by the sub-plan that is rooted
         * at this operator. 
         */
        private long cumulativeCosts = -1;
        
        
        /**
         * Gets the costs caused by this operator alone. The costs may be zero, if the
         * cost estimator has not been called on this operator.
         * 
         * @return This operators costs.
         */
        public long getOperatorCosts()
        {
                return this.operatorCosts;
        }
        
        /**
         * Gets the costs produced by the whole sub-plan that is rooted
         * at this operator. The costs may be zero, if the
         * cost estimator has not been called on this operator.
         * 
         * @return The cumulative costs for this operator.
         */
        public long getCumulativeCosts()
        {
                return this.cumulativeCosts;
        }
        
        /**
         * Sets the costs caused by this operator alone.
         * 
         * @param costs The costs for this operator.
         */
        public void setOperatorCosts(long costs)
        {
                this.operatorCosts = costs;
        }
        
        /**
         * Sets the costs produced by the whole sub-plan that is rooted
         * at this operator.
         * 
         * @param costs The cumulative costs for this operator and its sub-plan.
         */
        public void setCumulativeCosts(long costs)
        {
                this.cumulativeCosts = costs;
        }
        
        // --------------------------------------------------------------------------------------------
        //                                         Visitor
        // --------------------------------------------------------------------------------------------
        
        public void accept(OptimizerPlanVisitor visitor)
        {
                visitor.preVisit(this);
                
                Iterator<OptimizerPlanOperator> children = getChildren();
                while(children.hasNext())
                {
                        children.next().accept(visitor);
                }
                
                visitor.postVisit(this);
        }
        
        // --------------------------------------------------------------------------------------------
        //                                    Abstract Signatures
        // --------------------------------------------------------------------------------------------
        
        /**
         * Gets the operator's name.
         * 
         * @return The operator's name.
         */
        public abstract String getName();
        
        /**
         * Gets the output cardinality of this operator.
         * 
         * @return The output cardinality of this operator.
         */
        public abstract long getOutputCardinality();
        
        /**
         * Gets all table accesses that are below this operator.
         * 
         * @return All table accesses below this operator.
         */
        public abstract Set<Relation> getInvolvedRelations();
        
        /**
         * Gets the columns in the tuples produced by this operator.
         * 
         * @return The output columns.
         */
        public abstract Column[] getReturnedColumns();
        
        /**
         * Gets the columns after which this operators output is ordered.
         * 
         * @return The order columns.
         */
        public abstract OrderedColumn[] getColumnOrder();
        
        /**
         * Translates this optimizer plan operator into a physical plan operator.
         * The method works recursively, so that the whole sub-plan of this
         * operator is actually translated.
         *  
         * @param buffer The buffer pool that may be required to reference in the physical
         *               plan.
         * @param heap The query heap that may be required to reference in the physical plan.
         * @return The physical plan for the optimizer (sub-) plan rooted at this operator.
         */
        public abstract PhysicalPlanOperator createPhysicalPlan(BufferPoolManager buffer, QueryHeap heap);
        
        /**
         * Gets an iterator over all children of this operator.
         * 
         * @return An iterator over the children.
         */
        public abstract Iterator<OptimizerPlanOperator> getChildren();
}