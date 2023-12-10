package de.tuberlin.dima.minidb.qexec;

import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.qexec.predicate.JoinPredicate;

public class FilterCorrelatedOperatorClass implements  FilterCorrelatedOperator{

    private PhysicalPlanOperator child;
    private JoinPredicate correlatedPredicate;

    private DataTuple correlatedTuple;
    private boolean opened;

    public FilterCorrelatedOperatorClass(PhysicalPlanOperator child, JoinPredicate correlatedPredicate){

        this.child = child;
        this.correlatedPredicate = correlatedPredicate;
        this.opened = false;
    }
    @Override
    public void open(DataTuple correlatedTuple) throws QueryExecutionException {

        this.correlatedTuple = correlatedTuple;
        this.child.open(correlatedTuple);
        this.opened = true;
    }

    @Override
    public DataTuple next() throws QueryExecutionException {
        if(!opened){
            throw new QueryExecutionException("Operator is not yet opened");
        }

        DataTuple nextTuple = this.child.next();

        while(nextTuple != null){
            if(this.correlatedPredicate.evaluate( nextTuple, this.correlatedTuple)){
                return nextTuple;
            }
            nextTuple = this.child.next();
        }
        return null;
    }

    @Override
    public void close() throws QueryExecutionException {

        this.child = null;
        this.correlatedPredicate = null;

    }
}
