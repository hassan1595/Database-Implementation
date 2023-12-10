package de.tuberlin.dima.minidb.qexec;

import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.qexec.predicate.LocalPredicate;

public class FilterOperatorClass implements  FilterOperator{

    private PhysicalPlanOperator child;
    private LocalPredicate predicate;
    private boolean opened;

    public  FilterOperatorClass(PhysicalPlanOperator child, LocalPredicate predicate){
        this.child = child;
        this.predicate = predicate;
        this.opened = false;

    }

    @Override
    public void open(DataTuple correlatedTuple) throws QueryExecutionException {

        this.child.open(correlatedTuple);
        this.opened= true;

    }

    @Override
    public DataTuple next() throws QueryExecutionException {
        if(!opened){
            throw new QueryExecutionException("Operator is not yet opened");
        }
        DataTuple nextTuple = this.child.next();

        while(nextTuple != null){
            if(this.predicate.evaluate(nextTuple)){
                return nextTuple;
            }
            nextTuple = this.child.next();
        }
        return null;
    }

    @Override
    public void close() throws QueryExecutionException {

        this.child = null;
        this.predicate = null;
    }
}
