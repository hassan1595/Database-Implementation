package de.tuberlin.dima.minidb.qexec;

import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.qexec.predicate.JoinPredicate;

public class NestedLoopJoinOperatorClass implements  NestedLoopJoinOperator{

    private PhysicalPlanOperator outerChild;
    private PhysicalPlanOperator innerChild;
    private JoinPredicate joinPredicate;
    private int[] columnMapOuterTuple;
    private int[] columnMapInnerTuple;
    private  boolean joinFinished;
    private DataTuple currentOuterTuple;

    boolean opened;
    public NestedLoopJoinOperatorClass (PhysicalPlanOperator outerChild, PhysicalPlanOperator innerChild, JoinPredicate joinPredicate,
                                        int[] columnMapOuterTuple, int[] columnMapInnerTuple){
        this.outerChild = outerChild;
        this.innerChild = innerChild;
        this.joinPredicate = joinPredicate;
        this.columnMapOuterTuple = columnMapOuterTuple;
        this.columnMapInnerTuple = columnMapInnerTuple;
        this.opened = false;
    }
    @Override
    public void open(DataTuple correlatedTuple) throws QueryExecutionException {
        this.outerChild.open(correlatedTuple);
        this.joinFinished = false;
        this.currentOuterTuple = this.outerChild.next();
        if(this.currentOuterTuple == null){
            this.joinFinished = true;
        }
        else{
            this.innerChild.open(this.currentOuterTuple);
        }

        this.opened = true;

    }

    private DataTuple projectjoinedTuples( DataTuple outer, DataTuple inner, int[] columnMapOuterTuple, int[] columnMapInnerTuple) throws QueryExecutionException {

        DataField[] orderedFields = new DataField[columnMapOuterTuple.length];

        if(columnMapInnerTuple.length != columnMapOuterTuple.length){
            throw new QueryExecutionException("Column Maps must be of equal length");
        }

        for(int i = 0; i < columnMapOuterTuple.length; i++){

            if(columnMapOuterTuple[i] != -1){
                orderedFields[i] = outer.getField(columnMapOuterTuple[i]);
            }
            else{
                if(columnMapInnerTuple[i] == -1){
                    throw new QueryExecutionException("At each index of olumn Maps only one map has to have -1");
                }
                orderedFields[i] = inner.getField(columnMapInnerTuple[i]);
            }
        }
        return new DataTuple(orderedFields);
    }
    @Override
    public DataTuple next() throws QueryExecutionException {

        if(!opened){
            throw new QueryExecutionException("Operator is not yet opened");
        }

        while(!this.joinFinished){

            DataTuple nextInnerTuple = this.innerChild.next();
            if(nextInnerTuple != null){

                if(this.joinPredicate == null ||
                        this.joinPredicate.evaluate(this.currentOuterTuple, nextInnerTuple)){
                    return projectjoinedTuples(this.currentOuterTuple, nextInnerTuple,
                            this.columnMapOuterTuple, this.columnMapInnerTuple);
                }

            }
            else{
                this.currentOuterTuple = this.outerChild.next();
                if(this.currentOuterTuple == null){
                    this.joinFinished = true;
                }
                else{
                    this.innerChild.open(this.currentOuterTuple);
                }
            }
        }


        return null;
    }

    @Override
    public void close() throws QueryExecutionException {

        this.outerChild = null;
        this.innerChild = null;
        this.joinPredicate = null;
        this.columnMapOuterTuple = null;
        this.columnMapInnerTuple = null;

    }
}
