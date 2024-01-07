package de.tuberlin.dima.minidb.qexec;

import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataTuple;

import java.util.ArrayList;

public class MergeJoinOperatorClass implements MergeJoinOperator {
    private  PhysicalPlanOperator leftChild;
    private  PhysicalPlanOperator rightChild;
    private  int[] leftJoinColumns;
    private  int[] rightJoinColumns;
    private  int[] columnMapLeftTuple;
    private  int[] columnMapRightTuple;

    private DataTuple lastLeftTuple;
    private DataTuple lastRightTuple;
    private boolean foundEqual;
    private ArrayList<DataTuple> equalTuples;
    private int curremtEqualTuplesIndex;
    private boolean opened;

    public MergeJoinOperatorClass(PhysicalPlanOperator leftChild, PhysicalPlanOperator rightChild, int[] leftJoinColumns, int[] rightJoinColumns, int[] columnMapLeftTuple, int[] columnMapRightTuple) {
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.leftJoinColumns = leftJoinColumns;
        this.rightJoinColumns = rightJoinColumns;
        this.columnMapLeftTuple = columnMapLeftTuple;
        this.columnMapRightTuple = columnMapRightTuple;
        this.equalTuples = new ArrayList<DataTuple>();
        this.curremtEqualTuplesIndex = 0;
        this.foundEqual = false;
    }

    @Override
    public void open(DataTuple correlatedTuple) throws QueryExecutionException {
        this.leftChild.open(correlatedTuple);
        this.lastLeftTuple = this.leftChild.next();
        this.rightChild.open(correlatedTuple);
        this.lastRightTuple = this.rightChild.next();
        this.opened=true;

    }
    private DataTuple projectJoinedTuples( DataTuple leftTuple, DataTuple rightTuple, int[] columnMapLeftTuple, int[] columnMapRightTuple) throws QueryExecutionException {

        DataField[] orderedFields = new DataField[columnMapLeftTuple.length];

        if(columnMapRightTuple.length != columnMapLeftTuple.length){
            throw new QueryExecutionException("Column Maps must be of equal length");
        }

        for(int i = 0; i < columnMapLeftTuple.length; i++){

            if(columnMapLeftTuple[i] != -1){
                orderedFields[i] = leftTuple.getField(columnMapLeftTuple[i]);
            }
            else{
                if(columnMapRightTuple[i] == -1){
                    throw new QueryExecutionException("At each index of Column Maps only one map has to have -1");
                }
                orderedFields[i] = rightTuple.getField(columnMapRightTuple[i]);
            }
        }
        return new DataTuple(orderedFields);
    }
    private int compareTuples(DataTuple leftTuple, DataTuple rightTuple, int[] leftJoinColumns, int[] rightJoinColumns) throws QueryExecutionException {

        if(leftJoinColumns.length != rightJoinColumns.length){
            throw new QueryExecutionException("Column Maps must be of equal length");
        }
        int result;
        for(int i = 0; i < leftJoinColumns.length; i ++){
            DataField val1 =  leftTuple.getField(leftJoinColumns[i]);
            DataField val2 =  rightTuple.getField(rightJoinColumns[i]);
            result = val1.compareTo(val2);
            if (result != 0) {
                return result;
            }
        }


        return 0; // If all fields are equal
    }

    @Override
    public DataTuple next() throws QueryExecutionException {

        if(!opened){
            throw new QueryExecutionException("Operator is not yet opened");
        }


        if(foundEqual){
            if(this.curremtEqualTuplesIndex < this.equalTuples.size()){
                DataTuple resultTuple = projectJoinedTuples(this.lastLeftTuple, this.equalTuples.get(this.curremtEqualTuplesIndex),
                        this.columnMapLeftTuple, this.columnMapRightTuple);
                this.curremtEqualTuplesIndex++;
                return resultTuple;
            }
            else{
                this.curremtEqualTuplesIndex = 0;
                this.lastLeftTuple = this.leftChild.next();
                int compareNewResult = -1;
                if(this.lastLeftTuple != null){
                    compareNewResult = this.compareTuples(this.lastLeftTuple , this.equalTuples.get(0), this.leftJoinColumns, this.rightJoinColumns);
                }
                if(compareNewResult == 0){
                    DataTuple resultTuple = projectJoinedTuples(this.lastLeftTuple, this.equalTuples.get(this.curremtEqualTuplesIndex),
                            this.columnMapLeftTuple, this.columnMapRightTuple);
                    this.curremtEqualTuplesIndex++;
                    return resultTuple;
                }
                else{
                    this.foundEqual = false;
                    this.equalTuples.clear();;
                    this.curremtEqualTuplesIndex = 0;
                }
            }

        }

        while(this.lastLeftTuple != null && this.lastRightTuple != null){



            int compareResult = this.compareTuples(this.lastLeftTuple , this.lastRightTuple, this.leftJoinColumns, this.rightJoinColumns);
            if(compareResult == 0){
                this.foundEqual = true;


                while(compareResult == 0 ){

                    this.equalTuples.add(this.lastRightTuple);
                    this.lastRightTuple = this.rightChild.next();
                    if(this.lastRightTuple == null){
                        break;
                    }
                    compareResult = this.compareTuples(this.lastLeftTuple , this.lastRightTuple, this.leftJoinColumns, this.rightJoinColumns);

                }

                DataTuple resultTuple = projectJoinedTuples(this.lastLeftTuple, this.equalTuples.get(this.curremtEqualTuplesIndex),
                        this.columnMapLeftTuple, this.columnMapRightTuple);
                this.curremtEqualTuplesIndex++;
                return resultTuple;
            }
            else if(compareResult > 0){
                this.lastRightTuple = this.rightChild.next();
            }
            else{
                this.lastLeftTuple = this.leftChild.next();
            }

        }


        return null;
    }

    @Override
    public void close() throws QueryExecutionException {
        this.leftChild = null;
        this.rightChild = null;
        this.leftJoinColumns = null;
        this.rightJoinColumns = null;
        this.columnMapLeftTuple = null;
        this.columnMapRightTuple = null;

    }
}
