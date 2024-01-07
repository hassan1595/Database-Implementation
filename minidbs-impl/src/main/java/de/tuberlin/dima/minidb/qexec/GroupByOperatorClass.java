package de.tuberlin.dima.minidb.qexec;

import de.tuberlin.dima.minidb.core.*;
import de.tuberlin.dima.minidb.parser.OutputColumn;

public class GroupByOperatorClass implements GroupByOperator {
    private  PhysicalPlanOperator child;
    private  int[] groupColumnIndices;
    private  int[] aggColumnIndices;
    private  OutputColumn.AggregationType[] aggregateFunctions;
    private  DataType[] aggColumnTypes;
    private  int[] groupColumnOutputPositions;
    private  int[] aggregateColumnOutputPosition;
    private DataTuple firstTuple;

    private boolean basicCase;
    private boolean opened;



    public GroupByOperatorClass(PhysicalPlanOperator child, int[] groupColumnIndices, int[] aggColumnIndices, OutputColumn.AggregationType[] aggregateFunctions, DataType[] aggColumnTypes, int[] groupColumnOutputPositions, int[] aggregateColumnOutputPosition) {
        this.child = child;
        this.groupColumnIndices = groupColumnIndices;
        this.aggColumnIndices = aggColumnIndices;
        this.aggregateFunctions = aggregateFunctions;
        this.aggColumnTypes = aggColumnTypes;
        this.groupColumnOutputPositions = groupColumnOutputPositions;
        this.aggregateColumnOutputPosition = aggregateColumnOutputPosition;

    }

    @Override
    public void open(DataTuple correlatedTuple) throws QueryExecutionException {

        this.child.open(correlatedTuple);
        this.firstTuple = this.child.next();
        if(this.firstTuple == null && this.groupColumnIndices.length == 0 ){
            this.basicCase = true;
        }
        else{
            this.basicCase = false;
        }
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
    private DataField[] constructFields(DataTuple tuple, int[] columnIndices){
        DataField[] fields = new DataField[columnIndices.length];

        for(int i = 0 ; i < columnIndices.length; i ++){
            fields[i] = tuple.getField(columnIndices[i]).clone();
        }

        return fields;

    }

    private DataField[] constructAggregateFields(DataTuple tuple, int[] columnIndices, OutputColumn.AggregationType[] aggregateFunctions, DataType[] aggColumnTypes) throws QueryExecutionException {
        DataField[] aggFields = new DataField[aggregateFunctions.length];

        for(int i = 0 ; i < aggregateFunctions.length; i ++){


            if(this.aggregateFunctions[i] == OutputColumn.AggregationType.SUM){
                if (!this.aggColumnTypes[i].isArithmeticType()) {
                    throw new QueryExecutionException("Field is not Arithmetic");
                }
                ArithmeticType<DataField> zero = aggColumnTypes[i].createArithmeticZero();
                zero.add(tuple.getField(columnIndices[i]));
                aggFields[i] = (DataField) zero;

            }
            if(this.aggregateFunctions[i] == OutputColumn.AggregationType.AVG){
                ArithmeticType<DataField> zero = aggColumnTypes[i].createArithmeticZero();
                zero.add(tuple.getField(columnIndices[i]));
                aggFields[i] = (DataField) zero;
            }

            if(this.aggregateFunctions[i] == OutputColumn.AggregationType.MAX) {

                aggFields[i] = tuple.getField(columnIndices[i]).clone();
            }

            if(this.aggregateFunctions[i] == OutputColumn.AggregationType.MIN) {

                aggFields[i]  = tuple.getField(columnIndices[i]).clone();
            }

            if(this.aggregateFunctions[i] == OutputColumn.AggregationType.COUNT) {


                aggFields[i] = new IntField(1);
            }
        }

        return aggFields;

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

        if(this.basicCase){
            this.basicCase = false;
            DataField[] aggFields = new DataField[this.aggColumnIndices.length];
            for(int i = 0 ; i < this.aggregateFunctions.length; i ++) {

                if(this.aggregateFunctions[i] == OutputColumn.AggregationType.SUM){
                    if (!this.aggColumnTypes[i].isArithmeticType()) {
                        throw new QueryExecutionException("Field is not Arithmetic");
                    }
                    aggFields[i] = (DataField) this.aggColumnTypes[i].createArithmeticZero();
                }
                if(this.aggregateFunctions[i] == OutputColumn.AggregationType.AVG){
                    aggFields[i] = this.aggColumnTypes[i].getNullValue();
                }

                if(this.aggregateFunctions[i] == OutputColumn.AggregationType.MAX) {

                    aggFields[i] = this.aggColumnTypes[i].getNullValue();
                }

                if(this.aggregateFunctions[i] == OutputColumn.AggregationType.MIN) {

                    aggFields[i] = this.aggColumnTypes[i].getNullValue();
                }

                if(this.aggregateFunctions[i] == OutputColumn.AggregationType.COUNT) {

                    aggFields[i] = new IntField((0));
                }

            }

            return new DataTuple(aggFields);

        }

        if(this.firstTuple == null){
            return  null;
        }
        DataTuple  nextTuple = this.child.next();

        DataTuple groupTuple = new DataTuple(this.constructFields(firstTuple, this.groupColumnIndices));

        DataField[] aggFields = this.constructAggregateFields(firstTuple, this.aggColumnIndices, this.aggregateFunctions, this.aggColumnTypes);



        int num =1;
        while(nextTuple != null && this.compareTuples(firstTuple, nextTuple, this.groupColumnIndices,this.groupColumnIndices) == 0){
            num++;

            for(int i = 0 ; i < this.aggregateFunctions.length; i ++){

                DataField field = nextTuple.getField(this.aggColumnIndices[i]);

                if(this.aggregateFunctions[i] == OutputColumn.AggregationType.SUM){

                    if (!(field instanceof ArithmeticType )) {
                        throw new QueryExecutionException("Field is not Arithmetic");
                    }
                    ((ArithmeticType) aggFields[i]).add(field);

                }
                if(this.aggregateFunctions[i] == OutputColumn.AggregationType.AVG){
                    if (!(field instanceof ArithmeticType )) {
                        throw new QueryExecutionException("Field is not Arithmetic");
                    }
                    ((ArithmeticType) aggFields[i]).add(field);
                }

                if(this.aggregateFunctions[i] == OutputColumn.AggregationType.MAX) {



                    int result = aggFields[i].compareTo(field);
                    if (result < 0) {
                        aggFields[i] = field;

                    }
                }

                if(this.aggregateFunctions[i] == OutputColumn.AggregationType.MIN) {

                    int result = aggFields[i].compareTo(field);
                    if (result > 0) {

                        aggFields[i] = field;
                    }
                }

                if(this.aggregateFunctions[i] == OutputColumn.AggregationType.COUNT) {

                    ((ArithmeticType) aggFields[i]).add(new IntField(1));
                }


            }
            nextTuple = this.child.next();

        }
        this.firstTuple = nextTuple;
        for(int i = 0 ; i < this.aggregateFunctions.length; i ++) {
            if(this.aggregateFunctions[i] == OutputColumn.AggregationType.AVG) {
                if (!this.aggColumnTypes[i].isArithmeticType()) {
                    throw new QueryExecutionException("Field is not Arithmetic");
                }
                ((ArithmeticType) aggFields[i]).divideBy(num);
            }

        }

        DataTuple aggTuple = new DataTuple(aggFields);
        return this.projectJoinedTuples(groupTuple, aggTuple, this.groupColumnOutputPositions, this.aggregateColumnOutputPosition);

    }

    @Override
    public void close() throws QueryExecutionException {
        this.child = null;
        this.groupColumnIndices = null;
        this.aggColumnIndices = null;
        this.aggregateFunctions = null;
        this.aggColumnTypes = null;
        this.groupColumnOutputPositions = null;
        this.aggregateColumnOutputPosition = null;
        this.firstTuple=null;

    }
}
