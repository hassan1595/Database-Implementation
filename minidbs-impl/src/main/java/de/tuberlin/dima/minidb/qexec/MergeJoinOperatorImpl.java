package de.tuberlin.dima.minidb.qexec;

import de.tuberlin.dima.minidb.core.DataTuple;

public class MergeJoinOperatorImpl implements MergeJoinOperator {
    private final PhysicalPlanOperator leftChild;
    private final PhysicalPlanOperator rightChild;
    private final int[] leftJoinColumns;
    private final int[] rightJoinColumns;
    private final int[] columnMapLeftTuple;
    private final int[] columnMapRightTuple;

    public MergeJoinOperatorImpl(PhysicalPlanOperator leftChild, PhysicalPlanOperator rightChild, int[] leftJoinColumns, int[] rightJoinColumns, int[] columnMapLeftTuple, int[] columnMapRightTuple) {
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.leftJoinColumns = leftJoinColumns;
        this.rightJoinColumns = rightJoinColumns;
        this.columnMapLeftTuple = columnMapLeftTuple;
        this.columnMapRightTuple = columnMapRightTuple;
    }

    @Override
    public void open(DataTuple correlatedTuple) throws QueryExecutionException {

    }

    @Override
    public DataTuple next() throws QueryExecutionException {
        return null;
    }

    @Override
    public void close() throws QueryExecutionException {

    }
}
