package de.tuberlin.dima.minidb.qexec;

import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.core.DataType;
import de.tuberlin.dima.minidb.qexec.heap.QueryHeap;

import java.util.ArrayList;
import java.util.List;

public class SortOperatorImp implements SortOperator {
    private PhysicalPlanOperator child;

    private QueryHeap queryHeap;

    private DataType[] columnTypes;

    private int estimatedCardinality;

    private int[] sortColumns;

    private boolean[] columnsAscending;

    private List<DataTuple> tuplesToSort;

    private boolean sorted = false;


    public SortOperatorImp(PhysicalPlanOperator child, QueryHeap queryHeap, DataType[] columnTypes, int estimatedCardinality, int[] sortColumns, boolean[] columnsAscending) {
        this.child = child;
        this.queryHeap = queryHeap;
        this.columnTypes = columnTypes;
        this.estimatedCardinality = estimatedCardinality;
        this.sortColumns = sortColumns;
        this.columnsAscending = columnsAscending;
        this.tuplesToSort = new ArrayList<>();
    }

    @Override
    public void open(DataTuple correlatedTuple) throws QueryExecutionException {
        if (child != null) {
            child.open(correlatedTuple);
        }

        tuplesToSort.clear();
        sorted = false;
    }

    @Override
    public DataTuple next() throws QueryExecutionException {
        return null;
    }

    @Override
    public void close() throws QueryExecutionException {
        if (child != null) {
            child.close();
        }

        tuplesToSort.clear();
        sorted = false;
    }
}
