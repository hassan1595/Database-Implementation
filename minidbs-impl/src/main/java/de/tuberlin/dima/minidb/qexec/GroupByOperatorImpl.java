package de.tuberlin.dima.minidb.qexec;

import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.core.DataType;
import de.tuberlin.dima.minidb.parser.OutputColumn;

public class GroupByOperatorImpl implements GroupByOperator {
    private final PhysicalPlanOperator child;
    private final int[] groupColumnIndices;
    private final int[] aggColumnIndices;
    private final OutputColumn.AggregationType[] aggregateFunctions;
    private final DataType[] aggColumnTypes;
    private final int[] groupColumnOutputPositions;
    private final int[] aggregateColumnOutputPosition;

    public GroupByOperatorImpl(PhysicalPlanOperator child, int[] groupColumnIndices, int[] aggColumnIndices, OutputColumn.AggregationType[] aggregateFunctions, DataType[] aggColumnTypes, int[] groupColumnOutputPositions, int[] aggregateColumnOutputPosition) {
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

    }

    @Override
    public DataTuple next() throws QueryExecutionException {
        return null;
    }

    @Override
    public void close() throws QueryExecutionException {

    }
}
