package de.tuberlin.dima.minidb.qexec;

import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.core.DataType;
import de.tuberlin.dima.minidb.qexec.heap.ExternalTupleSequenceIterator;
import de.tuberlin.dima.minidb.qexec.heap.QueryHeap;
import de.tuberlin.dima.minidb.qexec.heap.QueryHeapException;

import java.io.IOException;
import java.util.*;

public class SortOperatorClass implements SortOperator {
    private PhysicalPlanOperator child;

    private QueryHeap queryHeap;

    private DataType[] columnTypes;

    private int estimatedCardinality;

    private int[] sortColumns;

    private boolean[] columnsAscending;

    private List<DataTuple> tuplesToSort;

    private boolean sorted = false;
    private int queryHeapId;
    private DataTuple[] sortedMinList;
    private ExternalTupleSequenceIterator[] tupleIterators;

    private Comparator<DataTuple> multiFieldComparator;
    private Iterator<DataTuple> lastListIterator;

    private boolean opened;

    public SortOperatorClass(PhysicalPlanOperator child, QueryHeap queryHeap, DataType[] columnTypes, int estimatedCardinality, int[] sortColumns, boolean[] columnsAscending) {
        this.child = child;
        this.queryHeap = queryHeap;
        this.columnTypes = columnTypes;
        this.estimatedCardinality = estimatedCardinality;
        this.sortColumns = sortColumns;
        this.columnsAscending = columnsAscending;
    }
    private static Comparator<DataTuple> getMultiFieldComparator(int[] sortColumns, boolean[] columnsAscending) {

        return (tuple1, tuple2) -> {
            for (int i = 0; i < sortColumns.length; i++) {
                int columnIndex = sortColumns[i];
                boolean ascending = columnsAscending[i];

                DataField val1 =  tuple1.getField(columnIndex);
                DataField val2 =  tuple2.getField(columnIndex);

                int result = ascending ? val1.compareTo(val2) : val2.compareTo(val1);
                if (result != 0) {
                    return result;
                }
            }
            return 0; // If all fields are equal
        };
    }

    @Override
    public void open(DataTuple correlatedTuple) throws QueryExecutionException {
        if (this.child != null) {
            this.child.open(correlatedTuple);
        }
        try {
            this.queryHeapId = this.queryHeap.reserveSortHeap(this.columnTypes, this.estimatedCardinality);
        } catch (QueryHeapException e) {
            throw new QueryExecutionException(e);
        }
        this.sorted = false;
        this.multiFieldComparator = this.getMultiFieldComparator(this.sortColumns, this.columnsAscending);
        this.opened=true;
    }



    @Override
    public DataTuple next() throws QueryExecutionException {

        if(!opened){
            throw new QueryExecutionException("Operator is not yet opened");
        }
        try {
            if(!this.sorted) {
                DataTuple nextChildTuple = this.child.next();
                int maximumCardinality;
                int sortIndex = 0;
                DataTuple[] subList;
                boolean heapUsed = false;
                subList = this.queryHeap.getSortArray(this.queryHeapId);
                maximumCardinality = this.queryHeap.getMaximalTuplesForInternalSort(this.queryHeapId);
                while (nextChildTuple != null) {


                    if (sortIndex < maximumCardinality) {
                        subList[sortIndex] = nextChildTuple;
                        sortIndex++;
                    } else {

                        Arrays.sort(subList, this.multiFieldComparator);
                        this.queryHeap.writeTupleSequencetoTemp(this.queryHeapId, subList, maximumCardinality);
                        heapUsed = true;
                        sortIndex = 0;
                        subList[sortIndex] = nextChildTuple;
                        sortIndex++;
                    }
                    nextChildTuple = this.child.next();
                }


                Arrays.sort(subList,0, sortIndex, this.multiFieldComparator);
                List<DataTuple> l = Arrays.asList(Arrays.copyOfRange(subList, 0, sortIndex));
                this.lastListIterator = l.iterator();
                ExternalTupleSequenceIterator[] tupleIterators = heapUsed?this.queryHeap.getExternalSortedLists(this.queryHeapId) : new ExternalTupleSequenceIterator[0];
                this.tupleIterators = tupleIterators;
                this.sortedMinList = new DataTuple[tupleIterators.length + 1];

                for (int i = 0; i < sortedMinList.length; i++) {

                    if(i ==sortedMinList.length - 1 ){
                        if(this.lastListIterator.hasNext()){

                            this.sortedMinList[i] = lastListIterator.next();
                        } else {
                            this.sortedMinList[i] = null;
                        }
                    }
                    else{
                        ExternalTupleSequenceIterator tupleIterator = tupleIterators[i];
                        if (tupleIterator.hasNext()) {
                            this.sortedMinList[i] = tupleIterator.next();
                        } else {
                            this.sortedMinList[i] = null;
                        }
                    }


                }
                this.sorted = true;
            }
            int minIndex = -1;
            DataTuple minTuple = null;


            for(int i = 0; i < this.sortedMinList.length; i ++){
                DataTuple currentTuple = this.sortedMinList[i];
                if( currentTuple != null && (minTuple == null || this.multiFieldComparator.compare(minTuple, currentTuple) > 0)){
                    minTuple = this.sortedMinList[i];
                    minIndex = i;
                }

            }
            if(minTuple != null){
                if(minIndex != this.tupleIterators.length)
                {
                    this.sortedMinList[minIndex] = this.tupleIterators[minIndex].hasNext()?this.tupleIterators[minIndex].next():null;
                }
                else{
                    this.sortedMinList[minIndex] = this.lastListIterator.hasNext()?this.lastListIterator.next():null;
                }

            }

            return minTuple;
            } catch (QueryHeapException | IOException e) {
                throw new QueryExecutionException(e);
            }

    }

    @Override
    public void close() throws QueryExecutionException {
        if (this.child != null) {
            this.child.close();
        }

        this.queryHeap.releaseSortHeap(this.queryHeapId);
        this.columnTypes = null;
        this.columnsAscending = null;
        this.queryHeap = null;
        this.sorted = false;
    }
}
