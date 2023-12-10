package de.tuberlin.dima.minidb.qexec;

import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.core.RID;
import de.tuberlin.dima.minidb.io.cache.CacheableData;
import de.tuberlin.dima.minidb.io.manager.BufferPoolException;
import de.tuberlin.dima.minidb.io.manager.BufferPoolManager;
import de.tuberlin.dima.minidb.io.tables.PageTupleAccessException;
import de.tuberlin.dima.minidb.io.tables.TablePage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public class FetchOperatorClass implements FetchOperator{

    private  PhysicalPlanOperator child;
    private BufferPoolManager bufferPool;
    private int tableResourceId;
    private int[] outputColumnMap;

    private int currentPageNumber;
    private  boolean opened;
    public FetchOperatorClass(PhysicalPlanOperator child, BufferPoolManager bufferPool, int tableResourceId, int[] outputColumnMap){

        this.child = child;
        this.bufferPool = bufferPool;
        this.tableResourceId = tableResourceId;
        this.outputColumnMap = outputColumnMap;
        this.currentPageNumber = -1;
        this.opened = false;
    }
    @Override
    public void open(DataTuple correlatedTuple) throws QueryExecutionException {

        this.child.open(correlatedTuple);
        this.opened = true;
    }

    private int getUniquelength(int[] arr){

        ArrayList<Integer> arrlist = new ArrayList<>();
        for(int x: arr){
            if(!arrlist.contains(x)){
                arrlist.add(x);
            }
        }
        return arrlist.size();
    }
    private long constructBiMap(int[] ColumnIndexes){
        long bitmap = 0;
        for(int idx:ColumnIndexes){
            if((bitmap & 1L << idx) == 0)
            {
                bitmap += 1L << idx;
            }

        }

        return  bitmap;

    }

    @Override
    public DataTuple next() throws QueryExecutionException {
        if(!opened){
            throw new QueryExecutionException("Operator is not yet opened");
        }
        DataTuple newTuple = this.child.next();
        if(newTuple == null){
            if(this.currentPageNumber != -1){
                this.bufferPool.unpinPage(tableResourceId, this.currentPageNumber);
            }
            return null;
        }
        if(!(newTuple.getField(0) instanceof RID)){
            throw new QueryExecutionException("Child Operator must return RIDs");
        }
        RID newRID = (RID) newTuple.getField(0);
        int pageNumber = newRID.getPageIndex();
        int tupleId = newRID.getTupleIndex();

        if(pageNumber != this.currentPageNumber){
            if(this.currentPageNumber != -1){
                this.bufferPool.unpinPage(tableResourceId, this.currentPageNumber);
            }
            this.currentPageNumber = pageNumber;
        }
        CacheableData cd = null;
        try {
             cd = this.bufferPool.getPageAndPin(this.tableResourceId, pageNumber);
        } catch (BufferPoolException | IOException e) {
            throw new QueryExecutionException(e);
        }
        if(!(cd instanceof TablePage)){
            throw new QueryExecutionException("Pages from Buferpool must be instances of Tablepage");
        }
        TablePage tp = (TablePage) cd;

        DataTuple orderedTuple;
        try {
            DataTuple dt = tp.getDataTuple(tupleId,this.constructBiMap(this.outputColumnMap),
                    this.getUniquelength(this.outputColumnMap));
            if(dt == null){
                throw new QueryExecutionException("Could not find RID from index in Table");
            }
            DataField[] orderedFields = new DataField[this.outputColumnMap.length];

            ArrayList<Integer> arrayList = new ArrayList<>();
            for (int num : this.outputColumnMap) {
                if(!arrayList.contains(num))
                {
                    arrayList.add(num);
                }
            }
            Collections.sort(arrayList);


            for(int fieldIdx = 0; fieldIdx < this.outputColumnMap.length; fieldIdx++){
                orderedFields[fieldIdx] = dt.getField(arrayList.indexOf(this.outputColumnMap[fieldIdx]));
            }
            orderedTuple = new DataTuple(orderedFields);
        } catch (PageTupleAccessException e) {
            throw new QueryExecutionException(e);
        }

        return orderedTuple;

    }

    @Override
    public void close() throws QueryExecutionException {

        this.child = null;
        this.bufferPool = null;
        this.outputColumnMap = null;
    }
}
