package de.tuberlin.dima.minidb.qexec;

import de.tuberlin.dima.minidb.api.AbstractExtensionFactory;
import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.io.cache.CacheableData;
import de.tuberlin.dima.minidb.io.cache.PageFormatException;
import de.tuberlin.dima.minidb.io.manager.BufferPoolException;
import de.tuberlin.dima.minidb.io.manager.BufferPoolManager;
import de.tuberlin.dima.minidb.io.tables.PageTupleAccessException;
import de.tuberlin.dima.minidb.io.tables.TablePage;
import de.tuberlin.dima.minidb.io.tables.TableResourceManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.fail;

public class TableScanOperatorClass implements TableScanOperator{
    private BufferPoolManager bufferPool;
    private TableResourceManager tableManager;
    private int resourceId;
    int[] producedColumnIndexes;
    LowLevelPredicate[] predicate;
    int prefetchWindowLength;
    int currentPage;
    int currentTuple;


    public TableScanOperatorClass(BufferPoolManager bufferPool, TableResourceManager tableManager, int resourceId,
    int[] producedColumnIndexes, LowLevelPredicate[] predicate, int prefetchWindowLength){

        this.bufferPool = bufferPool;
        this.tableManager = tableManager;
        this.resourceId = resourceId;
        this.producedColumnIndexes = producedColumnIndexes;
        this.predicate = predicate;
        this.prefetchWindowLength = prefetchWindowLength;
        this.currentPage = this.tableManager.getFirstDataPageNumber();
        this.currentTuple = 0;

    }

    @Override
    public void open(DataTuple correlatedTuple) throws QueryExecutionException {


        try {
            int firstPage = this.tableManager.getFirstDataPageNumber();
            int lastPage = Math.min(this.tableManager.getFirstDataPageNumber()
                    + this.prefetchWindowLength, this.tableManager.getLastDataPageNumber());
            this.bufferPool.prefetchPages(this.resourceId, firstPage,
                    lastPage);
        } catch (BufferPoolException e) {
            throw new QueryExecutionException(e);
        }

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






        int lastPageNumber = Math.max(this.tableManager.getFirstDataPageNumber(), this.tableManager.getLastDataPageNumber());
        if(currentPage > lastPageNumber){
            return null;
        }

        try {
        while(this.currentPage <= lastPageNumber)
            {
                CacheableData cd = this.bufferPool.getPageAndPin(this.resourceId, currentPage);
                this.bufferPool.unpinPage(this.resourceId, currentPage);;
                TablePage tp = AbstractExtensionFactory.getExtensionFactory().createTablePage(this.tableManager.getSchema(), cd.getBuffer());
                while(this.currentTuple < tp.getNumRecordsOnPage()){
                    DataTuple dt = tp.getDataTuple(this.predicate, this.currentTuple,
                            this.constructBiMap(this.producedColumnIndexes),
                            this.getUniquelength(this.producedColumnIndexes));
                    if(dt != null){

                        DataField[] orderedFields = new DataField[this.producedColumnIndexes.length];

                        ArrayList<Integer> arrayList = new ArrayList<>();
                        for (int num : this.producedColumnIndexes) {
                            if(!arrayList.contains(num))
                            {
                                arrayList.add(num);
                            }
                        }
                        Collections.sort(arrayList);


                        for(int fieldIdx = 0; fieldIdx < this.producedColumnIndexes.length; fieldIdx++){
                            orderedFields[fieldIdx] = dt.getField(arrayList.indexOf(this.producedColumnIndexes[fieldIdx]));
                        }

                        this.currentTuple++;
                        return new DataTuple(orderedFields);
                    }
                    else{
                        this.currentTuple++;
                    }
                }
                this.currentPage++;
                this.currentTuple = 0;
            }

        } catch (BufferPoolException | IOException  | PageTupleAccessException e) {
            throw new QueryExecutionException(e);
        } catch (PageFormatException e) {
            throw new RuntimeException(e);
        }

        return null;
    }

    @Override
    public void close() throws QueryExecutionException {

    }
}
