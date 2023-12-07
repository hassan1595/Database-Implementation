package de.tuberlin.dima.minidb.qexec;

import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.io.cache.PageFormatException;
import de.tuberlin.dima.minidb.io.index.BTreeIndex;
import de.tuberlin.dima.minidb.io.index.IndexResultIterator;
import de.tuberlin.dima.minidb.io.manager.BufferPoolManager;
import de.tuberlin.dima.minidb.io.tables.TableResourceManager;

import java.io.IOException;

public class IndexScanOperatorClass implements  IndexScanOperator{

    private BTreeIndex index;
    private DataField startKey;
    private DataField stopKey;
    private boolean startKeyIncluded;
    private boolean stopKeyIncluded;

    private IndexResultIterator<DataField> resIndex;

    public IndexScanOperatorClass(BTreeIndex index, DataField startKey, DataField stopKey, boolean startKeyIncluded, boolean stopKeyIncluded){

        this.index = index;
        this.startKey = startKey;
        this.stopKey = stopKey;
        this.startKeyIncluded = startKeyIncluded;
        this.stopKeyIncluded = stopKeyIncluded;
    }
    @Override
    public void open(DataTuple correlatedTuple) throws QueryExecutionException {

        try {
            this.resIndex = this.index.lookupKeys(this.startKey, this.stopKey,
                    this.startKeyIncluded, this.stopKeyIncluded);
        } catch (PageFormatException | IOException e) {
            throw new QueryExecutionException(e);
        }
    }

    @Override
    public DataTuple next() throws QueryExecutionException {


        try {
            if(this.resIndex.hasNext()){
                DataField[] df = new DataField[1];
                df[0] = this.resIndex.next();
                return new DataTuple(df);
            } else{
                return  null;
            }


        } catch (IOException | PageFormatException e) {
            throw new QueryExecutionException(e);
        }
    }

    @Override
    public void close() throws QueryExecutionException {
        this.startKey = null;
        this.stopKey = null;

    }
}
