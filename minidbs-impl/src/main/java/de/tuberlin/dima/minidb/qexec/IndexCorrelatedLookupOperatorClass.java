package de.tuberlin.dima.minidb.qexec;

import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.core.RID;
import de.tuberlin.dima.minidb.io.cache.PageFormatException;
import de.tuberlin.dima.minidb.io.index.BTreeIndex;
import de.tuberlin.dima.minidb.io.index.IndexResultIterator;

import java.io.IOException;

public class IndexCorrelatedLookupOperatorClass implements  IndexCorrelatedLookupOperator{

    private BTreeIndex index;
    private int correlatedColumnIndex;
    private IndexResultIterator<RID> resIndex;
    boolean opened;

    public IndexCorrelatedLookupOperatorClass(BTreeIndex index, int correlatedColumnIndex){

        this.index = index;
        this.correlatedColumnIndex = correlatedColumnIndex;
        this.opened = false;
    }
    @Override
    public void open(DataTuple correlatedTuple) throws QueryExecutionException {

        try {
            this.resIndex = this.index.lookupRids(correlatedTuple.getField(correlatedColumnIndex));
        } catch (PageFormatException | IOException e) {
            throw new QueryExecutionException(e);
        }

        this.opened = true;
    }

    @Override
    public DataTuple next() throws QueryExecutionException {
        if(!opened){
            throw new QueryExecutionException("Operator is not yet opened");
        }
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

    }
}
