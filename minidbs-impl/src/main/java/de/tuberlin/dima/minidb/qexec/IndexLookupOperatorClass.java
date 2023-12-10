package de.tuberlin.dima.minidb.qexec;

import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.core.RID;
import de.tuberlin.dima.minidb.io.cache.PageFormatException;
import de.tuberlin.dima.minidb.io.index.BTreeIndex;
import de.tuberlin.dima.minidb.io.index.IndexResultIterator;

import java.io.IOException;

public class IndexLookupOperatorClass implements IndexLookupOperator {

    private  BTreeIndex index;
    private DataField equalityLiteral;
    private IndexResultIterator<RID> resIndex;
    private DataField lowerBound;
    private boolean lowerIncluded;

    private DataField upperBound;

    private boolean upperIncluded;

    private  boolean between;
    private boolean opened;
    public IndexLookupOperatorClass(BTreeIndex index, DataField equalityLiteral){
        this.index = index;
        this.equalityLiteral = equalityLiteral;
        this.lowerBound = null;
        this.lowerIncluded = false;
        this.upperBound = null;
        this.upperIncluded = false;
        this.between = false;


    }

    public IndexLookupOperatorClass(BTreeIndex index, DataField lowerBound, boolean lowerIncluded, DataField upperBound,
                                    boolean upperIncluded){

        this.index= index;
        this.lowerBound = lowerBound;
        this.lowerIncluded = lowerIncluded;
        this.upperBound = upperBound;
        this.upperIncluded = upperIncluded;
        this.between = true;
        this.opened = false;
    }
    @Override
    public void open(DataTuple correlatedTuple) throws QueryExecutionException {
        try {
            if(between){
                this.resIndex = this.index.lookupRids(this.lowerBound, this.upperBound,this.lowerIncluded, this.upperIncluded);
            }
            else{
                this.resIndex = this.index.lookupRids(this.equalityLiteral);
            }
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
        this.index = null;
        this.resIndex = null;
        this.equalityLiteral = null;

    }
}
