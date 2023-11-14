package de.tuberlin.dima.minidb.io.tables;

import de.tuberlin.dima.minidb.core.DataTuple;

import java.util.ArrayList;

public class TupIterator implements TupleIterator {

    private final ArrayList<DataTuple> tuples;
    private int i = 0;

    public TupIterator(ArrayList<DataTuple> dts) {
        this.tuples = dts;
    }

    @Override
    public boolean hasNext() throws PageTupleAccessException {
        return (i < this.tuples.size());
    }

    @Override
    public DataTuple next() throws PageTupleAccessException {
        DataTuple res = this.tuples.get(i);
        this.i += 1;
        return res;
    }
}