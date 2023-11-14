package de.tuberlin.dima.minidb.io.tables;

import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.core.RID;
import de.tuberlin.dima.minidb.util.Pair;

import java.util.ArrayList;

public class RidIterator implements TupleRIDIterator {

    private final ArrayList<Pair<DataTuple, RID>> pairs;
    private int current = 0;

    public RidIterator(ArrayList<Pair<DataTuple, RID>> pairs) {
        this.pairs = pairs;
    }

    @Override
    public boolean hasNext() throws PageTupleAccessException {
        return (current < this.pairs.size());
    }

    @Override
    public Pair<DataTuple, RID> next() throws PageTupleAccessException {
        if (this.current == this.pairs.size()) {
            throw new PageTupleAccessException(this.current);
        }
        Pair<DataTuple, RID> pair = this.pairs.get(current);
        this.current += 1;
        return pair;
    }
}