package de.tuberlin.dima.minidb.warm_up;

import java.util.ArrayList;
import java.util.Comparator;

public class SortImpl implements Sort{
    @Override
    public ArrayList<Comparable> sort(ArrayList<Comparable> table) {

        Comparator<Comparable> c = new Comparator<Comparable>() {

            @SuppressWarnings("unchecked")
            @Override
            public int compare(Comparable o1, Comparable o2) {
                return o1.compareTo(o2);
            }
        };
        table.sort(c);
        return table;
    }
}
