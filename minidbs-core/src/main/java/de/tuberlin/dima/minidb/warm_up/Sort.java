package de.tuberlin.dima.minidb.warm_up;

import java.util.ArrayList;

/**
 * @author vianney created on 09.08.16.
 */
public interface Sort {
    /**
     * This method sorts the input table according to the comparison rule of the Comparable type
     */
    @SuppressWarnings("rawtypes")
	public ArrayList<Comparable> sort(ArrayList<Comparable> table);
}
