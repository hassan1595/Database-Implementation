package de.tuberlin.dima.minidb.test.warm_up;

import de.tuberlin.dima.minidb.api.AbstractExtensionFactory;
import de.tuberlin.dima.minidb.warm_up.Sort;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

import static org.junit.Assert.assertTrue;

/**
 * @author vianney created on 09.08.16.
 */
public class TestWarmUpStudents {

    /**
     * Fixed seed to make tests reproducible. Teacher test uses a different seed so please try your solution with another seed to check for remaining errors.
     */
    private static final long SEED = 342981672376724L;

    /**
     * Size of the list to sort
     */
    private static final int ARRAY_SIZE = 1000000;

    /**
     * The random number generator used to generate data.
     */
    private static Random random;

    @Before
    public void setUp() throws Exception {
        // load the custom code
        AbstractExtensionFactory.initializeDefault();
        random = new Random(SEED);

    }

    @After
    public void tearDown() throws Exception {

    }

    /**
     * Tests whether sorting works with doubles
     */
    @Test
    public void testSortDouble() {
        ArrayList<Double> table = new ArrayList<>();
        for (int i = 0; i < ARRAY_SIZE; i++) {
            table.add(random.nextDouble() * random.nextInt());
        }
        testSort(table);
    }

    /**
     * Tests whether sorting works with integers
     */
    @Test
    public void testSortInt() {
        ArrayList<Integer> table = new ArrayList<>();
        for (int i = 0; i < ARRAY_SIZE; i++) {
            table.add(random.nextInt());
        }
        testSort(table);
    }

    /**
     * Helper function, tests whether sorting works
     *
     * @param table
     */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void testSort(ArrayList<? extends Comparable> table) {
        Sort sort;
        sort = AbstractExtensionFactory.getExtensionFactory().createSortOperator();
        ArrayList sortedTable = sort.sort((ArrayList<Comparable>) table);
        Iterator<? extends Comparable> iter = sortedTable.iterator();
        Comparable lastElem, elem = null;
        assertTrue(iter != null);
        assertTrue("There is no element in the sorted list", iter.hasNext());
        elem = iter.next();
        lastElem = elem;
        assertTrue("List contains null values", lastElem != null);
        while (iter.hasNext()){
            elem = iter.next();
            assertTrue("List contains null values", elem != null);
            assertTrue("List is not sorted", elem.compareTo(lastElem) >= 0);
            lastElem = elem;
        }
    }
}
