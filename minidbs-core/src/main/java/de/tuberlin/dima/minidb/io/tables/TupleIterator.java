package de.tuberlin.dima.minidb.io.tables;


import de.tuberlin.dima.minidb.core.DataTuple;


/**
 * Interface describing an iterator that allows to iterate over
 * a sequence of tuples that are contained in a page.
 * 
 * For further information, see the specification of java.util.Iterator.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public interface TupleIterator
{

	/**
	 * This method checks, if further tuples are available from this iterator.
	 * 
	 * @return true, if there are more tuples available, false if not.
	 * @throws PageTupleAccessException Thrown, if the iterator failed to 
	 *                                  check for further tuples.
	 */
	public boolean hasNext() throws PageTupleAccessException;
	
	/**
	 * This gets the next tuple from the iterator, moving the iterator forward.
	 * This method should succeed, if a prior call to hasNext() returned true.
	 * 
	 * @return The next tuple in the sequence. 
	 * @throws PageTupleAccessException Thrown, if the iterator could not create a
	 *                                  tuple from the page records, or if there are no
	 *                                  more tuples in this iterator's sequence.
	 */
	public DataTuple next() throws PageTupleAccessException;
}
