package de.tuberlin.dima.minidb.qexec.heap;


import java.io.IOException;

import de.tuberlin.dima.minidb.core.DataTuple;


/**
 * Interface describing an iterator that allows to iterate over
 * a sequence of tuples that are provided by the query heap from an external list.
 * 
 * For further information, see the specification of java.util.Iterator.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public interface ExternalTupleSequenceIterator
{

	/**
	 * This method checks, if further tuples are available from this iterator.
	 * 
	 * @return true, if there are more tuples available, false if not.
	 * @throws QueryHeapException Thrown, if the query heap does not support
	 *                           the iterator any more.
	 */
	public boolean hasNext() throws QueryHeapException;
	
	/**
	 * This gets the next tuple from the iterator, moving the iterator forward.
	 * This method should succeed, if a prior call to hasNext() returned true.
	 * 
	 * @return The next tuple in the sequence.
	 * @throws QueryHeapException Thrown, if the query heap does not support
	 *                           the iterator any more.
	 * @throws IOException Thrown, if the iterator could not produce a
	 *                                  tuple for some I/O related reason.
	 */
	public DataTuple next() throws QueryHeapException, IOException;
}
