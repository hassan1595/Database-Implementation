package de.tuberlin.dima.minidb.io.index;

import java.io.IOException;

import de.tuberlin.dima.minidb.io.cache.PageFormatException;


/**
 * Interface describing an iterator that is returned to iterate over the result of an index operation.
 * This iterator allows to throw index-related exceptions.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 * 
 * @param <E> The type of element returned by this iterator. Examples are RID or the generic DataField.
 */
public interface IndexResultIterator<E>
{

	/**
	 * This method checks, if further elements are available from this iterator.
	 * 
	 * @return true, if there are more elements available, false if not.
	 * @throws IOException Thrown, if the method fails due to an I/O problem.
	 * @throws IndexFormatCorruptException Thrown, if the method fails because the index is in an inconsistent state.
	 * @throws PageFormatException Thrown, if a corrupt page prevents execution of this method.
	 */
	public boolean hasNext() throws IOException, IndexFormatCorruptException, PageFormatException;
	
	/**
	 * This gets the next element from the iterator, moving the iterator forward.
	 * This method should succeed, if a prior call to hasNext() returned true.
	 * 
	 * @return The next element in the sequence. 
	 * @throws IOException Thrown, if the method fails due to an I/O problem.
	 * @throws IndexFormatCorruptException Thrown, if the method fails because the index is in an inconsistent state.
	 * @throws PageFormatException Thrown, if a corrupt page prevents execution of this method.
	 */
	public E next() throws IOException, IndexFormatCorruptException, PageFormatException;
}
