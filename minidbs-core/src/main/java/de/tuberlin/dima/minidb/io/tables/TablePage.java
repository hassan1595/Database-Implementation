package de.tuberlin.dima.minidb.io.tables;


import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.io.cache.CacheableData;
import de.tuberlin.dima.minidb.io.cache.PageExpiredException;
import de.tuberlin.dima.minidb.io.cache.PageFormatException;
import de.tuberlin.dima.minidb.qexec.LowLevelPredicate;


/**
 * This interface defines methods to access data from a binary table page. The
 * table page consists of three sections: header, record sequence and variable-length-chunk.
 * <p>
 * The header is fixed to 32 bytes, the record sequence and the variable-length-chunk grow
 * depending on the number and length of variable-length fields in the inserted tuples.
 * <p>
 * <b>Definition of terms:</b> A tuple refers to the logical construct of a tuple, that is
 * inserted into the page or retrieved from the page. Record refers to the physical representation
 * in binary form in a page.
 * <p>
 * All methods return the tuples with their fields in the order as on the page.
 * <p>
 * Several methods contain a bitmap to describe which columns are supposed to be fetched and which
 * ones are not fetched. In such a bitmap, a <i>1</i> at position <i>n</i> (counting from the least
 * significant bit) means that the <i>n</i>'th column should be fetched. In that sense, a tuple is
 * for example constructed via a loop as in the following example:
 * 
 * <pre>
 * long bitmap = ...;
 * int numCols = ...;
 * 
 * Tuple t = new Tuple(numCols);
 * 
 * for (int i = 0; i < numCols & bitmap != 0; bitmap >>>= 1) {
 *   if (bitmap & 0x1 == 0) {
 *     continue;
 *   }
 *   
 *   t.assignColumn(i, "get-i'th-field-of-the-tuple");
 *   i++;
 * }
 * </pre>
 * 
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public interface TablePage extends CacheableData
{
	/**
	 * The magic number indicating a page containing table data.
	 */
	public static final int TABLE_DATA_PAGE_HEADER_MAGIC_NUMBER = 0xDEADBEEF;
	
	/**
	 * The number of bytes reserved for the table page header.
	 */
	public static final int TABLE_DATA_PAGE_HEADER_BYTES = 32;
	
	
	/**
	 * Gets the page number of this page, as is found in the header bytes 4 - 7.
	 * 
	 * @return The page number from the page header.
	 * @throws PageExpiredException Thrown, if the operation is performed 
	 * 								on a page that is identified to be expired. 
	 */	
	@Override
	public int getPageNumber() throws PageExpiredException;
	
	
	/**
	 * Gets how many records are currently stored on this page. This returns the total number
	 * of records, including those that are marked as deleted. The number retrieved by this
	 * function is the value from the header bytes 8 - 11.
	 * 
	 * @return The total number of records on this page.
	 * @throws PageExpiredException Thrown, if the operation is performed 
	 * 								on a page that is identified to be expired. 
	 */
	public int getNumRecordsOnPage() throws PageExpiredException;
	
	
	// ------------------------------------------------------------------------
	
	/**
	 * Inserts a tuple into the page by inserting the variable-length fields into the dedicated
	 * part of the page and inserting the record for the tuple into the record sequence.
	 * <p>
	 * If the method is not successful in inserting the tuple due to the fact that there is
	 * not enough space left, it returns false, but does not throw an exception.
	 * 
	 * @param tuple The tuple to be inserted.
	 * @return true, if the tuple was inserted, false, if the tuple was not inserted.
	 * @throws PageFormatException Thrown, if the format of the page is invalid, such as that
	 *                             current offset to the variable-length-chunk is invalid.
	 * @throws PageExpiredException Thrown, if the operation is performed 
	 * 								on a page that is identified to be expired. 
	 */
	public boolean insertTuple(DataTuple tuple) throws PageFormatException, PageExpiredException;
	
	
	/**
	 * Deletes a tuple by setting the tombstone flag to 1.
	 * 
	 * @param position The position of the tuple's record. The first record has position 0.
	 * @throws PageTupleAccessException Thrown, if the index is negative or larger than the number
	 *                                  of tuple on the page.
	 * @throws PageExpiredException Thrown, if the operation is performed 
	 * 								on a page that is identified to be expired. 
	 */
	public void deleteTuple(int position) throws PageTupleAccessException, PageExpiredException;
	
	
	/**
	 * Takes the DataTuple from the page whose record is found at the given position in the
	 * sequence of records on the page.
	 * <p>
	 * The position starts at 0, such that
	 * <code>getDataTuple(0, ...)</code> returns the tuple whose record starts directly after
	 * the page header.
	 * <p>
	 * The schema of the returned tuple is defined by the parameters <code>columnBitmap</code> and
	 * <code>numCols</code>. Only the columns for which the respective bit in <code>columnBitmap</code> is set are
	 * returned.
	 * <p>
	 * For variable-lengths fields, the method takes care of resolving the pointers in the fixed record and returns
	 * the contents of the field.
	 * <p>
	 * If the tombstone flag of the record is set, then this method returns null, but does
	 * not throw an exception.
	 * 
	 * @param position The position of the tuple's record. The first record has position 0.
	 * @param columnBitmap The bitmap describing which columns to fetch. See description of the class
	 *                     for details on how the bitmaps describe which columns to fetch.
	 * @param numCols The number of columns that should be fetched.
	 * 
	 * @return The tuple constructed from the record and its referenced variable-length fields,
	 *         or null, if the tombstone bit of the tuple is set.
	 * 
	 * @throws PageTupleAccessException Thrown, if the tuple could not be constructed (pointers invalid),
	 *                                  or the index negative or larger than the number of tuple on the page.
	 * @throws PageExpiredException Thrown, if the operation is performed 
	 * 								on a page that is identified to be expired. 
	 */
	public DataTuple getDataTuple(int position, long columnBitmap, int numCols)
	throws PageTupleAccessException, PageExpiredException;

	/**
	 * Takes the DataTuple from the page whose record is found at the given position in the
	 * sequence of records on the page.
	 * <p>
	 * The position starts at 0, such that
	 * <code>getDataTuple(0, ...)</code> returns the tuple whose record starts directly after
	 * the page header.
	 * <p>
	 * The schema of the returned tuple is defined by the parameters <code>columnBitmap</code> and
	 * <code>numCols</code>. Only the columns for which the respective bit in <code>columnBitmap</code> is set are
	 * returned.
	 * <p>
	 * For variable-lengths fields, the method takes care of resolving the pointers in the fixed record and returns
	 * the contents of the field.
	 * <p>
	 * If the tombstone flag of the record is set, then this method returns null, but does
	 * not throw an exception.
	 * 
	 * @param preds An array of predicates that the tuple must pass. The predicates are conjunctively
	 *              connected, so if any of the predicates evaluates to false, the tuple is discarded.
	 * @param position The position of the tuple's record. The first record has position 0.
	 * @param columnBitmap The bitmap describing which columns to fetch. See description of the class
	 *                     for details on how the bitmaps describe which columns to fetch.
	 * @param numCols The number of columns that should be fetched.
	 * 
	 * @return The tuple constructed from the record and its referenced variable-length fields,
	 *         or null, if the tombstone bit of the tuple is set or any predicate evaluates to false.
	 * 
	 * @throws PageTupleAccessException Thrown, if the tuple could not be constructed (pointers invalid),
	 *                                  or the index negative or larger than the number of tuple on the page.
	 * @throws PageExpiredException Thrown, if the operation is performed 
	 * 								on a page that is identified to be expired. 
	 */
	public DataTuple getDataTuple(LowLevelPredicate[] preds, int position, long columnBitmap, int numCols)
	throws PageTupleAccessException, PageExpiredException;
	
	/**
	 * Creates an iterator that iterates over all tuples contained in this page. Records whose tombstone
	 * bit is set are skipped.
	 * 
	 * @param numCols The number of columns that should be fetched.
	 * @param columnBitmap The bitmap describing which columns to fetch. See description of the class
	 *                     for details on how the bitmaps describe which columns to fetch.
	 *                     
	 * @return An iterator over the tuples represented by the records in this page.
	 * @throws PageTupleAccessException Thrown, if the iterator could not be created due to
	 *                                  invalid format.
	 * @throws PageExpiredException Thrown, if the operation is performed 
	 * 								on a page that is identified to be expired. 
	 */
	public TupleIterator getIterator(int numCols, long columnBitmap) throws PageTupleAccessException, PageExpiredException;
	
	/**
	 * Creates an iterator that iterates over all tuple contained in this page. This means
	 * that tuples, where the record has been marked as deleted (i.e. whose tombstone bit is set) 
	 * are skipped. Only tuples that pass all predicates are returned.
	 * 
	 * @param preds An array of predicates that the tuple must pass. The predicates are conjunctively
	 *              connected, so if any of the predicates evaluates to false, the tuple is discarded.
	 * @param numCols The number of columns that should be fetched.
	 * @param columnBitmap The bitmap describing which columns to fetch. See description of the class
	 *                     for details on how the bitmaps describe which columns to fetch.
	 * 
	 * @return An iterator over the tuples represented by the records in this page.
	 * @throws PageTupleAccessException Thrown, if the iterator could not be created due to
	 *                                  invalid format.
	 * @throws PageExpiredException Thrown, if the operation is performed 
	 * 								on a page that is identified to be expired. 
	 */
	public TupleIterator getIterator(LowLevelPredicate[] preds, int numCols, long columnBitmap)
	throws PageTupleAccessException, PageExpiredException;
	
	/**
	 * Creates an iterator as the function <code>getIterator()</code> does. In addition to the tuples,
	 * this iterator the RID that referenced the tuple's record.
	 * 
	 * @return An iterator over the tuple sequence, where the tuples contain in addition the RID.
	 * @throws PageTupleAccessException Thrown, if the iterator could not be created due to
	 *                                  invalid format.
	 * @throws PageExpiredException Thrown, if the operation is performed 
	 * 								on a page that is identified to be expired. 
	 */
	public TupleRIDIterator getIteratorWithRID() throws PageTupleAccessException, PageExpiredException;
	

}
