package de.tuberlin.dima.minidb.io.index;


import java.io.IOException;

import de.tuberlin.dima.minidb.catalogue.IndexSchema;
import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DuplicateException;
import de.tuberlin.dima.minidb.core.RID;
import de.tuberlin.dima.minidb.io.cache.PageFormatException;


/**
 * Interface defining the logic to search and insert data in a B-Tree index.
 * <p>
 * The nodes of the BTreeIndex are pages of type BTreeInnerNodePage and BTreeLeafPage.
 * The class is constructed with an instance of IndexSchema (from which information like
 * the page number of the root page or the first leaf page can be obtained) and 
 * an instance of BufferPoolManager that is used to request pages that are needed during the
 * traversal.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public interface BTreeIndex
{
	/**
	 * Gets the schema of the index represented by this instance.
	 * 
	 * @return The schema of the index.
	 */
	public IndexSchema getIndexSchema();	
	
	
	/**
	 * Gets all RIDs for the given key. If the key is not found, then the returned iterator will
	 * not return any element (the first call to hasNext() is false).
	 * <p>
	 * This method should in general not get all RIDs for that key from the index at once, but only
	 * some. If the sequence of RIDs for that key spans multiple pages, these pages should be loaded gradually
	 * and the RIDs should be extracted when needed. It makes sense to always extract all RIDs from one page
	 * in one step, in order to be able to unpin that page again. 
	 * <p>
	 * Consider an example, where the key has many RIDs, spanning three pages. The iterator should load those
	 * from the first page first. When they are all returned, it loads the next page, extracting all RIDs there,
	 * returning them one after the other, and so on. It makes sense to issue prefetch requests for the next leaf
	 * pages ahead. 
	 * 
	 * @param key The key to get the RIDs for.
	 * @return An Iterator over of all RIDs for key.
	 * @throws PageFormatException Thrown if during processing a page's layout was found to be
	 *                             found to be corrupted.
	 * @throws IndexFormatCorruptException Throws, if the evaluation failed because condition
	 *                                     of the BTree were found to be invalid.
	 * @throws IOException Thrown, if a page could not be loaded.
	 */
	public IndexResultIterator<RID> lookupRids(DataField key)
	throws PageFormatException, IndexFormatCorruptException, IOException;
	
	
	/**
	 * Gets all RIDs in a given key-range. The rage is defined by the start key <code>l</code> (lower bound) 
	 * and the stop key <code>u</code> (upper bound), where both <code>l</code> and <code>u</code> can be
	 * optionally included or excluded from the interval, e.g. [l, u) or [l, u].
	 * <p>
	 * This method should obey the same on-demand-loading semantics as the {@link #lookupRids(DataField)} method. I.e. it
	 * should NOT first retrieve all RIDs and then return an iterator over an internally kept list.
	 * 
	 * @param startKey The lower boundary of the requested interval.
	 * @param stopKey The upper boundary of the requested interval.
	 * @param startKeyIncluded A flag indicating whether the lower boundary is inclusive. True indicates an inclusive boundary. 
	 * @param stopKeyIncluded A flag indicating whether the upper boundary is inclusive. True indicates an inclusive boundary.
	 * @return An Iterator over of all RIDs for the given key range.
	 * @throws PageFormatException Thrown if during processing a page's layout was found to be
	 *                             found to be corrupted.
	 * @throws IndexFormatCorruptException Throws, if the evaluation failed because condition
	 *                                     of the BTree were found to be invalid.
	 * @throws IOException Thrown, if a page could not be loaded.
	 */
	public IndexResultIterator<RID> lookupRids(DataField startKey, DataField stopKey, boolean startKeyIncluded, boolean stopKeyIncluded)
	throws PageFormatException, IndexFormatCorruptException, IOException;
	
	
	/**
	 * Gets all Keys that are contained in the given key-range. The rage is defined by the start key <code>l</code> (lower bound) 
	 * and the stop key <code>u</code> (upper bound), where both <code>l</code> and <code>u</code> can be
	 * optionally included or excluded from the interval, e.g. [l, u) or [l, u].
	 * <p>
	 * This method should obey the same on-demand-loading semantics as the {@link #lookupRids(DataField)} method. I.e. it
	 * should NOT first retrieve all keys and then return an iterator over an internally kept list.
	 * 
	 * @param startKey The lower boundary of the requested interval.
	 * @param stopKey The upper boundary of the requested interval.
	 * @param startKeyIncluded A flag indicating whether the lower boundary is inclusive. True indicates an inclusive boundary. 
	 * @param stopKeyIncluded A flag indicating whether the upper boundary is inclusive. True indicates an inclusive boundary.
	 * @return An Iterator over of all keys for the given key range.
	 * @throws PageFormatException Thrown if during processing a page's layout was found to be
	 *                             found to be corrupted.
	 * @throws IndexFormatCorruptException Throws, if the evaluation failed because condition
	 *                                     of the BTree were found to be invalid.
	 * @throws IOException Thrown, if a page could not be loaded.
	 */
	public IndexResultIterator<DataField> lookupKeys(DataField startKey, DataField stopKey, boolean startKeyIncluded, boolean stopKeyIncluded)
	throws PageFormatException, IndexFormatCorruptException, IOException; 
	
	
	/**
	 * Inserts a pair of (key/RID) into the index. For unique indexes, this method must throw
	 * a DuplicateException, if the key is already contained.
	 * <p>
	 * If the page number of the root node or the first leaf node changes during the operation,
	 * then this method must notify the schema to reflect this change.
	 * 
	 * @param key The key of the pair to be inserted.
	 * @param rid The RID of the pair to be inserted.
	 * @throws PageFormatException Thrown if during processing a page's layout was found to be
	 *                             found to be corrupted.
	 * @throws IndexFormatCorruptException Throws, if the evaluation failed because condition
	 *                                     of the BTree were found to be invalid.
	 * @throws DuplicateException Thrown, if the key is already contained and the index is defined to be unique.
	 * @throws IOException Thrown, if a page could not be read or written.
	 */
	public void insertEntry(DataField key, RID rid)
	throws PageFormatException, IndexFormatCorruptException, DuplicateException, IOException;
	
}
