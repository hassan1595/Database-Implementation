package de.tuberlin.dima.minidb.io.index;


import de.tuberlin.dima.minidb.io.cache.CacheableData;


/**
 * Super interface to be implemented by classes that represent pages of a B-Tree index.
 * 
 * The interface implies the following generic header:
 * <ul>
 *   <li>Bytes 0 - 3 are an INT (little endian) holding the magic number for index pages</li>
 *   <li>Bytes 4 - 7 are an INT (little endian) holding the page number</li>
 *   <li>Bytes 8 - 11 are an INT (little endian) holding the type information that identifies
 *       the page for example as a page for an inner node or for a leaf node.</li>
 * </ul> 
 * 
 * The remainder of the header is up to the interpretation of the more specific page type.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public interface BTreeIndexPage extends CacheableData
{
	/**
	 * The number of bytes occupied by the header in each index page.
	 */
	public static final int INDEX_PAGE_HEADER_SIZE = 32;
	
	/**
	 * The magic number that identifies a page as an index page.
	 */
	public static final int INDEX_PAGE_MAGIC_NUMBER = 0xFEEDFACE;
}
