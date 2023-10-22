package de.tuberlin.dima.minidb.io.index;


import de.tuberlin.dima.minidb.catalogue.IndexSchema;
import de.tuberlin.dima.minidb.core.IntField;
import de.tuberlin.dima.minidb.io.cache.PageFormatException;


/**
 * Factory to create index pages.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public abstract class IndexPageFactory
{
	/**
	 * Creates an index page for the given schema and the contents of the buffer.
	 * The type of the page will be depending on the contents of the version field
	 * in the header contained in the binary buffer.
	 *   
	 * @param schema The schema for the index page.
	 * @param buffer The buffer containing the binary data from the page.
	 * @return A BTreeIndexPage wrapping the binary contents in the buffer.
	 * @throws PageFormatException Thrown, if the buffer does not contain a valid BTreeIndexPage.
	 */
	public static BTreeIndexPage createPage(IndexSchema schema, byte[] buffer)
	throws PageFormatException
	{
		// check if the buffer is large enough
		if (buffer.length < schema.getPageSize().getNumberOfBytes()) {
			throw new PageFormatException("The given buffer is too small to hold a page.");
		}
		
		// get the generic fields
		int magic = IntField.getIntFromBinary(buffer, 0);
		int pageNumber = IntField.getIntFromBinary(buffer, 4);
		int typeVersion = IntField.getIntFromBinary(buffer, 8);
		
		// verify header
		if (magic != BTreeIndexPage.INDEX_PAGE_MAGIC_NUMBER) {
			throw new PageFormatException
				("The given page is no valid index page. Magic number incorrect.");
		}
		if (pageNumber < 1) {
			throw new PageFormatException("Invalid page number in page header: " + pageNumber);
		}
		
		// instantiate depending on the version and type field
		if (typeVersion == BTreeInnerNodePage.HEADER_TYPE_VALUE) {
			return new BTreeInnerNodePage(schema, buffer);
		}
		else if (typeVersion == BTreeLeafPage.HEADER_TYPE_VALUE) {
			return new BTreeLeafPage(schema, buffer);
		}
		else {
			throw new PageFormatException("Unknown type indicator: " + typeVersion);
		}
	}
	
	
	/**
	 * Initializes an index page for the given schema and the contents of the buffer.
	 * The type of the page will be depending on the contents of the version field
	 * in the header contained in the binary buffer.
	 *   
	 * @param schema The schema for the index page.
	 * @param buffer The buffer containing the binary data from the page.
	 * @param newPageNumber The new page number for the index page to be initialized.
	 * @param leafPage True, if the leaf page to be initializes is a leaf page, false if it is a
	 *                 page for an inner node.
	 * @return The newly initialized BTreeIndexPage wrapping the binary contents in the buffer.
	 * @throws PageFormatException Thrown, if the buffer does not contain a valid BTreeIndexPage.
	 */
	public static BTreeIndexPage initIndexPage(IndexSchema schema, byte[] buffer,
	                                 int newPageNumber, boolean leafPage)
	throws PageFormatException
	{
		// check if the buffer is large enough
		if (buffer.length < schema.getPageSize().getNumberOfBytes()) {
			throw new PageFormatException("The given buffer is too small to hold a page.");
		}
		if (newPageNumber < 1) {
			throw new IllegalArgumentException("Page number must be greater than 0");
		}
		
		// encode generic fields
		IntField.encodeIntAsBinary(BTreeIndexPage.INDEX_PAGE_MAGIC_NUMBER, buffer, 0);
		IntField.encodeIntAsBinary(newPageNumber, buffer, 4);
		
		// encode fields depending on page type
		if (leafPage) {
			// code version
			IntField.encodeIntAsBinary(BTreeLeafPage.HEADER_TYPE_VALUE, buffer, 8);
			// code number of entries
			IntField.encodeIntAsBinary(0, buffer, 12);
			// code linked page
			IntField.encodeIntAsBinary(-1, buffer, 16);
			// code flags
			IntField.encodeIntAsBinary(0, buffer, 20);
			
			return new BTreeLeafPage(schema, buffer);
		}
		else {
			// code version
			IntField.encodeIntAsBinary(BTreeInnerNodePage.HEADER_TYPE_VALUE, buffer, 8);
			// code number of entries
			IntField.encodeIntAsBinary(0, buffer, 12);
			
			return new BTreeInnerNodePage(schema, buffer);
		}
		
	}
}
