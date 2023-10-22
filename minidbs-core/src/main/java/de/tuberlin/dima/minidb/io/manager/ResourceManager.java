package de.tuberlin.dima.minidb.io.manager;


import java.io.IOException;

import de.tuberlin.dima.minidb.io.cache.CacheableData;
import de.tuberlin.dima.minidb.io.cache.PageFormatException;
import de.tuberlin.dima.minidb.io.cache.PageSize;


/**
 * This base abstract class for all classes that give access to physical
 * resources and allow reading and writing of pages.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public abstract class ResourceManager
{
	/**
	 * Gets the size of the pages that are used by this resource.
	 * 
	 * @return The page size in bytes.
	 */
	public abstract PageSize getPageSize();

	
	/**
	 * Truncates the resource. After this operation completes, the physical resource has no
	 * more user data, only meta data (header, etc.).
	 * 
	 * @throws IOException Thrown, if the physical truncation fails.
	 */
	public abstract void truncate() throws IOException;
	
	/**
	 * Closes the resource represented by this resource manager. This operation ensures
	 * a consistent release of all system resources (file handles and locks) and.
	 * 
	 * @throws IOException Thrown, if the closing process fails due to an I/O problem.
	 */
	public abstract void closeResource() throws IOException;
	
	/**
	 * Reads the page with the given number into the given buffer.
	 * This method directly initiates an I/O operation and should only be called by the I/O threads.
	 * 
	 * @param buffer The byte buffer to read the page into. Must be at least as large as the page size.
	 * @param pageNumber The number of the page to load.
	 * @return The page object wrapping the page data in the byte buffer. The actual type of
	 *         that wrapping page depends on the type of the resource.
	 * @throws IOException Thrown, if the page could not be read due to an I/O error.
	 */
	public abstract CacheableData readPageFromResource(byte[] buffer, int pageNumber)
	throws IOException;
	
	/**
	 * Reads a sequence of pages starting with the one with the given number into the given buffers.
	 * This method directly initiates an I/O operation and should only be called by the I/O threads.
	 * 
	 * @param buffers The byte buffers to read the page into. Must be at least as large as the page size.
	 * @param firstPageNumber The number of the first page to load.
	 * @return The page object wrapping the page data in the byte buffer. The actual type of
	 *         that wrapping page depends on the type of the resource.
	 * @throws IOException Thrown, if some page could not be read due to an I/O error.
	 */
	public abstract CacheableData[] readPagesFromResource(byte[][] buffers, int firstPageNumber)
	throws IOException;
	
	/**
	 * Write the page data in the given byte buffer to the resource's page with the given
	 * number. The wrapping page object is used to determine additional properties, such
	 * as whether the page is empty.
	 * This method directly initiates an I/O operation and should only be called by the I/O threads.
	 * 
	 * @param buffer The byte buffer containing the binary page data. Must be at least as large as the page size.
	 * @param wrapper The page object wrapping the page data in the byte buffer.
	 * @throws IOException Thrown, if the page could not be written due to an I/O error.
	 */
	public abstract void writePageToResource(byte[] buffer, CacheableData wrapper)
	throws IOException;
	
	/**
	 * Write the page data in the given byte buffers to the resource's with the given
	 * number. The wrapping page object is used to determine additional properties, such
	 * as whether the page is empty.
	 * This method directly initiates an I/O operation and should only be called by the I/O threads.
	 * 
	 * @param buffers The bytes buffer containing the binary page data. Must be at least as large as the page size.
	 * @param wrappers The page objects wrapping the page data in the byte buffers.
	 * @throws IOException Thrown, if the page could not be written due to an I/O error.
	 */
	public abstract void writePagesToResource(byte[][] buffers, CacheableData[] wrappers)
	throws IOException;
	
	/**
	 * Initializes the given buffer to a new page of the type of the resource that is
	 * represented by this resource manager. This method assigns a page number to the newly
	 * created page. The resource manager may choose to directly write the new page to the
	 * resource or not.
	 * 
	 * @param ioBuffer The buffer to initialize the page into.
	 * @return The page object wrapping the page represented by the binary data initialized
	 *         into the buffer. 
	 * @throws IOException Thrown, if the resource manager decides to write the resource
	 *                     to the resource and that operation causes an error.
	 * @throws PageFormatException Thrown, if the page initialization failed.
	 */
	public abstract CacheableData reserveNewPage(byte[] ioBuffer)
	throws IOException, PageFormatException;
	
	/**
	 * Initializes the given buffer to a new page of the type of the resource that is
	 * represented by this resource manager. If this resource has multiple ways of initializing a
	 * page (for example an index, which can initialize leaf pages or inner-node pages), the type
	 * parameter specifies which kind of page to initialize.
	 * <p>
	 * This method assigns a page number to the newly created page. The resource manager
	 * may choose to directly write the new page to the resource or not.
	 * 
	 * @param ioBuffer The buffer to initialize the page into.
	 * @param type Optional parameter. May be used to specify which type page is to be initialized.
	 * @return The page object wrapping the page represented by the binary data initialized
	 *         into the buffer. 
	 * @throws IOException Thrown, if the resource manager decides to write the resource
	 *                     to the resource and that operation causes an error.
	 * @throws PageFormatException Thrown, if the page initialization failed.
	 */
	public abstract CacheableData reserveNewPage(byte[] ioBuffer, Enum<?> type)
	throws IOException, PageFormatException;
}
