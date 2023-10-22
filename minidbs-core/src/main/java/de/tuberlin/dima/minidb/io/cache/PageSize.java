package de.tuberlin.dima.minidb.io.cache;


/**
 * An enumeration type representing the various valid page sizes.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public enum PageSize
{
	/**
	 * Enum type for the page size of 4096 bytes.
	 */
	SIZE_4096(4096),

	/**
	 * Enum type for the page size of 8192 bytes.
	 */
	SIZE_8192(8192),
	
	/**
	 * Enum type for the page size of 16384 bytes.
	 */
	SIZE_16384(16384),
	
	/**
	 * Enum type for the page size of 65536 bytes.
	 */
	SIZE_65536(65536);
	
	// ------------------------------------------------------------------------
	
	/**
	 * The number of bytes in this page size.
	 */
	private final int bytes;
	
	/**
	 * Private constructor to instantiate enum elements.
	 * 
	 * @param bytes The number of bytes (= size of the page) represented
	 *              by this page size elements.
	 */
	PageSize(int bytes)
	{
		this.bytes = bytes;
	}
	
	/**
	 * Gets the number of bytes in this page size.
	 * 
	 * @return The number of bytes in this page size.
	 */
	public int getNumberOfBytes()
	{
		return this.bytes;
	}

	// ------------------------------------------------------------------------
	
	/**
	 * Attempts to find a page size enumeration element representing a page
	 * size with the given number of bytes.
	 * 
	 * @param bytes The number of bytes to find a page size element for.
	 * @return The page size element that represents a page with a size of the
	 *         given byte count.
	 * @throws UnsupportedPageSizeException If the system does not support pages that
	 *                                      have a size of the given byte count.
	 */
	public static PageSize getPageSize(int bytes)
	throws UnsupportedPageSizeException
	{
		for (int i = 0; i < PageSize.values().length; i++)
		{
			PageSize s = PageSize.values()[i];
			if (s.getNumberOfBytes() == bytes) {
				return s;
			}
		}
		throw new UnsupportedPageSizeException(bytes);
	}
	
	/**
	 * Gets the systems default page size, which is 4096 bytes.
	 * 
	 * @return The default page size of 4096 bytes.
	 */
	public static PageSize getDefaultPageSize()
	{
		return PageSize.SIZE_4096;
	}

}
