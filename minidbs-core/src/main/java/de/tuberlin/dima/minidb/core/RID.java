package de.tuberlin.dima.minidb.core;


/**
 * A immutable RowIdentifier - a physical pointer to a record in a table. The
 * RID consists of two parts: The page index and the record index.
 * The page index describes which page in a table file the tuple
 * is on. The record index describes the position of the tuple's record within
 * that page. 
 *
 * The RID is also a data type to be used as part of a tuple during regular
 * processing.
 *  
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public final class RID extends DataField
{
	/**
	 * A serial version UID to support object serialization.
	 */
	private static final long serialVersionUID = -3686062306503058240L;
	
	/**
	 * The row identifier as a single 64bit integer.
	 * The page index is in the more significant 32bits,
	 * the tuple index is in the less significant 32 bits.
	 */
	private long rid;
	
	/**
	 * Constructs a new RID for the given 64 bit identifier.
	 * The more significant 32bits are expected to hold the page
	 * index and the less significant 32 bits are expected to hold
	 * the tuple index.
	 *  
	 * @param rid The 64bit identifier.
	 */
	public RID(long rid)
	{
		if (rid < 0) {
			throw new IllegalArgumentException("RIDs mut be non-negative");
		}
		
		this.rid = rid;
	}
	
	/**
	 * Creates a new RID referencing the tuple on the given page
	 * at the given position.
	 *  
	 * @param pageIndex The index of the tuple's page.
	 * @param tupleIndex The position of the tuple in the page.
	 */
	public RID(int pageIndex, int tupleIndex)
	{
		if (pageIndex < 0 || tupleIndex < 0) {
			throw new IllegalArgumentException("Page and tuple indexes must be non-negative.");
		}
		
		this.rid = (((long) pageIndex) << 32) | (tupleIndex); 
	}
	
	/**
	 * Gets the row identifier as a single 64bit integer.
	 * The page index is in the more significant 32bits,
	 * the tuple index is in the less significant 32 bits.
	 * 
	 * @return The 64bit id.
	 */
	public long getID()
	{
		return this.rid;
	}
	
	/**
	 * Gets the page on which this referenced tuple is.
	 * 
	 * @return The tuple's page.
	 */
	public int getPageIndex ()
	{
		return (int) (this.rid >>> 32);
	}
	
	/**
	 * Gets the position of the tuple's record within its page.
	 * 
	 * @return The record position.
	 */
	public int getTupleIndex()
	{
		return (int) this.rid;
	}

	/* (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
    @Override
	public int compareTo(DataField o)
    {
		long diff =  ((RID)o).rid - this.rid;
		return (diff & 0xffffffff00000000L) == 0 ? (int) diff : (int) (diff >> 32);
    }
	
	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object other)
	{
		return other != null && other instanceof RID && ((RID) other).rid == this.rid;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode()
	{
		return ((int) this.rid) ^ ((int) (this.rid >>> 32));
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#encodeAsString()
	 */
	@Override
    public String encodeAsString()
    {
		return "(" + getPageIndex() + "." + getTupleIndex() + ")";
    }

	/* (non-Javadoc)
     * @see de.tuberlin.dima.minidb.core.DataField#getFromString(java.lang.String)
     */
    @Override
    public RID getFromString(String charEncoded, int len) throws DataFormatException
    {
    	throw new IllegalOperationException("RIDs cannot be read from strings.");
    }

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#encodeBinary(byte[], int)
	 */
	@Override
    public int encodeBinary(byte[] buffer, int offset)
    {
	    buffer[offset]     = (byte)  this.rid;
	    buffer[offset + 1] = (byte) (this.rid >>> 8);
	    buffer[offset + 2] = (byte) (this.rid >>> 16);
	    buffer[offset + 3] = (byte) (this.rid >>> 24);
	    buffer[offset + 4] = (byte) (this.rid >>> 32);
	    buffer[offset + 5] = (byte) (this.rid >>> 40);
	    buffer[offset + 6] = (byte) (this.rid >>> 48);
	    buffer[offset + 7] = (byte) (this.rid >>> 56);
	    return 8;
    }

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#getDataType()
	 */
	@Override
    public BasicType getBasicType()
    {
		return BasicType.RID;
    }

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#getFromBinary(byte[], int, int)
	 */
	@Override
	RID getFromBinary(byte[] binaryEncoded, int offs, int len)
    {
		return RID.getRidFromBinary(binaryEncoded, offs);
    }

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#getLength()
	 */
	@Override
    public int getNumberOfBytes()
    {
	    return 8;
    }

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#isNULL()
	 */
	@Override
    public boolean isNULL()
    {
		// RID is never NULL
	    return false;
    }
	
	
	/**
	 * Gets the number of bytes that a RID occupies.
	 * 
	 * @return The number of bytes that a RID occupies.
	 */
	public static int getRIDSize()
	{
		return 8;
	}
	
	/**
	 * Extract a RID from the first 8 bytes after the given offset
	 * from the given binary array.
	 * 
	 * @param binaryEncoded The binary array to extract the number from.
	 * @param offs The offset where the binary coded RID starts.
	 * @return The extracted RID.
	 * @throws ArrayIndexOutOfBoundsException Thrown, if the array has not enough bytes
	 *                                        to represent the binary encoded value.
	 */
	public static RID getRidFromBinary(byte[] binaryEncoded, int offs)
	{
		int tupleIndex = (binaryEncoded[offs]            & 0x000000ff) |
		                ((binaryEncoded[offs + 1] <<  8) & 0x0000ff00) |
		                ((binaryEncoded[offs + 2] << 16) & 0x00ff0000) |
		                ((binaryEncoded[offs + 3] << 24) & 0xff000000);

		int pageIndex = (binaryEncoded[offs + 4]        & 0x000000ff) |
		               ((binaryEncoded[offs + 5] <<  8) & 0x0000ff00) |
		               ((binaryEncoded[offs + 6] << 16) & 0x00ff0000) |
		               ((binaryEncoded[offs + 7] << 24) & 0xff000000);

		return new RID(pageIndex, tupleIndex);
	}
	
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		return "[RID: " + encodeAsString() + ']'; 
	}
}
