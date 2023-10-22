package de.tuberlin.dima.minidb.core;



/**
 * A specific data field (part of a tuple) holding a varchar. This data type does actually not
 * hold and runtime information about the maximal length of the character array, and it consequently
 * does no checking. Since objects instantiated from this class occur frequently and are used in performance
 * critical loops, this object carries as little space overhead as possible.
 * 
 * The checking that the length of the string matches is within bounds and to truncate it
 * is task of the calling component that verifies the schema.  
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public final class VarcharField extends DataField
{
	/**
	 * A serial version UID to support object serialization.
	 */
	private static final long serialVersionUID = -3789209507430256721L;

	/**
	 * The singleton NULL value.
	 */
	static final VarcharField NULL_VALUE = new VarcharField();
	
	/**
	 * The character data for this varchar. If this field is null, we have a NULL value.
	 */
	private String charData;
	
	
	/**
	 * Creates a new varchar field with the given string as character data.
	 * NOTE: No checking of the length happens here.
	 * 
	 * @param value The character data as a string.
	 */
	public VarcharField(String value)
	{
		this.charData = value.trim();
	}
	
	/**
	 * Package level constructor for default instances.
	 */
	VarcharField()
	{
		this.charData = null;
	}
	
	/**
	 * Gets the character data in this varchar.
	 * 
	 * @return The character data as a string.
	 */
	public String getValue()
	{
		return this.charData;
	}
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#getLength()
	 */
	@Override
	public int getNumberOfBytes()
	{
		return this.charData == null ? 0 : this.charData.length() * 2;
	}

	@Override
    public boolean isNULL()
    {
    	return this.charData == null;
    }

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#getDataType()
	 */
	@Override
    public BasicType getBasicType()
    {
		return BasicType.VAR_CHAR;
    }
	
	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object o)
	{
		return (o != null && o instanceof VarcharField && 
				( (((VarcharField) o).charData == null && this.charData == null) ||
				  (((VarcharField) o).charData != null && ((VarcharField) o).charData.equals(this.charData)) ) );
	}
	
	
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode()
	{
		return this.charData == null ? 0 : this.charData.hashCode();
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		return "[VARCHAR: " + this.charData + ']'; 
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#encodeAsString()
	 */
	@Override
	public String encodeAsString()
	{
		return this.charData == null ? "NULL" : this.charData;
	}

	/* (non-Javadoc)
     * @see de.tuberlin.dima.minidb.core.DataField#getFromString(java.lang.String)
     */
    @Override
    public VarcharField getFromString(String charEncoded, int len) throws DataFormatException
    {
    	// check for NULL representation
    	if (charEncoded == null || charEncoded.equalsIgnoreCase("null")) {
    		return NULL_VALUE;
    	}
    	else if (charEncoded.length() > len) {
    		throw new DataFormatException("The given string is too long.");
    	}
    	else {
    		return new VarcharField(charEncoded.trim());
    	}
    }

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#encodeBinary(byte[], int)
	 */
	@Override
	public int encodeBinary(byte[] buffer, int offset)
	{
		if (this.charData == null) {
			return 0;
		}
		else {
			for (int i = 0; i < this.charData.length(); i++) {
				char c = this.charData.charAt(i);
				buffer[offset++] = (byte) c;
				buffer[offset++] = (byte) (c >> 8);
			}
			return this.charData.length() * 2;
		}
	}
	
	/**
	 * Creates a new varchar field from the given binary data. The entire array is
	 * read and decoded into a char field. The decoding is done through a fix charset.
	 * 
	 * @param binaryEncoded The binary data.
	 * @return A new varchar field that holds the decoded binary data.
	 */
	public static VarcharField getFieldFromBinary(byte[] binaryEncoded)
	{
		return VarcharField.getFieldFromBinary(binaryEncoded, 0, binaryEncoded.length);
	}
	
	/**
	 * Creates a new varchar field from the binary data, starting at the given offset and using
	 * the given number of bytes. The decoding is done through a fix charset.
	 * 
	 * @param binaryEncoded The binary data.
	 * @param offs The offset where to start decoding.
	 * @param len The number of bytes to use during decoding.
	 * @return A new varchar field based on the decoded bytes.
	 */
	public static VarcharField getFieldFromBinary(byte[] binaryEncoded, int offs, int len)
	{
		StringBuilder bld = new StringBuilder(len >> 1);
		for (int i = 0; i < len - 1; i += 2) {
			char c = (char) ((binaryEncoded[offs++] & 0x00ff) |
			                ((binaryEncoded[offs++] << 8) &0xff00));
			bld.append(c);
		}
		
		return new VarcharField(bld.toString().trim());
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#getFromBinary(byte[], int, int)
	 */
	@Override
	VarcharField getFromBinary(byte[] binaryEncoded, int offs, int len)
	{
		return VarcharField.getFieldFromBinary(binaryEncoded, offs, len);
	}

	/* (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(DataField o)
	{
		// no type checking for performance reasons
		String other = ((VarcharField) o).charData;
		
		if (this.charData == null) {
			return other == null ? 0 : -1;
		}
		else {
			return other == null ? 1 : this.charData.compareTo(other);
		}
	}
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#clone()
	 */
	@Override
	public VarcharField clone()
	{
		VarcharField f = (VarcharField) super.clone();
		f.charData = new String(this.charData);
		return f;
	}
}
