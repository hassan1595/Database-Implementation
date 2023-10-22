package de.tuberlin.dima.minidb.core;


/**
 * A specific data field (part of a tuple) holding a char. This data type does actually not
 * hold and runtime information about the length of the character array, and it consequently
 * does no checking. This is, because objects from this class occur in multitude are used in
 * the innermost methods. And additional information and checking here would mean a significant
 * overhead in memory consumption and processing.
 * 
 * The checking that the length of the string matches the data types array length is task of
 * the calling component that verifies the schema.
 * 
 * The character is serialized though a 16 bit encoding character set, hence every character
 * occupies two bytes.
 * 
 * The NULL value for a CHAR is represented by the first two bytes zero. This is distinguishable
 * from other strings, because a char has always a length of at least one and does not contain
 * any zero character (corresponds to two zero bytes).
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public final class CharField extends DataField
{
	/**
	 * A serial version UID to support object serialization.
	 */
	private static final long serialVersionUID = -5336867519448250766L;


	/**
	 * The singleton NULL value.
	 */
	static final CharField NULL_VALUE = new CharField();
	
	
	/**
	 * The character data for this char. If it is null, we have the NULL value.
	 */
	private String charData;
	
	
	/**
	 * Creates a new char field with the given string as character data.
	 * NOTE: No checking of the length happens here.
	 * 
	 * @param value The character data as a string, or null, for the NULL value.
	 */
	public CharField(String value)
	{
		this.charData = value;
	}
	
	/**
	 * Package level constructor for default instances.
	 */
	CharField()
	{
		this.charData = null;
	}
	
	/**
	 * Gets the character data in this char.
	 * 
	 * @return The character data as a string, or null, is this field is NULL.
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
		// we need four bytes to serialize the NULL value
		if (this.charData == null) {
			return 2;
		}
		else {
			return this.charData.length() * 2;
		}
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#getDataType()
	 */
	@Override
    public BasicType getBasicType()
    {
		return BasicType.CHAR;
    }

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#isNULL()
	 */
	@Override
    public boolean isNULL()
    {
		return this.charData == null;
    }
	
	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object o)
	{
		return (o != null && o instanceof CharField && 
				( (((CharField) o).charData == null && this.charData == null) ||
				  (((CharField) o).charData != null && ((CharField) o).charData.equals(this.charData)) ) );
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
    public CharField getFromString(String charEncoded, int len) throws DataFormatException
    {
    	// check for NULL representation
    	if (charEncoded == null || charEncoded.equalsIgnoreCase("null")) {
    		return NULL_VALUE;
    	}
    	else if (charEncoded.length() > len) {
    		throw new DataFormatException("The given string is too long.");
    	}
    	else if (charEncoded.length() < len) {
    		StringBuilder bld = new StringBuilder(len);
    		bld.append(charEncoded);
    		for (int i = charEncoded.length(); i < len; i++) {
    			bld.append(' ');
    		}
    		return new CharField(bld.toString());
    	}
    	else {
    		return new CharField(charEncoded);
    	}
    }

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#encodeBinary(byte[], int)
	 */
	@Override
	public int encodeBinary(byte[] buffer, int offset)
	{
		if (this.charData == null) {
			buffer[offset    ] = 0;
			buffer[offset + 1] = 0;
			return 2;
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
	 * Creates a new char field from the given binary data. If the array contents indicates
	 * a NULL value, a NULL value field is returned. Otherwise, the entire array is
	 * read and decoded into a char field. The decoding is done through a fix charset.
	 * 
	 * @param binaryEncoded The binary data.
	 * @return A new char field that holds the decoded binary data.
	 */
	public static CharField getFieldFromBinary(byte[] binaryEncoded)
	{
		return CharField.getFieldFromBinary(binaryEncoded, 0, binaryEncoded.length);
	}
	
	/**
	 * Creates a new char field from the binary data, starting at the given offset and using
	 * the given number of bytes. The decoding is done through a fix char-set.
	 * 
	 * If the array contents indicates a NULL value, a NULL value field is returned.
	 * 
	 * @param binaryEncoded The binary data.
	 * @param offs The offset where to start decoding.
	 * @param len The number of bytes to use during decoding.
	 * @return A new char field based on the decoded bytes.
	 */
	public static CharField getFieldFromBinary(byte[] binaryEncoded, int offs, int len)
	{
		// check if we are null
		if (binaryEncoded.length >= offs + 2 &&
			binaryEncoded[offs] == 0 && binaryEncoded[offs + 1] == 0)
		{
			return NULL_VALUE;
		}
		
		StringBuilder bld = new StringBuilder(len >> 1);
		for (int i = 0; i < len - 1; i += 2) {
			char c = (char) ((binaryEncoded[offs++] & 0x00ff) |
			        ((binaryEncoded[offs++] << 8) & 0xff00));
			bld.append(c);
		}
		
		return new CharField(bld.toString());
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#getFromBinary(byte[], int, int)
	 */
	@Override
	CharField getFromBinary(byte[] binaryEncoded, int offs, int len)
	{
		return CharField.getFieldFromBinary(binaryEncoded, offs, len);
	}

	/* (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(DataField o)
	{
		// no type checking for performance reasons
		String other = ((CharField) o).charData;
		
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
	public CharField clone()
	{
		CharField f = (CharField) super.clone();
		f.charData = new String(this.charData);
		return f;
	}
}
