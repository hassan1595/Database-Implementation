package de.tuberlin.dima.minidb.core;


/**
 * A specific data field (part of a tuple) holding an integer (32 bit).
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public final class IntField extends DataField implements ArithmeticType<IntField>
{
	
	/**
	 * A serial version UID to support object serialization.
	 */
	private static final long serialVersionUID = 3177494545334924196L;
	
	/**
	 * The 32 bit value that is held by this integer object.
	 */
	private int value;
	
	
	/**
	 * Creates a new integer for the given value.
	 * 
	 * @param value The value for this data field.
	 */
	public IntField(int value)
	{
		this.value = value;
	}
	
	/**
	 * Package protected constructor to create default fields.
	 */
	IntField()
	{
		this.value = 0;
	}
	
	/**
	 * Gets the value of this data field.
	 * 
	 * @return The integer value held by this field.
	 */
	public int getValue()
	{
		return this.value;
	}
	

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#getDataType()
	 */
	@Override
    public BasicType getBasicType()
    {
		return BasicType.INT;
    }
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#getLength()
	 */
	@Override
	public int getNumberOfBytes()
	{
		return 4;
	}
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#isNULL()
	 */
	@Override
    public boolean isNULL()
    {
        return this.value == Integer.MIN_VALUE;
    }
	
	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object o)
	{
		return (o != null && o instanceof IntField && ((IntField) o).value == this.value); 
	}
	
	
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode()
	{
		return this.value;
	}
	
	/* 
	 * ************************************************************
	 *                   encoding / decoding
	 * ************************************************************            
	 */

	/**
	 * Extract an integer from the first 4 bytes from the given binary array.
	 * 
	 * @param binaryEncoded The binary array to extract the number from.
	 * @return The extracted number.
	 * @throws ArrayIndexOutOfBoundsException Thrown, if the array has not enough bytes
	 *                                        to represent the binary encoded value.
	 */
	public static IntField getFieldFromBinary(byte[] binaryEncoded)
    {
		return getFieldFromBinary(binaryEncoded, 0);
    }

	/**
	 * Extract a integer from the first 4 bytes after the given offset
	 * from the given binary array.
	 * 
	 * @param binaryEncoded The binary array to extract the number from.
	 * @param offs The offset where the binary coded number starts.
	 * @return The extracted number.
	 * @throws ArrayIndexOutOfBoundsException Thrown, if the array has not enough bytes
	 *                                        to represent the binary encoded value.
	 */
	public static IntField getFieldFromBinary(byte[] binaryEncoded, int offs)
	{
		return new IntField(getIntFromBinary(binaryEncoded, offs));
    }
	
	/**
	 * Extract an int from the first 4 bytes from the given binary array
	 * using little endian encoding.
	 * 
	 * @param binaryEncoded The binary array to extract the number from.
	 * @return The extracted number.
	 * @throws ArrayIndexOutOfBoundsException Thrown, if the array has not enough bytes
	 *                                        to represent the binary encoded value.
	 */
	public static int getIntFromBinary(byte[] binaryEncoded)
    {
		return getIntFromBinary(binaryEncoded, 0);
    }

	/**
	 * Extract an int from the first 4 bytes after the given offset
	 * from the given binary array using little endian encoding.
	 * 
	 * @param binaryEncoded The binary array to extract the number from.
	 * @param offs The offset where the binary coded number starts.
	 * @return The extracted number.
	 * @throws ArrayIndexOutOfBoundsException Thrown, if the array has not enough bytes
	 *                                        to represent the binary encoded value.
	 */
	public static int getIntFromBinary(byte[] binaryEncoded, int offs)
	{
		int val = (binaryEncoded[offs    ]        & 0x000000ff) |
                 ((binaryEncoded[offs + 1] <<  8) & 0x0000ff00) |
                 ((binaryEncoded[offs + 2] << 16) & 0x00ff0000) |
                 ((binaryEncoded[offs + 3] << 24) & 0xff000000); 
	    
		return val;
    }
	

	/**
	 * Encodes a given int into the binary array using little endian encoding.
	 * 
	 * @param value The value to be encoded.
	 * @param buffer The buffer to encode the value into.
	 * @param offset The offset into the buffer where to start the encoding.
	 */
	public static void encodeIntAsBinary(int value, byte[] buffer, int offset)
    {
	    buffer[offset]     = (byte) value;
	    buffer[offset + 1] = (byte) (value >>> 8);
	    buffer[offset + 2] = (byte) (value >>> 16);
	    buffer[offset + 3] = (byte) (value >>> 24);
    }
	

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#encodeAsString()
	 */
	@Override
    public String encodeAsString()
    {
		return isNULL() ? "NULL" : String.valueOf(this.value);
    }


	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#encodeBinary(byte[], int)
	 */
	@Override
	public int encodeBinary(byte[] buffer, int offset)
    {
		encodeIntAsBinary(this.value, buffer, offset);
	    return 4;
    }
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#getFromBinary(byte[], int)
	 */
	@Override
	public IntField getFromBinary(byte[] binaryEncoded, int offs, int len)
    {
		return IntField.getFieldFromBinary(binaryEncoded, offs);
    }


	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#getFromString(java.lang.String)
	 */
	@Override
	public IntField getFromString(String charEncoded, int len) throws DataFormatException
    {
	    try {
	    	int i = Integer.parseInt(charEncoded);
	    	return new IntField(i);
	    }
	    catch (NumberFormatException nfex) {
	    	if (charEncoded.equalsIgnoreCase("NULL")) {
	    		return (IntField) DataType.intType().getNullValue();
	    	}
	    	throw new DataFormatException("String does not contain a valid INT");
	    }
    }


	/* 
	 * ************************************************************
	 *                support for arithmetic
	 * ************************************************************            
	 */
	
	/* (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(DataField o)
	{
		// no type checking for performance reasons
		int anotherVal = ((IntField) o).value;
		return (this.value < anotherVal ? -1 : (this.value == anotherVal ? 0 : 1));
	}
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.ArithmeticType#add(de.tuberlin.dima.minidb.core.DataField)
	 */
    @Override
	public void add(IntField other)
    {
		this.value += other.value;
    }

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.ArithmeticType#add(int)
	 */
    @Override
	public void add(int summand)
    {
		this.value += summand;
    }

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.ArithmeticType#divideBy(de.tuberlin.dima.minidb.core.DataField)
	 */
    @Override
	public void divideBy(IntField other)
    {
	    this.value /= other.value;
	    
    }


	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.ArithmeticType#divideBy(int)
	 */
    @Override
	public void divideBy(int divisor)
    {
		this.value /= divisor;
    }


	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.ArithmeticType#multiplyWith(de.tuberlin.dima.minidb.core.DataField)
	 */
    @Override
	public void multiplyWith(IntField other)
    {
		this.value *= other.value;
    }


	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.ArithmeticType#multiplyWith(int)
	 */
    @Override
	public void multiplyWith(int factor)
    {
		this.value *= factor;
    }


	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.ArithmeticType#subtract(de.tuberlin.dima.minidb.core.DataField)
	 */
    @Override
	public void subtract(IntField other)
    {
		this.value -= other.value;
    }


	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.ArithmeticType#subtract(int)
	 */
    @Override
	public void subtract(int minuend)
    {
		this.value -= minuend;
    }

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.ArithmeticType#getZero()
	 */
	@Override
	public IntField createZero()
	{
		return new IntField(0);
	}
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.ArithmeticType#isZero()
	 */
	@Override
	public boolean isZero()
	{
		return this.value == 0;
	}
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.ArithmeticType#asLong()
	 */
	@Override
	public long asLong()
	{
		return this.value;
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.ArithmeticType#asDouble()
	 */
	@Override
	public double asDouble()
	{
		return this.value;
	}
}
