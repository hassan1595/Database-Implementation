package de.tuberlin.dima.minidb.core;


/**
 * A specific data field (part of a tuple) holding a small integer (16 bit).
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public final class SmallIntField extends DataField implements ArithmeticType<SmallIntField>
{
	/**
	 * A serial version UID to support object serialization.
	 */
	private static final long serialVersionUID = -4532402643045613777L;
	
	/**
	 * The 16 bit value that is held by this small integer object.
	 */
	private short value;

	/**
	 * Creates a new small integer for the given value.
	 * 
	 * @param value The value for this data field.
	 */
	public SmallIntField(short value)
	{
		this.value = value;
	}
	
	/**
	 * Package protected constructor to create default fields.
	 */
	SmallIntField()
	{
		this.value = 0;
	}
	
	/**
	 * Gets the value held by this small integer.
	 * 
	 * @return The contained value
	 */
	public short getValue()
    {
    	return this.value;
    }
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#getDataType()
	 */
	@Override
    public BasicType getBasicType()
    {
		return BasicType.SMALL_INT;
    }
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#getLength()
	 */
	@Override
	public int getNumberOfBytes()
	{
		return 2;
	}
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#isNULL()
	 */
	@Override
    public boolean isNULL()
    {
        return this.value == Short.MIN_VALUE;
    }
	
	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object o)
	{
		return (o != null && o instanceof SmallIntField && ((SmallIntField) o).value == this.value); 
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
	 * Extract a small integer from the first 2 bytes from the given binary array.
	 * 
	 * @param binaryEncoded The binary array to extract the number from.
	 * @return The extracted number.
	 */
	public static SmallIntField getFieldFromBinary(byte[] binaryEncoded)
    {
		return getFieldFromBinary(binaryEncoded, 0);
    }

	/**
	 * Extract a small integer from the first 2 bytes after the given offset
	 * from the given binary array.
	 * 
	 * @param binaryEncoded The binary array to extract the number from.
	 * @param offs The offset where the binary cosed number starts.
	 * @return The extracted number.
	 */
	public static SmallIntField getFieldFromBinary(byte[] binaryEncoded, int offs)
    {
		int value = (binaryEncoded[offs    ]       & 0x00ff) |
                   ((binaryEncoded[offs + 1] << 8) & 0xff00);
		
	    return new SmallIntField((short) value);
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
	    buffer[offset] = (byte) this.value;
	    buffer[offset + 1] = (byte) (this.value >>> 8);
	    return 2;
    }
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#getFromBinary(byte[], int)
	 */
	@Override
	public SmallIntField getFromBinary(byte[] binaryEncoded, int offs, int len)
    {		
		return SmallIntField.getFieldFromBinary(binaryEncoded, offs);
    }


	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#getFromString(java.lang.String)
	 */
	@Override
	public SmallIntField getFromString(String charEncoded, int len) throws DataFormatException
    {
	    try {
	    	short s = Short.parseShort(charEncoded);
	    	return new SmallIntField(s);
	    }
	    catch (NumberFormatException nfex) {
	    	if (charEncoded.equalsIgnoreCase("NULL")) {
	    		return (SmallIntField) DataType.smallIntType().getNullValue();
	    	}
	    	throw new DataFormatException("String does not contain a valid SMALL_INT");
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
	public int compareTo(DataField o) {
		// no type checking for performance reasons
		int anotherVal = ((SmallIntField) o).value;
		return (this.value < anotherVal ? -1 : (this.value == anotherVal ? 0 : 1));
	}
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.ArithmeticType#add(de.tuberlin.dima.minidb.core.DataField)
	 */
    @Override
	public void add(SmallIntField other)
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
	public void divideBy(SmallIntField other)
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
	public void multiplyWith(SmallIntField other)
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
	public void subtract(SmallIntField other)
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
	public SmallIntField createZero()
	{
		return new SmallIntField((short) 0);
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
