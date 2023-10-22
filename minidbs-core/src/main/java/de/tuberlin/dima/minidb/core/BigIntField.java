package de.tuberlin.dima.minidb.core;


/**
 * A specific data field (part of a tuple) holding an integer (64 bit).
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public final class BigIntField extends DataField implements ArithmeticType<BigIntField>
{
	/**
	 * A serial version UID to support object serialization.
	 */
	private static final long serialVersionUID = -2797940975446930276L;
	/**
	 * The 64 bit value that is held by this big integer object.
	 */
	private long value;
	
	
	/**
	 * Creates a new integer for the given value.
	 * 
	 * @param value The value for this data field.
	 */
	public BigIntField(long value)
	{
		this.value = value;
	}
	
	/**
	 * Package protected constructor to create default fields.
	 */
	BigIntField()
	{
		this.value = 0;
	}
	

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#getDataType()
	 */
	@Override
    public BasicType getBasicType()
    {
		return BasicType.BIG_INT;
    }
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#getLength()
	 */
	@Override
	public int getNumberOfBytes()
	{
		return 8;
	}
	
	
	/**
	 * Gets the value of this data field.
	 * 
	 * @return The integer value held by this field.
	 */
	public long getValue()
	{
		return this.value;
	}
	
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#isNULL()
	 */
	@Override
    public boolean isNULL()
    {
        return this.value == Long.MIN_VALUE;
    }
	
	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object o)
	{
		return (o != null && o instanceof BigIntField && ((BigIntField) o).value == this.value);
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode()
	{
		return ((int) this.value) ^ ((int) (this.value >>> 32));
	}

	
	/* 
	 * ************************************************************
	 *                   encoding / decoding
	 * ************************************************************            
	 */
	
	/**
	 * Extract a big integer from the first 8 bytes from the given binary array.
	 * 
	 * @param binaryEncoded The binary array to extract the number from.
	 * @return The extracted number.
	 */
	public static BigIntField getFieldFromBinary(byte[] binaryEncoded)
    {
		return getFieldFromBinary(binaryEncoded, 0);
    }

	/**
	 * Extract a big integer from the first 8 bytes after the given offset
	 * from the given binary array.
	 * 
	 * @param binaryEncoded The binary array to extract the number from.
	 * @param offs The offset where the binary coded number starts.
	 * @return The extracted number.
	 */
	public static BigIntField getFieldFromBinary(byte[] binaryEncoded, int offs)
    {
		long l = (binaryEncoded[offs    ]        & 0x000000ffL) |
                ((binaryEncoded[offs + 1] <<  8) & 0x0000ff00L) |
                ((binaryEncoded[offs + 2] << 16) & 0x00ff0000L) |
                ((binaryEncoded[offs + 3] << 24) & 0xff000000L);

		long ii = (binaryEncoded[offs + 4]        & 0x000000ffL) |
                 ((binaryEncoded[offs + 5] <<  8) & 0x0000ff00L) |
                 ((binaryEncoded[offs + 6] << 16) & 0x00ff0000L) |
                 ((binaryEncoded[offs + 7] << 24) & 0xff000000L);

	    return new BigIntField(l | (ii << 32));
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
	    buffer[offset]     = (byte)  this.value;
	    buffer[offset + 1] = (byte) (this.value >>> 8);
	    buffer[offset + 2] = (byte) (this.value >>> 16);
	    buffer[offset + 3] = (byte) (this.value >>> 24);
	    buffer[offset + 4] = (byte) (this.value >>> 32);
	    buffer[offset + 5] = (byte) (this.value >>> 40);
	    buffer[offset + 6] = (byte) (this.value >>> 48);
	    buffer[offset + 7] = (byte) (this.value >>> 56);
	    return 8;
    }
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#getFromBinary(byte[], int)
	 */
	@Override
	public BigIntField getFromBinary(byte[] binaryEncoded, int offs, int len)
    {		
		return BigIntField.getFieldFromBinary(binaryEncoded, offs);
    }


	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#getFromString(java.lang.String)
	 */
	@Override
	public BigIntField getFromString(String charEncoded, int len) throws DataFormatException
    {
	    try {
	    	long l = Long.parseLong(charEncoded);
	    	return new BigIntField(l);
	    }
	    catch (NumberFormatException nfex) {
	    	if (charEncoded.equalsIgnoreCase("NULL")) {
	    		return (BigIntField) DataType.bigIntType().getNullValue();
	    	}
	    	
	    	throw new DataFormatException("String does not contain a valid BIG_INT");
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
		long anotherVal = ((BigIntField) o).value;
		return (this.value < anotherVal ? -1 : (this.value == anotherVal ? 0 : 1));
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.ArithmeticType#add(de.tuberlin.dima.minidb.core.DataField)
	 */
	@Override
	public void add(BigIntField other)
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
	public void divideBy(BigIntField other)
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
	public void multiplyWith(BigIntField other)
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
	public void subtract(BigIntField other)
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
	public BigIntField createZero()
	{
		return new BigIntField(0);
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
