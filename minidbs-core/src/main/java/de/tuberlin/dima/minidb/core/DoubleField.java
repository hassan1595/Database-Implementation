package de.tuberlin.dima.minidb.core;


/**
 * A specific data field (part of a tuple) holding an 64 bit IEEE floating point value.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public final class DoubleField extends DataField implements ArithmeticType<DoubleField>
{
	
	/**
	 * A serial version UID to support object serialization.
	 */
	private static final long serialVersionUID = 4642265017406611843L;
	
	/**
	 * The 64 bit value that is held by this double object.
	 */
	private double value;
	
	
	/**
	 * Creates a new double field for the given value.
	 * 
	 * @param value The value for this data field.
	 */
	public DoubleField(double value)
	{
		this.value = value;
	}
	
	/**
	 * Package protected constructor to create default fields.
	 */
	DoubleField()
	{
		this.value = 0.0;
	}
	
	/**
	 * Gets the value of this data field.
	 * 
	 * @return The float value held by this field.
	 */
	public double getValue()
	{
		return this.value;
	}
	

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#getDataType()
	 */
	@Override
    public BasicType getBasicType()
    {
		return BasicType.DOUBLE;
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
        return Double.isNaN(this.value);
    }
	
	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object o)
	{
		if (o != null && o instanceof DoubleField) {
			DoubleField other = (DoubleField) o;
			
			return (other.isNULL() && isNULL()) ||
			       (other.value == this.value); 
		}
		else {
			return false;
		}
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode()
	{
		long bits = Double.doubleToLongBits(this.value);
		return ((int) bits) ^ ((int) (bits >>> 32));
	}
	
	/* 
	 * ************************************************************
	 *                   encoding / decoding
	 * ************************************************************            
	 */

	/**
	 * Extract a double from the first 8 bytes from the given binary array.
	 * 
	 * @param binaryEncoded The binary array to extract the number from.
	 * @return The extracted field.
	 * @throws ArrayIndexOutOfBoundsException Thrown, if the array has not enough bytes
	 *                                        to represent the binary encoded value.
	 */
	public static DoubleField getFieldFromBinary(byte[] binaryEncoded)
    {
		return getFieldFromBinary(binaryEncoded, 0);
    }

	/**
	 * Extract a double from the first 8 bytes after the given offset
	 * from the given binary array.
	 * 
	 * @param binaryEncoded The binary array to extract the number from.
	 * @param offs The offset where the binary cosed number starts.
	 * @return The extracted field.
	 * @throws ArrayIndexOutOfBoundsException Thrown, if the array has not enough bytes
	 *                                        to represent the binary encoded value.
	 */
	public static DoubleField getFieldFromBinary(byte[] binaryEncoded, int offs)
	{
		long bits1 = (binaryEncoded[offs]            & 0x000000ffL) |
	                ((binaryEncoded[offs + 1] <<  8) & 0x0000ff00L) |
	                ((binaryEncoded[offs + 2] << 16) & 0x00ff0000L) |
	                ((binaryEncoded[offs + 3] << 24) & 0xff000000L);
		
		long bits2 = (binaryEncoded[offs + 4]        & 0x000000ffL) |
                    ((binaryEncoded[offs + 5] <<  8) & 0x0000ff00L) |
                    ((binaryEncoded[offs + 6] << 16) & 0x00ff0000L) |
                    ((binaryEncoded[offs + 7] << 24) & 0xff000000L);
		
		return new DoubleField(Double.longBitsToDouble(bits1 | (bits2 << 32)));
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
		long bits = Double.doubleToRawLongBits(this.value);
		
	    buffer[offset]     = (byte) bits;
	    buffer[offset + 1] = (byte) (bits >>> 8);
	    buffer[offset + 2] = (byte) (bits >>> 16);
	    buffer[offset + 3] = (byte) (bits >>> 24);
	    buffer[offset + 4] = (byte) (bits >>> 32);
	    buffer[offset + 5] = (byte) (bits >>> 40);
	    buffer[offset + 6] = (byte) (bits >>> 48);
	    buffer[offset + 7] = (byte) (bits >>> 56);
	    return 8;
    }
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#getFromBinary(byte[], int)
	 */
	@Override
	public DoubleField getFromBinary(byte[] binaryEncoded, int offs, int len)
    {
		return DoubleField.getFieldFromBinary(binaryEncoded, offs);
    }


	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#getFromString(java.lang.String)
	 */
	@Override
	public DoubleField getFromString(String charEncoded, int len) throws DataFormatException
    {
	    try {
	    	double d = Double.parseDouble(charEncoded);
	    	return new DoubleField(d);
	    }
	    catch (NumberFormatException nfex) {
	    	if (charEncoded.equalsIgnoreCase("NULL")) {
	    		return (DoubleField) DataType.doubleType().getNullValue();
	    	}
	    	throw new DataFormatException("String does not contain a valid DOUBLE");
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
		DoubleField other = (DoubleField) o; 
		if (isNULL()) {
			return other.isNULL() ? 0 : -1;
		}
		else if (other.isNULL()) {
			return 1;
		}
		else {
			return (this.value < other.value ? -1 : (this.value == other.value ? 0 : 1));
		}
	}
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.ArithmeticType#add(de.tuberlin.dima.minidb.core.DataField)
	 */
    @Override
	public void add(DoubleField other)
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
	public void divideBy(DoubleField other)
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
	public void multiplyWith(DoubleField other)
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
	public void subtract(DoubleField other)
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
	public DoubleField createZero()
	{
		return new DoubleField(0.0);
	}
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.ArithmeticType#isZero()
	 */
	@Override
	public boolean isZero()
	{
		return this.value == +0.0 || this.value == -0.0;
	}
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.ArithmeticType#asLong()
	 */
	@Override
	public long asLong()
	{
		return (long) this.value;
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
