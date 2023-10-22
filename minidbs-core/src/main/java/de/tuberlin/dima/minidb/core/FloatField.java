package de.tuberlin.dima.minidb.core;


/**
 * A specific data field (part of a tuple) holding an 32 bit IEEE floating point value.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public final class FloatField extends DataField implements ArithmeticType<FloatField>
{
	
	/**
	 * A serial version UID to support object serialization.
	 */
	private static final long serialVersionUID = 7750453110036649634L;
	
	/**
	 * The 32 bit value that is held by this small integer object.
	 */
	private float value;
	
	
	/**
	 * Creates a new float field for the given value.
	 * 
	 * @param value The value for this data field.
	 */
	public FloatField(float value)
	{
		this.value = value;
	}
	
	/**
	 * Package protected constructor to create default fields.
	 */
	FloatField()
	{
		this.value = 0.0f;
	}
	
	/**
	 * Gets the value of this data field.
	 * 
	 * @return The float value held by this field.
	 */
	public float getValue()
	{
		return this.value;
	}
	

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#getDataType()
	 */
	@Override
    public BasicType getBasicType()
    {
		return BasicType.FLOAT;
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
        return Float.isNaN(this.value);
    }
	
	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object o)
	{
		if (o != null && o instanceof FloatField) {
			FloatField other = (FloatField) o;
			
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
		int bits = Float.floatToIntBits(this.value);
		return bits;
	}
	
	
	/* 
	 * ************************************************************
	 *                   encoding / decoding
	 * ************************************************************            
	 */

	/**
	 * Extract a float from the first 4 bytes from the given binary array.
	 * 
	 * @param binaryEncoded The binary array to extract the number from.
	 * @return The extracted field.
	 * @throws ArrayIndexOutOfBoundsException Thrown, if the array has not enough bytes
	 *                                        to represent the binary encoded value.
	 */
	public static FloatField getFieldFromBinary(byte[] binaryEncoded)
    {
		return getFieldFromBinary(binaryEncoded, 0);
    }

	/**
	 * Extract a float from the first 4 bytes after the given offset
	 * from the given binary array.
	 * 
	 * @param binaryEncoded The binary array to extract the number from.
	 * @param offs The offset where the binary cosed number starts.
	 * @return The extracted field.
	 * @throws ArrayIndexOutOfBoundsException Thrown, if the array has not enough bytes
	 *                                        to represent the binary encoded value.
	 */
	public static FloatField getFieldFromBinary(byte[] binaryEncoded, int offs)
	{
		int bits = (binaryEncoded[offs]            & 0x000000ff) |
	              ((binaryEncoded[offs + 1] <<  8) & 0x0000ff00) |
	              ((binaryEncoded[offs + 2] << 16) & 0x00ff0000) |
	              ((binaryEncoded[offs + 3] << 24) & 0xff000000);
		
		return new FloatField(Float.intBitsToFloat(bits));
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
		int bits = Float.floatToRawIntBits(this.value);
		
	    buffer[offset]     = (byte) bits;
	    buffer[offset + 1] = (byte) (bits >>> 8);
	    buffer[offset + 2] = (byte) (bits >>> 16);
	    buffer[offset + 3] = (byte) (bits >>> 24);
	    return 4;
    }
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#getFromBinary(byte[], int)
	 */
	@Override
	public FloatField getFromBinary(byte[] binaryEncoded, int offs, int len)
    {
		return FloatField.getFieldFromBinary(binaryEncoded, offs);
    }


	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#getFromString(java.lang.String)
	 */
	@Override
	public FloatField getFromString(String charEncoded, int len) throws DataFormatException
    {
	    try {
	    	float f = Float.parseFloat(charEncoded);
	    	return new FloatField(f);
	    }
	    catch (NumberFormatException nfex) {
	    	if (charEncoded.equalsIgnoreCase("NULL")) {
	    		return (FloatField) DataType.floatType().getNullValue();
	    	}
	    	throw new DataFormatException("String does not contain a valid FLOAT");
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
		FloatField other = (FloatField) o; 
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
	public void add(FloatField other)
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
	public void divideBy(FloatField other)
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
	public void multiplyWith(FloatField other)
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
	public void subtract(FloatField other)
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
	public FloatField createZero()
	{
		return new FloatField(0.0f);
	}
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.ArithmeticType#isZero()
	 */
	@Override
	public boolean isZero()
	{
		return this.value == +0.0f || this.value == -0.0f;
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
