package de.tuberlin.dima.minidb.core;


/**
 * The enumeration of data types available in the system. The enumeration instances contain
 * additional information about type properties.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public final class DataType
{
	/**
	 * The basic type (INT, CHAR, TIME, ...) of this data type.
	 */
	private final BasicType basicType;
	
	/**
	 * A private default instance of a field of this data type.
	 */
	private final DataField nullInstance;
	
	/**
	 * An instance that indicates the highest possible value;
	 */
	private final DataField maxValue;
	
	/**
	 * An instance that indicates the highest possible value;
	 */
	private final DataField minValue;
	
	/**
	 * The length of the data type. For array data types, it is their length
	 * (or maximal length, if variable length type), otherwise it is one.
	 */
	private final int length;
	
	/**
	 * Gets the number of bytes that a value of this data type occupies.
	 * For array data types, this is the number of bytes required by the maximal value length.
	 */
	private final int numberOfBytes;
	
	
	// ------------------------------------------------------------------------
	
	/**
	 * Invisible constructor to instantiate only through methods.
	 * 
	 * @param basicType The basic type of this DataType.
	 * @param nullInstance A value of from the domain of the data type that encodes NULL.
	 * @param minValue The minimum value of the data type.
	 * @param maxValue The maximum value of the data type.
	 * @param length The length of this data type.
	 * @param bytes The number of bytes for this type.
	 */
	private DataType(BasicType basicType, DataField nullInstance, DataField minValue, DataField maxValue, int length, int bytes)
	{
		this.basicType = basicType;
		this.nullInstance = nullInstance;
		this.minValue = minValue;
		this.maxValue = maxValue;
		this.length = length;
		this.numberOfBytes = bytes;
	}
	
	
	// ------------------------------------------------------------------------
	
	
	/**
	 * Gets the basic type of this data type, e.g. 'INT' or 'DATE' or 'VARCHAR'.
	 * 
	 * @return The basic type of this data type.
	 */
	public BasicType getBasicType()
	{
		return this.basicType;
	}
	
	/**
	 * Gets the instance representing the NULL value for this data type.
	 * 
	 * @return The NULL value instance.
	 */
	public DataField getNullValue()
	{
		return this.nullInstance;
	}
	
	/**
	 * Gets the maximal value from this DataType.
	 *
	 * @return The highest value.
	 */
	public DataField getMaxValue()
	{
		return this.maxValue;
	}

	/**
	 * Gets the minimal value for this DataType.
	 *
	 * @return The lowest value.
	 */
	public DataField getMinValue()
	{
		return this.minValue;
	}
	
	/**
	 * Gets the length of the data type. For array data types, it is their length
	 * (or maximal length, if variable length type), otherwise it is one.
	 * 
	 * @return The length of the data type.
	 */
	public int getLength()
	{
		return this.length;
	}

	/**
	 * Gets the number of bytes that a value of this data type occupies.
	 * For array data types, this is the number of bytes required by the maximal value length.
	 * 
	 * @return The number of bytes occupied.
	 */
	public int getNumberOfBytes()
    {
    	return this.numberOfBytes;
    }

	/**
	 * Checks whether the data of this type consists of a single
	 * element or of an array of elements (such as for char(x), varchar(y), ...) 
	 * 
	 * @return true, if the type is an array, false otherwise.
	 */
	public boolean isArrayType()
    {
    	return this.basicType.isArrayType();
    }

	/**
	 * Checks if the fields of this data type are of a fix length
	 * or if the length is variable. For array types, this indicates if the
	 * length of the array is fixed. None array types are always of
	 * fix length.
	 * 
	 * Example: char(x) is an array type of fix length, varchar(y) is a type of
	 * variable length.
	 * 
	 * @return true, if this is a fix length type, false otherwise.
	 */
	public boolean isFixLength()
    {
    	return this.basicType.isFixLength();
    }
	
	/**
	 * Checks if the given type is an arithmetic type which can be used for calculations.
	 * 
	 * @return True, if the type is arithmetic, false otherwise.
	 */
	public boolean isArithmeticType()
	{
		return this.basicType.isArithmeticType();
	}
	
	/**
	 * Creates a zero value for this data type that is cast to <tt>ArithmeticType</tt> to
	 * be used within aggregations.
	 * 
	 * @return A zero represented as a <tt>DataField</tt> of this DataType.
	 */
	public ArithmeticType<DataField> createArithmeticZero()
	{
		if (!this.basicType.isArithmeticType()) {
			throw new IllegalOperationException("The given field is no arithmetic type.");
		}
		else {
			return DataType.asArithmeticType(getNullValue()).createZero();
		}
	}
	

	
	/**
	 * Creates a new instance for the subclass of DataField that this data type corresponds to.
	 * The value of the data type is parsed from the given string.
	 * 
	 * For example <code>DataType.INT.getFromString("-2876")</code> returns a new instance
	 * of IntField holding the integer <code>-2876</code>.
	 * 
	 * @param charEncoded The string with the encoded value.
	 * @return A new subclass of DataField with the value that was extracted from the string.
	 * @throws DataFormatException Thrown, if the string did not hold a character representation 
	 *                             of this data type
	 */
	public DataField getFromString(String charEncoded)
	throws DataFormatException
	{
		return this.nullInstance.getFromString(charEncoded, this.length);
	}
	
	/**
	 * Creates a new instance for the subclass of DataField that this data type corresponds to.
	 * The value of the data type is deserialized from the binary array interpreting the
	 * contents of the array as laid out little endian.
	 * 
	 * For example <code>DataType.intType().getFromBinary(new byte[] {1, 1, 0, 0})</code> returns a
	 * new instance of IntField holding the integer <code>257</code>.
	 * 
	 * @param binaryEncoded The binary array with the serialized value.
	 * @return A new subclass of DataField with the value that was composed from the binary fields.
	 * @throws ArrayIndexOutOfBoundsException Thrown, if the array has not enough bytes
	 *                                        to represent the binary encoded value.
	 */
	public DataField getFromBinary(byte[] binaryEncoded)
	{
		return this.nullInstance.getFromBinary(binaryEncoded, 0, binaryEncoded.length);
	}
	
	/**
	 * Creates a new instance for the subclass of DataField that this data type corresponds to.
	 * The value of the data type is deserialized from the binary array starting at the given
	 * offset, interpreting the contents of the array as laid out little endian.
	 * 
	 * For example <code>DataType.intType().getFromBinary(new byte[] {1, 1, 1, 1, 0, 0}, 2)</code>
	 * returns a new instance of IntField holding the integer <code>257</code>.
	 * 
	 * @param binaryEncoded The binary array with the serialized value.
	 * @param offset The position to start in the binary array.
	 * @return A new subclass of DataField with the value that was composed from the binary fields.
	 * @throws ArrayIndexOutOfBoundsException Thrown, if the array has not enough bytes
	 *                                        to represent the binary encoded value.
	 */
	public DataField getFromBinary(byte[] binaryEncoded, int offset)
	{
		return this.nullInstance.getFromBinary(binaryEncoded, offset, getNumberOfBytes());
	}
	
	/**
	 * Creates a new instance for the subclass of DataField that this data type corresponds to.
	 * The value of the data type is deserialized from the binary array starting at the given
	 * offset, using the number of bytes as given by the length, interpreting the contents of
	 * the array as laid out little endian. The length field is ignored for non array data types
	 * like INT or FLOAT.
	 * 
	 * @param binaryEncoded The binary array with the serialized value.
	 * @param offset The position to start in the binary array.
	 * @param len The number of bytes to be deserialized. Only relevant for array types 
	 *            like CHAR and VARCHAR
	 * @return A new subclass of DataField with the value that was composed from the binary fields.
	 * @throws ArrayIndexOutOfBoundsException Thrown, if the array has not enough bytes
	 *                                        to represent the binary encoded value.
	 */
	public DataField getFromBinary(byte[] binaryEncoded, int offset, int len)
	{
		return this.nullInstance.getFromBinary(binaryEncoded, offset, len);
	}
	
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		StringBuilder bld = new StringBuilder(12);
		bld.append(this.basicType.name());
		
		if (isArrayType()) {
			bld.append('(');
			bld.append(this.length);
			bld.append(')');
		}
		
		return bld.toString();
	}
	
	/**
	 * Gives an arithmetic type cast to the given field.
	 * 
	 * @param field The field to be casted.
	 * @return The field casted to an arithmetic type.
	 * @throws IllegalArgumentException Thrown, if the field's data type is not an arithmetic type. 
	 */
	public static ArithmeticType<DataField> asArithmeticType(DataField field)
	{
		if (field.getBasicType().isArithmeticType()) {
			@SuppressWarnings("unchecked")
			ArithmeticType<DataField> at = (ArithmeticType<DataField>) field;
			return at;
		}
		else {
			throw new IllegalArgumentException("The given field is no arithmetic type.");
		}
	}
	
	
	// ------------------------------------------------------------------------
	
	
	/**
	 * Gets the data type constant representing 'SMALL_INT'.
	 *  
	 * @return The data type constant representing 'SMALL_INT'.
	 */
	public static DataType smallIntType()
	{
		return SMALL_INT;
	}
	
	/**
	 * Gets the data type constant representing 'INT'.
	 *  
	 * @return The data type constant representing 'INT'.
	 */
	public static DataType intType()
	{
		return INT;
	}
	
	/**
	 * Gets the data type constant representing 'BIG_INT'.
	 *  
	 * @return The data type constant representing 'BIG_INT'.
	 */
	public static DataType bigIntType()
	{
		return BIG_INT;
	}
	
	/**
	 * Gets the data type constant representing 'FLOAT'.
	 *  
	 * @return The data type constant representing 'FLOAT'.
	 */
	public static DataType floatType()
	{
		return FLOAT;
	}
	
	/**
	 * Gets the data type constant representing 'DOUBLE'.
	 *  
	 * @return The data type constant representing 'DOUBLE'.
	 */
	public static DataType doubleType()
	{
		return DOUBLE;
	}
	
	/**
	 * Gets the data type constant representing 'DATE'.
	 *  
	 * @return The data type constant representing 'DATE'.
	 */
	public static DataType dateType()
	{
		return DATE;
	}
	
	/**
	 * Gets the data type constant representing 'TIME'.
	 *  
	 * @return The data type constant representing 'TIME'.
	 */
	public static DataType timeType()
	{
		return TIME;
	}
	
	/**
	 * Gets the data type constant representing 'TIMESTAMP'.
	 *  
	 * @return The data type constant representing 'TIMESTAMP'.
	 */
	public static DataType timestampType()
	{
		return TIMESTAMP;
	}
	
	/**
	 * Gets the data type constant representing 'RID'.
	 *  
	 * @return The data type constant representing 'RID'.
	 */
	public static DataType ridType()
	{
		return RID;
	}
	
	/**
	 * Gets the data type representing a 'CHAR' of the given length.
	 *  
	 * @return The data type representing CHAR(length).
	 */
	public static DataType charType(int length)
	{
		char[] maxChar = new char[length];
		
		for (int i = 0; i < length; i++) {
			maxChar[i] = 0xffff;
		}
		
		return new DataType(BasicType.CHAR, CharField.NULL_VALUE, CharField.NULL_VALUE, 
				new CharField(new String(maxChar)), length, length * 2);
	}
	
	/**
	 * Gets the data type representing a 'VAR_CHAR' of the given maximal length.
	 *  
	 * @return The data type representing VAR_CHAR(maxLength).
	 */
	public static DataType varcharType(int maxLength)
	{
		char[] maxChar = new char[maxLength];
		
		for (int i = 0; i < maxLength; i++) {
			maxChar[i] = 0xffff;
		}
		
		return new DataType(BasicType.VAR_CHAR, VarcharField.NULL_VALUE, VarcharField.NULL_VALUE, 
				new VarcharField(new String(maxChar)), maxLength, maxLength * 2);
	}
	
	/**
	 * Gets the data type that is associated with the given basic type and the
	 * given length.
	 * 
	 * @param basicType The basic type (INT, CHAR, TIME, ...).
	 * @param length The length. Only relevant for array types (CHAR, VARCHAR).
	 * @return The data type for the basic type and length.
	 */
	public static DataType get(BasicType basicType, int length)
	{
		switch (basicType) {
		case SMALL_INT:
			return smallIntType();
		case INT:
			return intType();
		case BIG_INT:
			return bigIntType();
		case FLOAT:
			return floatType();
		case DOUBLE:
			return doubleType();
		case DATE:
			return dateType();
		case TIME:
			return timeType();
		case TIMESTAMP:
			return timestampType();
		case CHAR:
			return charType(length);
		case VAR_CHAR:
			return varcharType(length);
		default:
			return null;
		}
	}
	
	// ------------------------------------------------------------------------
	
	/**
	 * The instance representing the data type for 'Small Integer'.
	 */
	private static final DataType SMALL_INT = new DataType(
			BasicType.SMALL_INT,
			new SmallIntField(Short.MIN_VALUE), new SmallIntField(Short.MIN_VALUE),
			new SmallIntField(Short.MAX_VALUE), 1, 2);
	
	/**
	 * The instance representing the data type for 'Integer'.
	 */
	private static final DataType INT = new DataType(
			BasicType.INT,
			new IntField(Integer.MIN_VALUE), new IntField(Integer.MIN_VALUE),
			new IntField(Integer.MAX_VALUE), 1, 4);
	
	/**
	 * The instance representing the data type for 'Big Integer'.
	 */
	private static final DataType BIG_INT = new DataType(
			BasicType.BIG_INT,
			new BigIntField(Long.MIN_VALUE), new BigIntField(Long.MIN_VALUE), 
			new BigIntField(Long.MAX_VALUE), 1, 8);
	
	/**
	 * The instance representing the data type for 'Float'.
	 */
	private static final DataType FLOAT = new DataType(
			BasicType.FLOAT,
			new FloatField(Float.NaN), new FloatField(Float.MIN_VALUE), 
			new FloatField(Float.MAX_VALUE), 1, 4);
	
	/**
	 * The instance representing the data type for 'Double'.
	 */
	private static final DataType DOUBLE = new DataType(
			BasicType.DOUBLE,
			new DoubleField(Double.NaN), new DoubleField(Double.MIN_VALUE), 
			new DoubleField(Double.MAX_VALUE), 1, 8);
	
	/**
	 * The instance representing the data type for 'Date'.
	 */
	private static final DataType DATE = new DataType(
			BasicType.DATE,
			new DateField(DateField.NULL_VALUE), new DateField(DateField.NULL_VALUE), 
			new DateField(Integer.MAX_VALUE), 1, 4);
	
	/**
	 * The instance representing the data type for 'Time'.
	 */
	private static final DataType TIME = new DataType(
			BasicType.TIME,
			new TimeField(TimeField.NULL_VALUE), new TimeField(TimeField.NULL_VALUE), 
			new TimeField(Integer.MAX_VALUE), 1, 8);
	
	/**
	 * The instance representing the data type for 'Timestamp'.
	 */
	private static final DataType TIMESTAMP = new DataType(
			BasicType.TIMESTAMP,
			new TimestampField(TimestampField.NULL_VALUE), new TimestampField(TimestampField.NULL_VALUE),
			new TimestampField(Long.MAX_VALUE), 1, 8);
	
	/**
	 * The instance representing the data type for 'RID'.
	 */
	private static final DataType RID = new DataType(
			BasicType.RID,
			new RID(0, 0), new RID(0, 0), new RID(Integer.MAX_VALUE, Integer.MAX_VALUE), 1, 8);
	
}
