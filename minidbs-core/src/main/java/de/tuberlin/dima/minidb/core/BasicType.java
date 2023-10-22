package de.tuberlin.dima.minidb.core;


/**
 * The enumeration of the basic data types available in the system. The enumeration instances contain
 * additional information about type properties.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public enum BasicType
{
	/**
	 * The instance representing the data type for 'Small Integer'.
	 */
	SMALL_INT(false, true, true),
	
	/**
	 * The instance representing the data type for 'Integer'.
	 */
	INT(false, true, true),
	
	/**
	 * The instance representing the data type for 'Big Integer'.
	 */
	BIG_INT(false, true, true),
	
	/**
	 * The instance representing the data type for 'Float'.
	 */
	FLOAT(false, true, true),
	
	/**
	 * The instance representing the data type for 'Double'.
	 */
	DOUBLE(false, true, true),
	
	/**
	 * The instance representing the data type for 'Char'.
	 */
	CHAR(true, true, false),
	
	/**
	 * The instance representing the data type for 'VarChar'.
	 */
	VAR_CHAR(true, false, false),
	
	/**
	 * The instance representing the data type for 'Date'.
	 */
	DATE(false, true, false),
	
	/**
	 * The instance representing the data type for 'Time'.
	 */
	TIME(false, true, false),
	
	/**
	 * The instance representing the data type for 'Timestamp'.
	 */
	TIMESTAMP(false, true, false),
	
	/**
	 * The instance representing the data type for 'RID'.
	 */
	RID(false, true, false);
	
	
	// ------------------------------------------------------------------------
	
	
	/**
	 * Invisible constructor to instantiate parameterized elements in the enumeration.
	 * 
	 * @param arrayType A flag indicating whether this data type is an array.
	 * @param fixLength A flag indicating whether this data type is of fixed length.
	 * @param arithmetic A flag indicating whether this data type is arithmetic.
	 */
	private BasicType(boolean arrayType, boolean fixLength, boolean arithmetic)
	{
		this.arrayType = arrayType;
		this.isFixLength = fixLength;
		this.isArithmetic = arithmetic;
	}
	
	
	/**
	 * A flag indicating whether the data of this type consists of a single
	 * element or of an array of elements (such as for char(x), varchar(y), ...) 
	 */
	private boolean arrayType;

	/**
	 * A flag indicating whether the fields of this data type are of a fix length
	 * or if the length is variable. For array types, this indicates if the
	 * length of the array is fixed.
	 */
	private boolean isFixLength;

	/**
	 * Flag indicating whether this data type is arithmetic, i.e. whether computations can
	 * be performed on it.
	 */
	private boolean isArithmetic;
	
	
	// ------------------------------------------------------------------------
	
	
	/**
	 * Checks whether the data of this type consists of a single
	 * element or of an array of elements (such as for char(x), varchar(y), ...) 
	 * 
	 * @return true, if the type is an array, false otherwise.
	 */
	public boolean isArrayType()
    {
    	return this.arrayType;
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
    	return this.isFixLength;
    }
	
	/**
	 * Checks if the given type is an arithmetic type which can be used for calculations.
	 * 
	 * @return True, if the type is arithmetic, false otherwise.
	 */
	public boolean isArithmeticType()
	{
		return this.isArithmetic;
	}
}
