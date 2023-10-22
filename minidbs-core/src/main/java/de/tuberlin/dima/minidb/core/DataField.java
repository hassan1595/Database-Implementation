package de.tuberlin.dima.minidb.core;

import java.io.Serializable;


/**
 * The basic class that all classes for fields in a tuple inherit from.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public abstract class DataField implements Comparable<DataField>, Cloneable, Serializable
{
	/**
	 * A serial version UID to support object serialization.
	 */
	private static final long serialVersionUID = 4508159155489758273L;

	/**
	 * Gets the basic type (INT, CHAR, DATE, ...) of this field.
	 *  
	 * @return This field's basic type.
	 */
	public abstract BasicType getBasicType();
	
	
	/**
	 * Checks if this instance represents a field whose value is NULL.
	 * 
	 * @return true, if the value is NULL, false otherwise.
	 */
	public abstract boolean isNULL();
	
	/**
	 * Gets the number of bytes needed to represent this field binary.
	 * <p>
	 * For non-array types of fixed length (such as INT), this method returns
	 * always the same value, which is exactly the same value as the corresponding
	 * <tt>DataType</tt>'s method would return.
	 * <p>
	 * For array types of fixed length (such as CHAR), this method will return the same
	 * value for all fields that correspond to values from the same column. This value
	 * is also the same as the <tt>DataType</tt> instance for that column would return.
	 * <p>
	 * For array types of variable length (such as VARCHAR), this method returns the specific
	 * number of bytes needed to represent this specific instance.
	 * 
	 * @return The number of bytes required to represent this field.
	 */
	public abstract int getNumberOfBytes();
	
	
	/**
	 * Creates a new instance for the subclass of DataField that this data type corresponds to.
	 * The value of the data type is parsed from the given string.
	 * 
	 * For example <code>DataType.intType().getFromString("-2876")</code> returns a new instance
	 * of IntField holding the integer <code>-2876</code>. DataType.charType(8).getFromString("abcde")
	 * returns a new instance of CharField holding the String "abcde   ", and
	 * DataType.varcharType(4).getFromString("abcde") will throw a DataFormatException.
	 * 
	 * @param charEncoded The string with the encoded value.
	 * @param length The length of the field. Will only be interpreted by array types, such as CHAR and VARRCHAR.
	 * @return A new subclass of DataField with the value that was extracted from the string.
	 * @throws DataFormatException Thrown, if the string did not hold a character representation 
	 *                             of this data type.
	 */
	public abstract DataField getFromString(String charEncoded, int length)
	throws DataFormatException;
	
	
	/**
     * Encodes the value in this field as a string. For example the following
     * <code>new IntField(34).encodeAsString()</code> returns "34".
     * 
     * @return A string representation of the fields value.
     */
    public abstract String encodeAsString();
	
	
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
	 * @throws ArrayIndexOutOfBoundsException Thrown, if the array has not enough bytes after offs
	 *                                        to represent the binary encoded value, or if offs
	 *                                        is negative, or if len is negative.
	 */
	abstract DataField getFromBinary(byte[] binaryEncoded, int offset, int len);
	
	/**
	 * Serializes the value of this field as a binary sequence, using little endian
	 * encoding. The serialization starts at the given offset and uses as many bytes
	 * as necessary to represent the value. For none array types, those are as many as
	 * returned by the <code>getFieldWidth()</code> method, for array types this
	 * depends on the actual value.
	 * 
	 * @param buffer The buffer to serialize the value into.
	 * @param offset The offset into the binary array to start the serialization at.
	 * @return The number of bytes written into the array.
	 * @throws ArrayIndexOutOfBoundsException Thrown, if the array has not enough space
	 *                                        to hold the binary encoded value.
	 */
	public abstract int encodeBinary(byte[] buffer, int offset)
	throws ArrayIndexOutOfBoundsException;
	
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		return '[' + getBasicType().name() + ": " + encodeAsString() + ']'; 
	}
	
	/**
	 * Creates a deep copy of this data field.
	 * 
	 * @return A deep copy of this data field.
	 */
	@Override
	public DataField clone()
	{
		try {
			return (DataField) super.clone();
		}
		catch (CloneNotSupportedException e) {
			throw new InternalOperationFailure("BUG: Cloning a data field failed!", true, null);
		}
	}
}
