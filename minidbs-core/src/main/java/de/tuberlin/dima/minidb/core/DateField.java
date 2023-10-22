package de.tuberlin.dima.minidb.core;



/**
 * The representation of a date field. The date is coded into a single 32 bit
 * integer the following way:
 * 
 * - The most significant 16 bits are the full year, unsigned.
 * - Bits 8 - 15 are the month, starting at 0 for January, till 11 for December.
 * - Bits 0 - 7 are the day.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public final class DateField extends DataField
{
	/**
	 * A serial version UID to support object serialization.
	 */
	private static final long serialVersionUID = -6996255022361126710L;

	/**
	 * The integer date value representing a NULL date.
	 */
	static final int NULL_VALUE = 0xffffffff;
	
	/**
	 * The date, encoded as described in the class specification. 
	 */
	private int date;

	
	/**
	 * Creates a new Date for the given day, month and year.
	 * 
	 * @param day The day of the month, 1 - 31.
	 * @param month The month, 0 - 11.
	 * @param year The year.
	 */
	public DateField(int day, int month, int year) throws DataFormatException
	{
		if (day < 1 || day > 31 || month < 0 || month > 11 || year < -10000 || year > 10000)
		{
			throw new DataFormatException("Invalid fields for a date.");
		}
		
		this.date = (year << 16) | ((month & 0xff) << 8) | (day & 0xff); 
	}
	
	/**
	 * Constructs a new date with the given encoded value. No checking is done.
	 * This constructor is for package level use only.
	 * 
	 * @param dateValue The encoded date.
	 */
	DateField(int dateValue)
	{
		this.date = dateValue;
	}
	
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#getDataType()
	 */
	@Override
    public BasicType getBasicType()
    {
		return BasicType.DATE;
    }

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#getNumberOfBytes()
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
		return this.date == NULL_VALUE;
    }
	
	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object o)
	{
		return (o != null && o instanceof DateField && ((DateField) o).date == this.date); 
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode()
	{
		return this.date;
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#encodeAsString()
	 */
	@Override
	public String encodeAsString()
	{
		if (isNULL()) {
			return "NULL";
		}
		else {
			StringBuilder bld = new StringBuilder(11);
			bld.append(getYear());
			bld.append('-');
			bld.append(getMonth() + 1);
			bld.append('-');
			bld.append(getDay());
			return bld.toString();
		}
	}
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#getFromString(java.lang.String)
	 */
	@Override
	public DateField getFromString(String charEncoded, int len) throws DataFormatException
	{
		if (charEncoded == null || charEncoded.equalsIgnoreCase("NULL")) {
			return (DateField) DataType.dateType().getNullValue();
		}
		else {
			int delim1 = charEncoded.indexOf('-');
			int delim2 = charEncoded.indexOf('-', 5);
			if (delim1 == 4 && (delim2 == 6 || delim2 == 7)) {
				try {
					int year = Integer.parseInt(charEncoded.substring(0, 4));
					int month = Integer.parseInt(charEncoded.substring(5, delim2));
					int day = Integer.parseInt(charEncoded.substring(delim2 + 1));
					
					return new DateField(day, month - 1, year);
				}
				catch (NumberFormatException ex) {
				}
				catch (IndexOutOfBoundsException ex) {
				}
			}

			// if we get here, we had a parse error
			throw new DataFormatException("Invalid date format. Expected Format: YYYY-MM-DD");
		}
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#encodeBinary(byte[], int)
	 */
	@Override
	public int encodeBinary(byte[] buffer, int offset) throws ArrayIndexOutOfBoundsException
	{
		buffer[offset    ] = (byte) (this.date       );
		buffer[offset + 1] = (byte) (this.date >>>  8);
		buffer[offset + 2] = (byte) (this.date >>> 16);
		buffer[offset + 3] = (byte) (this.date >>> 24);
		return 4;
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#getFromBinary(byte[], int, int)
	 */
	@Override
	DateField getFromBinary(byte[] binaryEncoded, int offs, int len)
	{
		return DateField.getFieldFromBinary(binaryEncoded, offs);
	}
	
	/**
	 * Deserializes a date from a binary form. Performs no checking, if this date
	 * is not in a valid format.
	 * 
	 * @param binaryEncoded The binary buffer to extract the date from.
	 * @param offs The offset in the buffer where to start the extraction from.
	 * @return The Date formed from the binary representation.
	 */
	public static DateField getFieldFromBinary(byte[] binaryEncoded, int offs)
	{
		int val = (binaryEncoded[offs    ]        & 0x000000ff) |
		         ((binaryEncoded[offs + 1] <<  8) & 0x0000ff00) |
		         ((binaryEncoded[offs + 2] << 16) & 0x00ff0000) |
			     ((binaryEncoded[offs + 3] << 24) & 0xff000000);
		
		return new DateField(val);
	}
	
	/**
	 * Extracts the year from the date.
	 * 
	 * @return The date's year.
	 */
	public int getYear()
	{
		return this.date >>> 16; 
	}
	
	/**
	 * Extracts the month from the date.
	 * 
	 * @return The date's month.
	 */
	public int getMonth()
	{
		return (this.date >> 8) & 0xff;
	}
	
	/**
	 * Extracts the day of month from the date.
	 * 
	 * @return The date's day of month.
	 */
	public int getDay()
	{
		return this.date & 0xff;
	}

	/* (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(DataField o)
	{
		// no type checking for performance reasons
		DateField other = (DateField) o; 
		if (isNULL()) {
			return other.isNULL() ? 0 : -1;
		}
		else if (other.isNULL()) {
			return 1;
		}
		else {
			return (this.date < other.date ? -1 : (this.date == other.date ? 0 : 1));
		}
	}
}
