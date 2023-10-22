package de.tuberlin.dima.minidb.core;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.SimpleTimeZone;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;


/**
 * This class represents a field of type TIMESTAMP. The timestamp is internally
 * represents as a 64 bit value counting the milliseconds since
 * midnight, January 1st, 1970, UTC.
 * 
 * Default parsing and printing of timestamps is done from the format
 * "YYYY-MM-DD HH:MM:SS.mmm". For example "1997-11-20 13:56:12.003" represents
 * the 20th of November, 1997 AD, 1:56:12 pm and 3 milliseconds. 
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class TimestampField extends DataField
{
	/**
	 * A serial version UID to support object serialization.
	 */
	private static final long serialVersionUID = 6088794747097283188L;

	/**
	 * The singleton time zone representing the UTC time zone.
	 */
	private static final TimeZone TIME_ZONE = new SimpleTimeZone(0, "UTC");

	/**
	 * The millisecond value representing a NULL timestamp.
	 */
	static final long NULL_VALUE = Long.MIN_VALUE;
	
	/**
	 * The milliseconds since midnight, January 1st, 1970.
	 */
	private long millis;
	
	
	/**
	 * Constructs a new Timestamp for the current point in time, as given
	 * by the host computers internal clock.
	 */
	public TimestampField()
	{
		this.millis = System.currentTimeMillis();
	}
	
	/**
	 * Constructs a new timestamp for the given values.
	 * 
	 * @param day The day of month, starting at 1 - 31.
	 * @param month The month, 0 - 11;
	 * @param year The year.
	 * @param hour The hour in 24h mode, 0 - 23.
	 * @param minute The minute, 0 - 59.
	 * @param second The second, 0 - 59.
	 * @param millis The milliseconds, 0 - 999.
	 * 
	 * @throws IllegalArgumentException Thrown, if any of the fields is out of range.
	 */
	public TimestampField(int day, int month, int year, int hour, int minute, int second, int millis)
	throws IllegalArgumentException
	{
		GregorianCalendar cal = new GregorianCalendar(TIME_ZONE);
		cal.setLenient(false);
		
		cal.set(Calendar.YEAR, year);
		cal.set(Calendar.MONTH, month);
		cal.set(Calendar.DAY_OF_MONTH, day);
		cal.set(Calendar.HOUR_OF_DAY, hour);
		cal.set(Calendar.MINUTE, minute);
		cal.set(Calendar.SECOND, second);
		cal.set(Calendar.MILLISECOND, millis);
		
		try {
			this.millis = cal.getTimeInMillis();
		}
		catch (Exception ex) {
			throw new IllegalArgumentException("The timestamp parameters were out of bounds.");
		}
	}
	
	/**
	 * Package visible constructor to create timestamps from milliseconds directly.
	 *  
	 * @param millis The milliseconds since midnight, January 1st, 1970, UTC.
	 */
	TimestampField(long millis)
	{
		this.millis = millis;
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#getDataType()
	 */
	@Override
    public BasicType getBasicType()
    {
		return BasicType.TIMESTAMP;
    }

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#isNULL()
	 */
	@Override
    public boolean isNULL()
    {
    	return this.millis == NULL_VALUE;
    }
	
	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object o)
	{
		return (o != null && o instanceof TimestampField && ((TimestampField) o).millis == this.millis); 
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode()
	{
		return ((int) this.millis) ^ ((int) (this.millis >>> 32));
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#getNumberOfBytes()
	 */
	@Override
    public int getNumberOfBytes()
    {
    	return 8;
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
			StringBuilder bld = new StringBuilder(32);
			
			GregorianCalendar cal = new GregorianCalendar(TIME_ZONE);
			cal.setTimeInMillis(this.millis);
			
			bld.append(cal.get(Calendar.YEAR)).append('-');
			bld.append(cal.get(Calendar.MONTH) + 1).append('-');
			bld.append(cal.get(Calendar.DAY_OF_MONTH)).append(' ');
			bld.append(cal.get(Calendar.HOUR_OF_DAY)).append(':');
			bld.append(cal.get(Calendar.MINUTE)).append(':');
			bld.append(cal.get(Calendar.SECOND)).append('.');
			bld.append(cal.get(Calendar.MILLISECOND));
			
			return bld.toString();
		}
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#getFromString(java.lang.String)
	 */
	@Override
    public TimestampField getFromString(String charEncoded, int len) throws DataFormatException
    {
		if (charEncoded == null || charEncoded.equalsIgnoreCase("NULL")) {
			return (TimestampField) DataType.timestampType().getNullValue();
		}
		else {
			try {
				Pattern p = Pattern.compile("^(\\d{4})-(\\d{1,2})-(\\d{1,2}) (\\d{1,2}):(\\d{2}):(\\d{2})\\.(\\d{3})$");
				Matcher m = p.matcher(charEncoded);
				if (m.matches()) {
					int year = Integer.parseInt(m.group(1));
					int month = Integer.parseInt(m.group(2));
					int day = Integer.parseInt(m.group(3));
					int hour = Integer.parseInt(m.group(4));
					int minute = Integer.parseInt(m.group(5));
					int second = Integer.parseInt(m.group(6));
					int millies = Integer.parseInt(m.group(7));
					
					return new TimestampField(day, month - 1, year, hour, minute, second, millies);
				}
				else {
					throw new DataFormatException("String did not contain a well formed timestamp.");
				}
			}
			catch (PatternSyntaxException psex) {
				throw new InternalOperationFailure
					("Timestamp regex pattern not accepted int this version!!!", false, psex);
			}
			catch (NumberFormatException nfex) {
				throw new InternalOperationFailure
					("Timestamp regex pattern captured wrong result", false, nfex);
			}
		}
    }

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#encodeBinary(byte[], int)
	 */
	@Override
	public int encodeBinary(byte[] buffer, int offset) throws ArrayIndexOutOfBoundsException
	{
	    buffer[offset]     = (byte)  this.millis;
	    buffer[offset + 1] = (byte) (this.millis >>> 8);
	    buffer[offset + 2] = (byte) (this.millis >>> 16);
	    buffer[offset + 3] = (byte) (this.millis >>> 24);
	    buffer[offset + 4] = (byte) (this.millis >>> 32);
	    buffer[offset + 5] = (byte) (this.millis >>> 40);
	    buffer[offset + 6] = (byte) (this.millis >>> 48);
	    buffer[offset + 7] = (byte) (this.millis >>> 56);
	    return 8;
	}
	
	/**
	 * Deserialized a timestamp from a binary form. Performs no checking, if this timestamp
	 * is not in a valid format.
	 * 
	 * @param binaryEncoded The binary buffer to extract the timestamp from.
	 * @param offs The offset in the buffer where to start the extraction from.
	 * @return The timestamp formed from the binary representation.
	 */
	public static TimestampField getFieldFromBinary(byte[] binaryEncoded, int offs)
	{
		long bits1 = (binaryEncoded[offs    ]        & 0x000000ffL) |
		            ((binaryEncoded[offs + 1] <<  8) & 0x0000ff00L) |
		            ((binaryEncoded[offs + 2] << 16) & 0x00ff0000L) |
		            ((binaryEncoded[offs + 3] << 24) & 0xff000000L);
		
		long bits2 = (binaryEncoded[offs + 4]        & 0x000000ffL) |
                    ((binaryEncoded[offs + 5] <<  8) & 0x0000ff00L) |
                    ((binaryEncoded[offs + 6] << 16) & 0x00ff0000L) |
                    ((binaryEncoded[offs + 7] << 24) & 0xff000000L);
		
		return new TimestampField(bits1 | (bits2 << 32));
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#getFromBinary(byte[], int, int)
	 */
	@Override
	TimestampField getFromBinary(byte[] binaryEncoded, int offs, int len)
	{
		return TimestampField.getFieldFromBinary(binaryEncoded, offs);
	}

	/* (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(DataField o)
	{
		// no type checking for performance reasons
		long otherMillis = ((TimestampField) o).millis; 
		return (this.millis < otherMillis ? -1 : (this.millis == otherMillis ? 0 : 1));
	}
}
