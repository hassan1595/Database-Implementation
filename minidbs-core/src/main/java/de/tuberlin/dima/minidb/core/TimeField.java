package de.tuberlin.dima.minidb.core;


import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;


/**
 * The representation of a time field. The time is coded into a 64 bit
 * integer the following way:
 * 
 * - The less significant 32 bits hold the number of milliseconds since last midnight that
 *   represent the time. A value of 0 is midnight.
 *   
 * - The more significant 32 bits hold the time offset to the equivalent UTC time in
 *   milliseconds. It hence represents the time zone.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public final class TimeField extends DataField
{
	/**
	 * A serial version UID to support object serialization.
	 */
	private static final long serialVersionUID = 8765411635812059726L;

	/**
	 * The time value representing a NULL value.
	 */
	static final long NULL_VALUE = 0xffffffffffffffffL;
	
	/**
	 * The hosts default time zone.
	 */
	private static final TimeZone TIME_ZONE = TimeZone.getDefault();
	
	/**
	 * The time, encoded as described in the class specification. 
	 */
	private long time;

	
	/**
	 * Creates a new Time field for the given hour, minute and second. The constructed
	 * time is interpreted to be in the hosts default time zone.
	 * 
	 * The constructor checks, if the fields form a valid time.
	 * 
	 * @param hour The hour in 24h mode, 0 - 23.
	 * @param minute The minute, 0 - 59.
	 * @param second The second, 0 - 59.
	 * @throws DataFormatException Thrown when a parameter is not within its valid range.
	 */
	public TimeField(int hour, int minute, int second)
	throws DataFormatException
	{
		this(hour, minute, second, TIME_ZONE.getRawOffset());
	}
	
	/**
	 * Creates a new Time field for the given hour, minute and second. The time is
	 * interpreted to be in the time zone as indicated by the otcOffset, which is
	 * the number of milliseconds that the time zone varies from the UTC.
	 * 
	 * The constructor checks, if the fields form a valid time.
	 * 
	 * @param hour The hour in 24h mode, 0 - 23.
	 * @param minute The minute, 0 - 59.
	 * @param second The second, 0 - 59.
	 * @param utcOffset the number of milliseconds that the time zone deviates from the UTC.
	 * @throws DataFormatException Thrown when a parameter is not within its valid range.
	 */
	public TimeField(int hour, int minute, int second, int utcOffset)
	throws DataFormatException
	{
		if (hour < 0 || hour > 23 || minute < 0 || minute > 59 || second < 0 || second > 59
				|| utcOffset < -43200000 || utcOffset > 432000000)
		{
			throw new DataFormatException("Time parameters are out of range.");
		}
		
		int millis = (((hour) * 60 + minute) * 60 + second) * 1000;
		
		this.time = (((long) utcOffset) << 32) | millis;
	}
	
	/**
	 * Constructs a new time with the given encoded value. No checking is done.
	 * This constructor is for package level use only.
	 * 
	 * @param timeValue The encoded time.
	 */
	TimeField(long timeValue)
	{
		this.time = timeValue;
	}
	
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#getDataType()
	 */
	@Override
    public BasicType getBasicType()
    {
		return BasicType.TIME;
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
	 * @see de.tuberlin.dima.minidb.core.DataField#isNULL()
	 */
	@Override
    public boolean isNULL()
    {
		return this.time == NULL_VALUE;
    }
	
	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object o)
	{
		return (o != null && o instanceof TimeField && ((TimeField) o).compareTo(this) == 0); 
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode()
	{
		long thisAbsolute = getTimeAsMillis() - getUTCOffset();
		return ((int) thisAbsolute) ^ ((int) (thisAbsolute >>> 32));
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#encodeAsString()
	 */
	@Override
	public String encodeAsString()
	{
		// check for NULL value
		if (isNULL()) {
			return "NULL";
		}
		else {
			// print the time
			StringBuilder bld = new StringBuilder(11);
			bld.append(getHour());
			bld.append(':');
			bld.append(getMinute());
			bld.append(':');
			bld.append(getSecond());
			
			// check for timezone offset
			int offs = (int) (this.time >> 32);
			if (offs != 0)
			{
				bld.append(" (UTC ");
				
				if (offs < 0) {
					bld.append('-');
					offs *= -1;
				} else {
					bld.append('+');
				}
				
				int hours = offs / (60*60*1000);
				int minutes = (offs / (60*1000)) % 60;
				int seconds = (offs / 1000) % 60;
				
				bld.append(hours);
				if (minutes > 0 || seconds > 0) {
					bld.append(':').append(minutes);
					if (seconds > 0) {
						bld.append(':').append(seconds);
					}
				}
				
				bld.append(')');
			}
			
			return bld.toString();
		}
	}
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#getFromString(java.lang.String)
	 */
	@Override
	public TimeField getFromString(String charEncoded, int len) throws DataFormatException
	{
		if (charEncoded == null || charEncoded.equalsIgnoreCase("NULL")) {
			return (TimeField) DataType.timeType().getNullValue();
		}
		else {
			try {
				int offset = 0;
				int pos = charEncoded.indexOf(' ');
				if (pos != -1) {
					// we have offset and time
					String offsetString = charEncoded.substring(pos + 1);
					charEncoded = charEncoded.substring(0, pos);
					
					// parse the time zone
					Pattern pattern = Pattern.compile("^\\(UTC (\\+|\\-)(\\d{1,2})(:(\\d{2}))?(:(\\d{2}))?\\)$");
					Matcher matcher = pattern.matcher(offsetString);
					
					if (matcher.matches()) {
						int h = 0, m = 0, s = 0, sig = 1;

						sig = matcher.group(1).equals("+") ? 1 : -1;
						h = Integer.parseInt(matcher.group(2));

						if (matcher.group(3) != null) {
							m = Integer.parseInt(matcher.group(4));
							if (matcher.group(5) != null) {
								s = Integer.parseInt(matcher.group(6));
							}
						}
						else if (matcher.group(5) != null) {
							throw new InternalOperationFailure
							("Time regex pattern incorrect!!!", false, null);
						}
						
						offset = (((h) * 60 + m) * 60 + s) * 1000 * sig;
					}
					else {
						throw new DataFormatException
						("Invalid time zone format. Expected Format: (UTC +/-HH:MM:SS)");
					}
					
				}
				
				// parse the regular time
				Pattern tp = Pattern.compile("^(\\d{1,2}):(\\d{2}):(\\d{2})$");
				Matcher tm = tp.matcher(charEncoded);
				if (tm.matches()) {
					int hour = Integer.parseInt(tm.group(1));
					int minute = Integer.parseInt(tm.group(2));
					int second = Integer.parseInt(tm.group(3));
					
					return new TimeField(hour, minute, second, offset);
				}
			}
			catch (IndexOutOfBoundsException iobex) {
				throw new InternalOperationFailure
				    ("Time parsing logic incorrect!!!", false, iobex);
			}
			catch (PatternSyntaxException psex) {
				throw new InternalOperationFailure
					("Time regex pattern not accepted int this version!!!", false, psex);
			}
			catch (NumberFormatException nfex) {
				throw new InternalOperationFailure
					("Time regex pattern captured wrong result", false, nfex);
			}

			// if we get here, we had a parse error
			throw new DataFormatException("Invalid time format. Expected Format: hh:mm:ss [(UTC +/-HH:MM:SS)]");
		}
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#encodeBinary(byte[], int)
	 */
	@Override
	public int encodeBinary(byte[] buffer, int offset) throws ArrayIndexOutOfBoundsException
	{
		buffer[offset    ] = (byte) (this.time       );
		buffer[offset + 1] = (byte) (this.time >>>  8);
		buffer[offset + 2] = (byte) (this.time >>> 16);
		buffer[offset + 3] = (byte) (this.time >>> 24);
		buffer[offset + 4] = (byte) (this.time >>> 32);
		buffer[offset + 5] = (byte) (this.time >>> 40);
		buffer[offset + 6] = (byte) (this.time >>> 48);
		buffer[offset + 7] = (byte) (this.time >>> 56);
		return 8;
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.core.DataField#getFromBinary(byte[], int, int)
	 */
	@Override
	TimeField getFromBinary(byte[] binaryEncoded, int offs, int len)
	{
		return TimeField.getFieldFromBinary(binaryEncoded, offs);
	}
	
	/**
	 * Deserialized a time from a binary form. Performs no checking, if this time
	 * is not in a valid format.
	 * 
	 * @param binaryEncoded The binary buffer to extract the date from.
	 * @param offs The offset in the buffer where to start the extraction from.
	 * @return The Time formed from the binary representation.
	 */
	public static TimeField getFieldFromBinary(byte[] binaryEncoded, int offs)
	{
		long bits1 = (binaryEncoded[offs    ]        & 0x000000ff) |
		            ((binaryEncoded[offs + 1] <<  8) & 0x0000ff00) |
		            ((binaryEncoded[offs + 2] << 16) & 0x00ff0000) |
		            ((binaryEncoded[offs + 3] << 24) & 0xff000000);
		
		long bits2 = (binaryEncoded[offs + 4]        & 0x000000ff) |
                    ((binaryEncoded[offs + 5] <<  8) & 0x0000ff00) |
                    ((binaryEncoded[offs + 6] << 16) & 0x00ff0000) |
                    ((binaryEncoded[offs + 7] << 24) & 0xff000000);
		
		return new TimeField( bits1 | (bits2 << 32) );
	}
	
	/**
	 * Extracts the hour from the time.
	 * 
	 * @return The time's hour.
	 */
	public int getHour()
	{
		return ((int) this.time) / (60*60*1000); 
	}
	
	/**
	 * Extracts the minute from the time.
	 * 
	 * @return The time's minute.
	 */
	public int getMinute()
	{
		int minutes = ((int) this.time) / (60*1000);
		return minutes % 60;
	}
	
	/**
	 * Extracts the second from the time.
	 * 
	 * @return The time's second.
	 */
	public int getSecond()
	{
		int seconds = ((int) this.time) / 1000;
		return seconds % 60;
	}
	
	/**
	 * Gets the time as the milliseconds since midnight in the times 
	 * time zone.
	 * 
	 * @return The milliseconds since midnight.
	 */
	public int getTimeAsMillis()
	{
		return (int) this.time;
	}
	
	/**
	 * Gets the milliseconds deviation from UTC for the time's timezone.
	 * 
	 * @return The milliseconds deviation from UTC.
	 */
	public int getUTCOffset()
	{
		return (int) (this.time >>> 32);
	}

	/* (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(DataField o)
	{
		// no type checking for performance reasons
		TimeField other = (TimeField) o;
		if (isNULL()) {
			return other.isNULL() ? 0 : -1;
		}
		else if (other.isNULL()) {
			return 1;
		}
		else {
			// calculate time millis - offset
			long thisAbsolute = getTimeAsMillis() - getUTCOffset();
			long otherAbsolute = other.getTimeAsMillis() - other.getUTCOffset();
			return (thisAbsolute < otherAbsolute ? -1 : (thisAbsolute == otherAbsolute ? 0 : 1));
		}
	}
}
