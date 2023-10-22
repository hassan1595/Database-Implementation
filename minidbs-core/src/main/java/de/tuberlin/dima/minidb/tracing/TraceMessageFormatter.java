package de.tuberlin.dima.minidb.tracing;


import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;


/**
 * A simple formatter to produce message log file output from log messages.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class TraceMessageFormatter extends Formatter
{
	/**
	 * The reused buffer for the message construction.
	 */
	private final StringBuilder buffer = new StringBuilder(1024);
	
	/**
	 * The reused calendar object to format the messages.
	 */
	private final GregorianCalendar calender = new GregorianCalendar();

	
	/* (non-Javadoc)
	 * @see java.util.logging.Formatter#format(java.util.logging.LogRecord)
	 */
	@Override
	public String format(LogRecord record)
	{
		// sequence number:
		this.buffer.append(record.getSequenceNumber());
		this.buffer.append('|');
		
		// append the time
		this.calender.setTimeInMillis(record.getMillis());
		
		this.buffer.append(this.calender.get(Calendar.YEAR));
		this.buffer.append('-');
		this.buffer.append(this.calender.get(Calendar.MONTH) + 1);
		this.buffer.append('-');
		this.buffer.append(this.calender.get(Calendar.DAY_OF_MONTH));
		this.buffer.append(',');
		this.buffer.append(this.calender.get(Calendar.HOUR_OF_DAY));
		this.buffer.append(':');
		this.buffer.append(this.calender.get(Calendar.MINUTE));
		this.buffer.append(':');
		this.buffer.append(this.calender.get(Calendar.SECOND));
		this.buffer.append('|');
		
		// append the level
		Level level = record.getLevel();
		if (level == Level.SEVERE) {
			this.buffer.append("ERROR");
		}
		else {
			this.buffer.append(level.getName());
		}
		
		// append the message
		this.buffer.append('|');
		this.buffer.append(record.getMessage());
		
		// append the exception, if there is one
		Throwable t = record.getThrown();
		if (t != null) {
			this.buffer.append("|[");
			StringWriter w = new StringWriter();
			t.printStackTrace(new PrintWriter(w));
			this.buffer.append(w.getBuffer());
			this.buffer.append(']');
		}
		
		this.buffer.append('\n');
		
		// return the string
		String s = this.buffer.toString();
		this.buffer.setLength(0);
		return s;
	}
}
