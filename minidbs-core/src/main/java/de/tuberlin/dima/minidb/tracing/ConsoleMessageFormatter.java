package de.tuberlin.dima.minidb.tracing;


import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;


/**
 * A simple formatter to produce console output from log messages.
 * 
 * @author Stephan Ewen (stephan.ewen@ctu-berlin.de)
 */
public class ConsoleMessageFormatter extends Formatter
{
	/**
	 * The reused buffer for the message construction.
	 */
	private final StringBuilder buffer = new StringBuilder(1024);

	
	/* (non-Javadoc)
	 * @see java.util.logging.Formatter#format(java.util.logging.LogRecord)
	 */
	@Override
	public String format(LogRecord record)
	{
		Level level = record.getLevel();
		if (level == Level.SEVERE) {
			this.buffer.append("ERROR - ");
		}
		else {
			this.buffer.append(level.getName()).append(" - ");
		}
		this.buffer.append(record.getMessage());
		this.buffer.append('\n');
		
		String s = this.buffer.toString();
		this.buffer.setLength(0);
		return s;
	}
}
