package de.tuberlin.dima.minidb.parser;


/**
 * A class that represents an error that occurred during the parsing of a string.
 * 
 * A parse error is characterized by the statement that was parsed, an error code,
 * the position where the error occurred and the token that could not be parsed.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class ParseException extends Exception
{
	/**
	 * A default serialVersionUID for serialization support. 
	 */
	private static final long serialVersionUID = 3108265411077059985L;
	
	
	/* 
	 * --------------------------------------------------------------------
	 *                                 Error Codes
	 * --------------------------------------------------------------------
	 */

	/**
	 * An enumeration of possible error codes.
	 */
	public static enum ErrorCode
	{
		NONE("No error has occured during parsing."),
		UNKNOWN_ERROR("An parse error of unknown type has occurred."),
		
		INVALID_CHARACTER("An invalid character has been encountered."),
		INVALID_OPERAND("And invalid operand has been encountered."),
		INVALID_NUMBER("An invalid prefix of a number has been encountered."),
		UNCLOSED_LITERAL("A literal has not been closed before the end."),
		
		INVALID_TOKEN("An invalid token was encountered."),
		INVALID_COLUMN_FORMAT("The format of the column is not valid."),
		INVALID_PREDICATE_FORMAT("The format of the predicate is not valid."),
		INVALID_CLAUSE("The string contains an unrecognized clause."),
		UNKNOWN_KEYWORD("An unknown keyword has been encountered.");
		
		/**
		 * The error message associated with this code. 
		 */
		private String message;
		
		
		/**
		 * Constructs a new ErrorCode with the given string as textual message.
		 * 
		 * @param message The message for the ErrorCode.
		 */
		private ErrorCode(String message) {
			this.message = message;
		}
		
		/**
		 * Returns the error message that corresponds to this ErrorCode.
		 * 
		 * @return The message for this ErrorCode.
		 */
		public String getErrorMessage() {
			return this.message;
		}
	}
	
	/**
	 * Returns the error message that corresponds to the given error code.
	 * 
	 * @param code The error code to translate to a message.
	 * @return The message for the given ErrorCode.
	 */
	public static String getErrorMessage(ErrorCode code)
	{
		return code.getErrorMessage();
	}
	
	
	/* 
	 * --------------------------------------------------------------------
	 *                                  Exception
	 * --------------------------------------------------------------------
	 */
	
	/**
	 * The statement that rose the error when it was parsed.
	 */
	protected String statement;
	
	/**
	 * An additional informational token, like e.g. a keyword that was not
	 * recognized.
	 */
	protected String token;
	
	/**
	 * The token that was expected at a position, if known.
	 */
	protected String expected;
	
	/**
	 * The error code, describing the kind of error that arose.
	 */
	protected ErrorCode errorcode;
	
	/**
	 * The position in the statement at which the parse error occurred:
	 */
	protected int position;
	
	
	
	/**
	 * Default constructor with no additional information.
	 */
	public ParseException()
	{
		super();
		
		// we don't know anything
		this.errorcode = ErrorCode.UNKNOWN_ERROR;
		this.position = -1;
	}

	/**
	 * Create a ParseException for the given statement and the given error.
	 * 
	 * @param statement The statement to be parsed.
	 * @param code The ErrorCode that arose.
	 */
	public ParseException(String statement, ErrorCode code)
	{
		this(statement, code, -1, null, null);
	}
	
	/**
	 * Create a ParseException for the given statement and the given error and position.
	 * 
	 * @param statement The statement to be parsed.
	 * @param code The ErrorCode that arose.
	 * @param position The position where the parse error occurred.
	 */
	public ParseException(String statement, ErrorCode code, int position)
	{
		this (statement, code, position, null, null);
	}
	
	/**
	 * Create a ParseException for the given statement and the given error, position
	 * and token.
	 * 
	 * @param statement The statement to be parsed.
	 * @param code The ErrorCode that arose.
	 * @param position The position where the parse error occurred.
	 * @param token The token that caused the parse error.
	 */
	public ParseException(String statement, ErrorCode code, int position, String token)
	{
		this(statement, code, position, token, null);
	}
	
	/**
	 * Create a ParseException for the given statement and the given error, position,
	 * token and expected token
	 * 
	 * @param statement The statement to be parsed.
	 * @param code The ErrorCode that arose.
	 * @param position The position where the parse error occurred.
	 * @param token The token that caused the parse error.
	 * @param expected The expected token.
	 */
	public ParseException(String statement, ErrorCode code, int position,
			              String token, String expected)
	{
		super();
		
		this.statement = statement;
		this.errorcode = code == null ? ErrorCode.UNKNOWN_ERROR : code;
		this.position = position;
		this.token = token;
		this.expected = expected;
	}

	
	/* (non-Javadoc)
	 * @see java.lang.Throwable#getMessage()
	 */
	@Override
	public String getMessage()
	{
		StringBuilder message = new StringBuilder(256);

		if (this.statement != null && this.position >= 0 && this.position < this.statement.length())
		{
			message.append("A parse error occured at position ").append(this.position);
			message.append(":\n").append(this.statement, 0, this.position).append(" [>>");
			message.append(this.statement, this.position, this.statement.length()).append("\nError: ");
			message.append(this.errorcode.getErrorMessage()).append('\n');
		}
		else if (this.statement != null) {
			message.append("An error occured while parsing '").append(this.statement);
			message.append("'\nError: ").append(this.errorcode.getErrorMessage()).append('\n');
		}
		else {
			message.append("An error occurred during parsing: " + this.errorcode.getErrorMessage());
		}
		
		if (this.token != null) {
			message.append("The following token caused the error: ").append(this.token);
			message.append('\n');
		}
		if (this.expected != null) {
			message.append("Expected tokens include: [").append(this.expected).append("]\n");
		}
		
		return message.toString();
	}
	
	/**
	 * Returns the statement that provoked the parse error.
	 * 
	 * @return The statement that was parsed.
	 */
	public String getStatement()
	{
		return this.statement;
	}

	/**
	 * Gets the token associated with the parse error.
	 * 
	 * @return The associated token, or null, if no such token is given.
	 */
	public String getToken()
	{
		return this.token;
	}
	
	/**
	 * Gets the expected token assicoated with this parse error.
	 * 
	 * @return The expected token, or null, if no such token is given.
	 */
	public String getExpectedToken()
	{
		return this.expected;
	}

	/**
	 * Gets the ErrorCode for this Exception.
	 * 
	 * @return The ErrorCode for this Exception
	 */
	public ErrorCode getErrorcode()
	{
		return this.errorcode;
	}

	/**
	 * Gets the position in the statement where the parse error occurred.
	 *  
	 * @return The error position, or -1, if the position is not known.
	 */
	public int getPosition()
	{
		return this.position;
	}

	
	/* (non-Javadoc)
	 * @see java.lang.Throwable#toString()
	 */
	@Override
	public String toString()
	{
		return "ParseError:\nErrorCode: " + this.errorcode.name() + "\nStatement: " + this.statement +
		       "\nPosition: " + this.position + "\nToken: " + this.token + "\n";
	}
}
