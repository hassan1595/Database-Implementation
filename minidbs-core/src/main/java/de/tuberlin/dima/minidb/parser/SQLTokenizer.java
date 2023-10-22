package de.tuberlin.dima.minidb.parser;


import de.tuberlin.dima.minidb.parser.ParseException;
import de.tuberlin.dima.minidb.parser.Token;
import de.tuberlin.dima.minidb.parser.ParseException.ErrorCode;
import de.tuberlin.dima.minidb.parser.Token.TokenType;


public class SQLTokenizer
{
	/**
	 * Status indicator constant for being outside a literal or identifier.
	 */
	private static final int STATUS_NOT_DETERMINED = 0;
	
	/**
	 * Status indicator constant for being inside a quoted literal.
	 */
	private static final int STATUS_INSIDE_LITERAL = 1;
	
	/**
	 * Status indicator constant for being inside an keyword or identifier.
	 */
	private static final int STATUS_INSIDE_WORD = 2;
	
	/**
	 * Status indicator constant for being inside an operand (=, <, >=, ...).
	 */
	private static final int STATUS_INSIDE_OPERAND = 3;

	/**
	 * Status indicator constant for being inside another construct.
	 */
	private static final int STATUS_INSIDE_NUMBER = 4;
	
	
	/**
	 * The string to be tokenized.
	 */
	protected String string;
	
	/**
	 * The current position in the string.
	 */
	protected int position;
	
	/**
	 * The last token that the tokenizer has parsed.
	 */
	protected Token lastToken;
	
	/**
	 * The start position of the last token.
	 */
	protected int lastPosition;
	
	/**
	 * The part of the token that we currently extract.
	 */
	private StringBuilder currentToken = new StringBuilder(16);
	
	/**
	 * Cached length of the string.
	 */
	private int len;
	
	
	
	
	/**
	 * Sets up a tokenizer for the given string.
	 * 
	 * @param statement The SQL statement to be tokenized.
	 */
	public SQLTokenizer(String statement)
	{
		this.string = statement;
		this.position = 0;
		this.len = statement.length();
	}
	
	
	/**
	 * Returns the next token without advancing the parsers internal position.
	 * 
	 * @return The next token
	 * @throws ParseException Thrown, if an error occurs, such as encountering
	 * 					an invalid character at the wrong space.
	 */
	public Token peek() throws ParseException
	{
		// save current state of the tokenizer
		int sPosition = this.position;
		Token sLastToken = this.lastToken;
		int sLastPosition = this.lastPosition;
		StringBuilder sCurrentToken = new StringBuilder(16);
		
		// get the next token
		Token nextToken = this.nextToken();
		
		// return the tokenizer to its past state
		this.position = sPosition;
		this.lastPosition = sLastPosition;
		this.lastToken = sLastToken;
		this.currentToken.setLength(0);
		this.currentToken.append(sCurrentToken.toString());
		
		return nextToken;
	}
	
	/**
	 * Grabs the next token from the parsed string. This method always returns a token.
	 * If the statement is exhausted, then the token <code>END_OF_STATEMENT</code> is returned.
	 * 
	 * @return The next token.
	 * @throws ParseException Thrown, if an error occurs, such as encountering
	 *                  an invalid character at the wrong space. 
	 */
	public Token nextToken() throws ParseException
	{
		int status = STATUS_NOT_DETERMINED;
		char c = 0;
		char previous;
		
		this.lastPosition = this.position;
		this.currentToken.setLength(0);
		
		// skip white-spaces
		while (this.position < this.len && Character.isWhitespace(this.string.charAt(this.position)))
		{
			this.position++;
		}
		
		while (this.position < this.len)
		{
			previous = c;
			c = this.string.charAt(this.position++);
			
			// quotes
			switch (status)
			{
			case STATUS_NOT_DETERMINED:
				if (c == '.') {	
					return token(Token.TokenType.PERIOD);
				}
				else if (c == ',') {
					return token(Token.TokenType.COMMA);
				}
				else if (c == '(') {
					return token(Token.TokenType.PARENTHESIS_OPEN);
				}
				else if (c == ')') {
					return token(Token.TokenType.PARENTHESIS_CLOSE);
				}
				else if (c == '+') {
					return token(Token.TokenType.PLUS);
				}
				else if (c == '-' && (this.lastToken.type == TokenType.REAL_NUMBER ||  
						this.lastToken.type == TokenType.INTEGER_NUMBER || this.lastToken.type == TokenType.IDENTIFIER || 
						this.lastToken.type == TokenType.PARENTHESIS_CLOSE )) { // minus is ambiguous for negative numbers and the numerical operator, therefore we examine the last token
					return token(Token.TokenType.MINUS);
				}
				else if (c == '*') {
					return token(Token.TokenType.MUL);
				}
				else if (c == '/') {
					return token(Token.TokenType.DIV);
				}
				else if (c == '=' || c == '>' || c == '<') {
					status = STATUS_INSIDE_OPERAND;
				}
				else if (c == '"') {
					status = STATUS_INSIDE_LITERAL; 
				}
				else if (c =='-' || Character.isDigit(c)) { 
					status = STATUS_INSIDE_NUMBER;
					this.currentToken.append(c);
				}
				else if (Character.isUnicodeIdentifierStart(c)) {
					status = STATUS_INSIDE_WORD;
					this.currentToken.append(c);
				}
				else {
					throw new ParseException(this.string, ErrorCode.INVALID_CHARACTER,
							this.lastPosition, "" + c);
				}
				break;
				
			case STATUS_INSIDE_LITERAL:
				if (c == '"' && previous != '\\') {
					// found end of quoted literal
					return token(Token.TokenType.LITERAL, this.currentToken.toString());
				}
				else if (c != '\\') {
					this.currentToken.append(c);
				}
				break;
				
			case STATUS_INSIDE_WORD:
				if (Character.isUnicodeIdentifierPart(c)) {
					this.currentToken.append(c);
				}
				else {
					// found end of identifier
					this.position--;
					return token(getTokenForIdentifier(this.currentToken.toString()));
				}
				break;
				
			case STATUS_INSIDE_NUMBER:
				if (Character.isDigit(c)) {
					this.currentToken.append(c);
				}
				else if (c == '.') {
					// see if we had a period before.
					if (this.currentToken.indexOf(".") != -1) {
						// we had a period before
						throw new ParseException(this.string, ErrorCode.INVALID_NUMBER,
								this.lastPosition, this.currentToken.append(c).toString());
					} else {
						this.currentToken.append(c);
					}
				}
				else {
					// end of number
					this.position--;
					return token(getNumberToken(this.currentToken.toString()));
				}
				break;
				
			case STATUS_INSIDE_OPERAND:
				if (c == '=' || c == '>' || c == '<') {
					// two character operand
					return token(getTokenForOperand(previous, c));
				}
				else {
					// single character operand
					this.position--;
					return token(getTokenForOperand(previous));
				}
				
			default:
				throw new IllegalStateException
					("SQL Parser has reached an illegal internal state.");
			}
			
		}
		
		// we get here, when we reach the end of the string
		switch (status)
		{
		case STATUS_NOT_DETERMINED:
			if (this.currentToken.length() > 0) {
				throw new IllegalStateException
						("SQL Parser has reached an illegal internal state.");
			}
			return token(TokenType.END_OF_STATEMENT);
		
		case STATUS_INSIDE_OPERAND:
			return token(getTokenForOperand(c)); // can operator be the last token in a string? why not throw an exception?
			
		case STATUS_INSIDE_LITERAL:
			throw new ParseException(this.string, ErrorCode.UNCLOSED_LITERAL, this.lastPosition);
		
		case STATUS_INSIDE_WORD:
			return token(getTokenForIdentifier(this.currentToken.toString()));
		
		case STATUS_INSIDE_NUMBER:
			return token(getNumberToken(this.currentToken.toString()));
			
		default:
			throw new IllegalStateException
					("SQL Parser has reached an illegal internal state.");
		}
	}
	
	/**
	 * Gets the last parsed token
	 * 
	 * @return The last token.
	 */
	public Token getLastToken()
	{
		return this.lastToken;
	}


	/**
	 * Gets the current position of the tokenizer.
	 * 
	 * @return The position of the next token.
	 */
	public int getCurrentPosition()
	{
		return this.position;
	}
	
	/**
	 * Gets the position of the last parsed token.
	 * 
	 * @return The position of the last token.
	 */
	public int getLastPosition()
	{
		return this.lastPosition;
	}
	
	/**
	 * Creates and returns a token for the given type and contents.
	 * Also sets the last token to that new token.
	 * 
	 * @param type The type for the new token.
	 * @param contents The contents of the new token.
	 * @return The new token.
	 */
	private Token token(Token.TokenType type, String contents)
	{
		Token t = contents == null ? new Token(type) : new Token(type, contents);
		this.lastToken = t;
		return t;
	}
	
	/**
	 * Creates and returns a token for the given type.
	 * Also sets the last token to that new token.
	 * 
	 * @param type The type for the new token.
	 * @return The new token.
	 */
	private Token token(Token.TokenType type)
	{
		return token(type, null);
	}
	
	/**
	 * Sets the last token to that new token.
	 * 
	 * @param type The new token.
	 * @return The new token.
	 */
	private Token token(Token newToken)
	{
		this.lastToken = newToken;
		return newToken;
	}
	
	/**
	 * Gets the token associated with the given string. Will return any type of
	 * keyword token (such as <code>SELECT</code> or <code>AND</code>) or
	 * an <code>EMPTY</code> token or <code>IDENTIFIER</code> token.
	 * 
	 * @param str The string to determine the token for.
	 * @return The token that describes the string.
	 */
	private final Token getTokenForIdentifier(String str)
	{
		if (str == null || str.length() == 0) {
			return new Token(Token.TokenType.EMPTY);
		}
		else if (str.equalsIgnoreCase("SELECT")) {
			return new Token(Token.TokenType.SELECT);
		}
		else if (str.equalsIgnoreCase("FROM")) {
			return new Token(Token.TokenType.FROM);
		}
		else if (str.equalsIgnoreCase("WHERE")) {
			return new Token(Token.TokenType.WHERE);
		}
		else if (str.equalsIgnoreCase("GROUP")) {
			return new Token(Token.TokenType.GROUP);
		}
		else if (str.equalsIgnoreCase("HAVING")) {
			return new Token(Token.TokenType.HAVING);
		}
		else if (str.equalsIgnoreCase("ORDER")) {
			return new Token(Token.TokenType.ORDER);
		}
		else if (str.equalsIgnoreCase("BY")) {
			return new Token(Token.TokenType.BY);
		}
		else if (str.equalsIgnoreCase("INSERT")) {
			return new Token(Token.TokenType.INSERT);
		}
		else if (str.equalsIgnoreCase("INTO")) {
			return new Token(Token.TokenType.INTO);
		}
		else if (str.equalsIgnoreCase("UPDATE")) {
			return new Token(Token.TokenType.UPDATE);
		}
		else if (str.equalsIgnoreCase("SET")) {
			return new Token(Token.TokenType.SET);
		}
		else if (str.equalsIgnoreCase("DELETE")) {
			return new Token(Token.TokenType.DELETE);
		}
		else if (str.equalsIgnoreCase("AS")) {
			return new Token(Token.TokenType.AS);
		}
		else if (str.equalsIgnoreCase("AND")) {
			return new Token(Token.TokenType.AND);
		}
		else if (str.equalsIgnoreCase("ASC")||str.equalsIgnoreCase("ASCENDING")) {
			return new Token(Token.TokenType.ASCENDING);
		}
		else if (str.equalsIgnoreCase("DESC")||str.equalsIgnoreCase("DESCENDING")) {
			return new Token(Token.TokenType.DESCENDING);
		}
		else if (str.equalsIgnoreCase("COUNT")) {
			return new Token(Token.TokenType.AGG_COUNT);
		}
		else if (str.equalsIgnoreCase("SUM")) {
			return new Token(Token.TokenType.AGG_SUM);
		}
		else if (str.equalsIgnoreCase("AVG")) {
			return new Token(Token.TokenType.AGG_AVG);
		}
		else if (str.equalsIgnoreCase("MIN")) {
			return new Token(Token.TokenType.AGG_MIN);
		}
		else if (str.equalsIgnoreCase("MAX")) {
			return new Token(Token.TokenType.AGG_MAX);
		}
		else if (str.equalsIgnoreCase("VALUES")) {
			return new Token(Token.TokenType.VALUES);
		}
		else {
			return new Token(Token.TokenType.IDENTIFIER, str);
		}
	}
	
	/**
	 * Gets the operand <code>Token</code> for the given operand that is
	 * made up by the single given character.
	 *  
	 * @param first The character making up the operand.
	 * @return The token that describes the operand.
	 * @throws IllegalStateException If the given character does not describe an
	 *                               operand. 
	 */
	private final Token getTokenForOperand(char first)
	{
		if (first == '=') {
			return new Token(Token.TokenType.OPERAND_EQUAL, "=");
		}
		else if (first == '<') {
			return new Token(Token.TokenType.OPERAND_SMALLER_THAN, "<");
		}
		else if (first == '>') {
			return new Token(Token.TokenType.OPERAND_GREATER_THAN, ">");
		}
		else {
			throw new IllegalStateException
					("SQL Parser has reached an illegal internal state.");
		}
	}
	
	
	/**
	 * Gets the operand <code>Token</code> for the given operand that is
	 * made up by the two characters.
	 * 
	 * @param first The first character making up the operand.
	 * @param second The second character making up the operand.
	 * @return The token that describes the operand.
	 * @throws ParseException If the given characters do not describe an
	 *                        operand. 
	 */
	private final Token getTokenForOperand(char first, char second)
	throws ParseException
	{
		if (first == '<') {
			if (second == '=') {
				return new Token(Token.TokenType.OPERAND_SMALLER_EQUAL, "<=");
			}
			else if (second == '>') {
				return new Token(Token.TokenType.OPERAND_UNEQUAL, "<>");
			}
		}
		else if (first == '>' && second == '=') {
			return new Token(Token.TokenType.OPERAND_GREATER_EQUAL, ">=");
		}
		
		throw new ParseException(this.string, ErrorCode.INVALID_OPERAND,
										this.position-2, "" + first + second);
	}
	
	
	/**
	 * Determines the numerical <code>Token<code> for this string. The token is
	 * either of type <code>INTEGER_NUMBER</code> or <code>REAL_NUMBER</code>.
	 * 
	 * @param str The string to be analyzed.
	 * @return The token for the string.
	 * @throws ParseException If the string does not correspond to a number.
	 */
	private final Token getNumberToken(String str)
	throws ParseException
	{
		try {
			// first, try if the string is an integer
			Long.parseLong(str);
			// success, return the token
			return new Token(Token.TokenType.INTEGER_NUMBER, str);
		}
		catch (NumberFormatException nfex) {}
		
		// now, see if the string is a real
		try {
			Double.parseDouble(str);
			// success this time
			return new Token(Token.TokenType.REAL_NUMBER, str);
		}
		catch (NumberFormatException nfex) {
			throw new ParseException(this.string, ErrorCode.INVALID_NUMBER,
					this.position - 1, str);
		}
	}
}
