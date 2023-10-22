package de.tuberlin.dima.minidb.parser;


import java.util.ArrayList;
import java.util.Iterator;


/**
 * A parse tree node representing a generic predicate. It represents predicates for both the
 * WHERE clause and the HAVING clause. Predicate can occur in the form
 * <ul>
 *   <li><i>tab.col op literal</i> (WHERE clause only) </li>
 *   <li><i>tabA.colC op tabB.ColD</i> (WHERE clause only) </li>
 *   <li><i>outputCol(=alias col) op literal</i> (HAVING clause only) </li>
 * </ul>
 * 
 * The predicate has a type that declares which of the above types this predicate has. 
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 * @author Michael Saecker (changed Predicate to a more general construct supporting expressions)
 */
public class Predicate implements ParseTreeNode
{
	/**
	 * Enumeration that defines the type of a predicate.
	 */
	public enum PredicateType
	{
		/**
		 * Enumeration type representing the status that the exact type of predicate
		 * has not yet been determined.
		 */
		UNDETERMINED, 
		
		/**
		 * Predicate type for a predicate of the form <i>column &th;op&gt; column</i>.
		 * The predicate is hence a join predicate.
		 */
		COLUMN_COLUMN,
		
		/**
		 * Predicate type for a predicate of the form <i>column &th;op&gt; literal</i>.
		 * The predicate is hence a local predicate.
		 */
		COLUMN_LITERAL,
		
		/**
		 * Predicate type for a predicate of the form <i>column-alias &th;op&gt; literal</i>.
		 * The predicate is hence a predicate for the having clause.
		 */
		ALIASCOLUMN_LITERAL;
	}
	
	
	/**
	 * Enumeration to identify the type of predicate operator.
	 */
	public enum Operator
	{
		/**
		 * Type for undefined operator.
		 */
		UNDETERMINED("?"),
		
		/**
		 * Type for operator <i>equal</i> '='.
		 */
		EQUAL("="),
		
		/**
		 * Type for operator <i>unequal</i> '<>'.
		 */
		NOT_EQUAL("<>"),
		
		/**
		 * Type for operator <i>smaller</i> '<'.
		 */
		SMALLER("<"),
		
		/**
		 * Type for operator <i>smaller-or-equal</i> '<='.
		 */
		SMALLER_OR_EQUAL("<="),
		
		/**
		 * Type for operator <i>greater</i> '>'.
		 */
		GREATER(">"),
		
		/**
		 * Type for operator <i>greater-or-equal</i> '>='.
		 */
		GREATER_OR_EQUAL(">=");
		
		/**
		 * The symbol of this operator.
		 */
		private String symbol;
		
		
		/**
		 * Creates an operator with the given symbol.
		 * 
		 * @param symbol The symbol of the operator.
		 */
		private Operator(String symbol)
		{
			this.symbol = symbol;
		}
		
		/**
		 * Gets this operator's symbol.
		 * 
		 * @return The operator symbol.
		 */
		public String getSymbol()
		{
			return this.symbol;
		}
		
		/**
		 * Gets the inverted operator for this operator. E.g. a '&lt;' will become a
		 * '&gt;='.
		 * 
		 * @return The inverted operator.
		 */
		public Operator getInverted()
		{
			if (this == EQUAL) {
				return NOT_EQUAL;
			}
			if (this == NOT_EQUAL) {
				return EQUAL;
			}
			if (this == SMALLER) {
				return GREATER_OR_EQUAL;
			}
			if (this == SMALLER_OR_EQUAL) {
				return GREATER;
			}
			if (this == GREATER) {
				return SMALLER_OR_EQUAL;
			}
			if (this == GREATER_OR_EQUAL) {
				return SMALLER;
			}
			return null;
		}
		
		/**
		 * Gets the operator that applies when the left and right side are switched.
		 * 
		 * @return The operator for switched sides.
		 */
		public Operator getSideSwitched()
		{
			if (this == SMALLER) {
				return GREATER;
			}
			else if (this == GREATER) {
				return SMALLER;
			}
			else if (this == SMALLER_OR_EQUAL) {
				return GREATER_OR_EQUAL;
			}
			else if (this == GREATER_OR_EQUAL) {
				return SMALLER_OR_EQUAL;
			}
			else {
				return this;
			}
		}
	}
	
	
	/**
	 * This predicate's type.
	 */
	protected PredicateType type;
	
	/**
	 * This predicate's operator.
	 */
	protected Operator op;

	/**	
	 * The column on the left hand side.
	 */
	protected Column leftHandColumn;
	
	/**
	 * The alias column on the left hand side. 
	 */
	protected OutputColumn leftHandAliasColumn;
	
	/**
	 * The left hand side expression.
	 */
	protected ParseTreeNode leftExpr;
	
	/**
	 * The literal on the right hand side.
	 */
	protected Literal rightHandLiteral;
	
	/**
	 * The column on the right hand side.
	 */
	protected Column rightHandColumn;	
	
	/**
	 * The right hand side expression.
	 */
	protected ParseTreeNode rightExpr;
	
	
	
	/**
	 * Creates a plain empty predicate with yet undetermined type.
	 */
	public Predicate() {
		this.type = PredicateType.UNDETERMINED;
		this.op = Operator.UNDETERMINED;
	}
	
	
	/**
	 * Sets the left hand side to the given expression. Dynamically analyzes the type 
	 * of column to create the left hand side.
	 * 
	 * @param expression The expression forming the left hand side.
	 * @param col The column/output column in the left hand side.
	 */
	public void setLeftHandSide(ParseTreeNode expression, ParseTreeNode col)
	{
		if(col instanceof Column)
		{
			setLeftHandSide(expression, (Column) col);
		} else if(col instanceof OutputColumn)
		{
			setLeftHandSide(expression, (OutputColumn) col);
		} else 
		{
			throw new IllegalStateException("Left hand side of predicate contains neither a column nor an output column.");
		}
	}
	
	/**
	 * Sets the left hand side to the given expression. To be used during the construction of a 
	 * predicate in the WHERE clause.
	 * Erases previously set fields that would be inconsistent with this left hand side
 	 * (previous left hand alias column) and tries to determine the type.
	 * 
	 * @param expression The expression forming the left hand side.
	 * @param col The column in the left hand side.
	 */
	public void setLeftHandSide(ParseTreeNode expression, Column col)
	{
		this.leftExpr = expression;
		this.leftHandAliasColumn = null;
		this.leftHandColumn = col;
		determineType();
	}

	/**
	 * Sets the left hand side to the given expression. To be used for predicates in the HAVING
	 * clause. The output column can either be directly the column that is child of the select clause
	 * or a new OutputColumn object whos's alias name is only interpreted.
	 * 
	 * Erases previously set fields that would be inconsistent with this left hand side (previous left 
	 * hand column or right hand column) and tries to determine the type.
	 * 
	 * @param expression The expression forming the left hand side.
	 * @param col The alias column in the left hand side.
	 */
	public void setLeftHandSide(ParseTreeNode expression, OutputColumn col)
	{
		this.leftExpr = expression;
		this.leftHandAliasColumn = col;
		this.leftHandColumn = null;
		this.rightHandColumn = null;
		determineType();
	}
	
	/**
	 * Sets the operator for this predicate.
	 * 
	 * @param op The predicate operator.
	 */
	public void setOperator(Operator op)
	{
		this.op = op;
	}

	/**
	 * Sets the right hand side to the given expression. Dynamically analyzes the type of the expression
	 * to set the right hand side.
	 * 
	 * @param expression The expression forming the right hand side.
	 * @param col The column in the right hand side / null if no column was set
	 */
	public void setRightHandSide(ParseTreeNode expression, ParseTreeNode col)
	{
		if(col == null && expression instanceof Literal)
		{
			setRightHandSide((Literal) expression);
		} else if(col instanceof Column)
		{
			setRightHandSide(expression, (Column) col);
		} else	
		{
			throw new IllegalStateException("Right hand side of predicate contains neither a column nor is it a literal.");			
		}
	}	
	
	/**
	 * Sets the right hand side to the given expression. Erases previously
	 * set fields that would be inconsistent with this right hand side (previous right 
	 * hand column or left hand alias column) and tries to determine the type.
	 * 
	 * @param expression The expression forming the right hand side.
	 * @param col The column in the right hand side.
	 */
	public void setRightHandSide(ParseTreeNode expression, Column col)
	{
		this.rightExpr = expression;
		this.rightHandColumn = col;
		this.rightHandLiteral = null;
		this.leftHandAliasColumn = null;
		determineType();
	}
	
	/**
	 * Sets the right hand side to the given literal. Erases previously
	 * set fields that would be inconsistent with this right hand side (previous right 
	 * hand column) and tries to determine the type.
	 * 
	 * @param literal The literal forming the right hand side.
	 */
	public void setRightHandSide(Literal literal)
	{
		this.rightExpr = literal;
		this.rightHandLiteral = literal;
		this.rightHandColumn = null;
		determineType();
	}
		
	
	/**
	 * Sets the type of the predicate.
	 * 
	 * @param type The type of the predicate
	 */
	public void setType(PredicateType type)
	{
		this.type = type;
	}
	
	/**
	 * Checks whether the predicate is completely formed with all of its children.
	 * 
	 * @return true, is the predicate is complete, false otherwise.
	 */
	public boolean isComplete()
	{
		return ( (  this.type == PredicateType.ALIASCOLUMN_LITERAL &&
			    this.leftHandAliasColumn != null &&
			    this.rightHandLiteral != null ) ||
			 (  this.type == PredicateType.COLUMN_COLUMN &&
			    this.leftHandColumn != null &&
			    this.rightHandColumn != null ) ||
			 (  this.type == PredicateType.COLUMN_LITERAL &&
			    this.leftHandColumn != null &&
			    this.rightHandColumn != null ) ) &&
			    this.leftExpr != null && this.rightExpr != null &&
			    this.op != Operator.UNDETERMINED;
	}	
	
	/**
	 * Gets this predicate's type.
	 * 
	 * @return The predicate type.
	 */
	public PredicateType getType()
	{
		return this.type;
	}

	/**
	 * Gets this predicate's operator.
	 * 
	 * @return The predicate operator.
	 */
	public Operator getOp()
	{
		return this.op;
	}

	/**
	 * Gets the expression forming the left hand side.
	 * 
	 * @return The left hand side expression, or null, if not set or used.
	 */
	public ParseTreeNode getLeftHandExpression()
	{
		return this.leftExpr;
	}

	/**
	 * Gets the column forming the left hand side.
	 * 
	 * @return The left hand side column, or null, if not set or used.
	 */
	public Column getLeftHandColumn()
	{
		return this.leftHandColumn;
	}

	/**
	 * Gets the column alias forming the left hand side.
	 * 
	 * @return The left hand side column alias, or null, if not set or used.
	 */
	public OutputColumn getLeftHandAliasColumn()
	{
		return this.leftHandAliasColumn;
	}

	/**
	 * Gets the literal forming the right hand side.
	 * 
	 * @return The right hand side literal, or null, if not set or used.
	 */
	public Literal getRightHandLiteral()
	{
		return this.rightHandLiteral;
	}

	/**
	 * Gets the column forming the right hand side.
	 * 
	 * @return The right hand side column, or null, if not set or used.
	 */
	public Column getRightHandColumn()
	{
		return this.rightHandColumn;
	}


	/**
	 * Gets the expression forming the right hand side.
	 * 
	 * @return The right hand side expression, or null, if not set or used.
	 */
	public ParseTreeNode getRightHandExpression()
	{
		return this.rightExpr;
	}

//	public void determineType(boolean having, boolean rightContainsColumn) 
//	{
//		if(leftExpr != null && rightExpr != null && op != Operator.UNDETERMINED)
//		{
//			if(having)
//			{
//				type = PredicateType.ALIASCOLUMN_LITERAL;
//			} 
//			else if(rightContainsColumn)
//			{
//				type = PredicateType.COLUMN_COLUMN;
//			} else 
//			{
//				type = PredicateType.COLUMN_LITERAL;
//			}
//		}
//		else {
//			type = PredicateType.UNDETERMINED;
//		}
//		
//	}
//	
	/**
	 * Tries to determine the predicate type from the fields that have been set.
	 */
	private void determineType()
	{
		if (this.leftHandAliasColumn != null && this.leftHandColumn == null) {
			this.type = PredicateType.ALIASCOLUMN_LITERAL;
		}
		else if (this.leftHandColumn != null && this.leftHandAliasColumn == null) {
			if (this.rightHandColumn != null && this.rightHandLiteral == null) {
				this.type = PredicateType.COLUMN_COLUMN;
			} 
			else if (this.rightHandColumn == null && this.rightHandLiteral != null) {
				this.type = PredicateType.COLUMN_LITERAL;
			}
			else {
				this.type = PredicateType.UNDETERMINED;
			}
		}
		else {
			this.type = PredicateType.UNDETERMINED;
		}
	}
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#getChildren()
	 */
	@Override
	public Iterator<? extends ParseTreeNode> getChildren()
	{
		ArrayList<ParseTreeNode> list = new ArrayList<ParseTreeNode>();
		
		// add left hand side
		if (this.leftExpr != null) {
			list.add(this.leftExpr);
		}
		
		// add right hand side
		if (this.rightExpr != null) {
			list.add(this.rightExpr);
		}
		
		return list.iterator();
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#getNodeContents()
	 */
	@Override
	public String getNodeContents()
	{
		StringBuilder bld = new StringBuilder();
				
		bld.append(this.leftExpr == null ? "null" : this.leftExpr.getNodeContents());
		bld.append(' ').append(this.op == null ? "?" : this.op.getSymbol()).append(' ');
		bld.append(this.rightExpr == null ? "null" : this.rightExpr.getNodeContents());
		
		return bld.toString();
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#getNodeName()
	 */
	@Override
	public String getNodeName()
	{
		return "PREDICATE";
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#getNumberOfChildren()
	 */
	@Override
	public int getNumberOfChildren()
	{
		int children = 0;
		
		children += this.leftExpr == null ? 0 : 1;
		children += this.rightExpr == null ? 0 : 1;
		
		return children;
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#isIdenticalTo(de.tuberlin.dima.minidb.parser.ParseTreeNode)
	 */
	@Override
	public boolean isIdenticalTo(ParseTreeNode node)
	{
		if (node != null && node instanceof Predicate)
		{
			Predicate other = (Predicate) node;
			
			return ( (this.leftExpr == null && other.leftExpr == null) ||
					 (this.leftExpr != null && other.leftExpr != null &&
					  this.leftExpr.isIdenticalTo(other.leftExpr))
				   ) &&
				   ( (this.rightExpr == null && other.rightExpr == null) ||
					 (this.rightExpr != null && other.rightExpr != null &&
					  this.rightExpr.isIdenticalTo(other.rightExpr))
				   ) && (this.op == other.op);
		}
		
		return false;
	}
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) 
	{
		return (obj instanceof ParseTreeNode) && this.isIdenticalTo((ParseTreeNode) obj);
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		return getNodeContents();
	}
}
