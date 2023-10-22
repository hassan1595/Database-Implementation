package de.tuberlin.dima.minidb.parser;


import java.util.ArrayList;
import java.util.Iterator;


public class Expression implements ParseTreeNode
{
	/**
	 * Enumeration to identify the type of predicate operator.
	 */
	public enum NumericalOperator
	{
		UNDETERMINED("?"),
		
		/**
		 * Type for operator <i>addition</i> '+'.
		 */
		PLUS("+"),
		
		/**
		 * Type for operator <i>subtraction</i> '-'.
		 */
		MINUS("-"),
		
		/**
		 * Type for operator <i>multiplication</i> '*'.
		 */
		MUL("*"),
		
		/**
		 * Type for operator <i>division</i> '/'.
		 */
		DIV("/");
		
		/**
		 * The symbol of this operator.
		 */
		private String symbol;
		
		/**
		 * Creates an operator with the given symbol.
		 * 
		 * @param symbol The symbol of the operator.
		 */
		private NumericalOperator(String symbol)
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
		 * Returns whether the operator is commutative.
		 * 
		 * @return true, if operator is either plus or mul
		 */
		public boolean isCommutative()
		{	
			return (this == PLUS || this == MUL);	
		}
	}
	
	// ------------------------------------------------------------------------

	
	/**
	 * Left branch of the expression tree.
	 */
	protected ParseTreeNode left;
	
	/**
	 * Right branch of the expression tree.
	 */
	protected ParseTreeNode right;
	
	/**
	 * Operator combining the left and right tree branches.
	 */
	protected NumericalOperator op; 
	

	
	/**
	 * Creates an expression without initializing its values.
	 */
	public Expression()
	{	
		this.left = null;
		this.op = null;
		this.right = null;		
	}
	
	/**
	 * Creates an expression with the given parameters
	 * 
	 * @param left The left branch of the expression.
	 * @param right The right branch of the expression.
	 * @param op The numerical operator of the expression.
	 */
	public Expression(ParseTreeNode left, ParseTreeNode right, NumericalOperator op)
	{
		this.left = left;
		this.op = op;
		this.right = right;		
	}
	
	/**
	 * Sets the left branch of this expression.
	 *  
	 * @param left the subtree to set as left branch
	 */
	public void setLeftBranch(ParseTreeNode left)
	{
		this.left = left;
	}

	/**
	 * Returns the left branch of this expression.
	 *  
	 * @return left branch of this expression.
	 */
	public ParseTreeNode getLeftBranch()
	{
		return this.left;
	}
	
	/**
	 * Sets the right branch of this expression.
	 *  
	 * @param right the subtree to set as right branch
	 */
	public void setRightBranch(ParseTreeNode right)
	{
		this.right = right;
	}
	
	/**
	 * Returns the right branch of this expression.
	 *  
	 * @return right branch of this expression.
	 */
	public ParseTreeNode getRightBranch()
	{
		return this.right;
	}
	
	/**
	 * Sets the operator of this expression.
	 * 
	 * @param operator the operator to set as operator of this expression.
	 */
	public void setOperator(NumericalOperator operator)
	{
		this.op = operator;
	}
	
	/**
	 * Returns the operator of this expression.
	 * 
	 * @return the operator of this expression.
	 */
	public NumericalOperator getOperator()
	{
		return this.op;
	}
	
	/**
	 * Returns an expression (clone) with all occurrences of 'search' replaced by 'replace'.
	 * The non-expression children of expression are not cloned. (no deep clone)
	 * 
	 * @param expr The expression to search through.
	 * @param search The node to search for.
	 * @param replace The node to replace found nodes with.
	 * @return The Expression with the replaced nodes.
	 */
	public static ParseTreeNode replaceExpression(ParseTreeNode expression, ParseTreeNode search, ParseTreeNode replace)
	{
		if(expression instanceof Expression)
		{
			Expression expr = (Expression) expression;
			// create copy of expression
			Expression res = new Expression();
	
			// set operator
			res.op = expr.getOperator();
			switch(expr.getOperator())
			{
			case DIV:
				res.op = NumericalOperator.DIV;
				break;
			case MUL:
				res.op = NumericalOperator.MUL;
				break;
			case MINUS:
				res.op = NumericalOperator.MINUS;
				break;
			case PLUS:
				res.op = NumericalOperator.PLUS;
				break;
			default:
				res.op = NumericalOperator.UNDETERMINED;
				break;
			}
			
			// replace left part of the expression
			ParseTreeNode left;
			if(expr.getLeftBranch() instanceof Expression)
			{
				left = replaceExpression((expr.getLeftBranch()), search, replace);
			} 
			else 
			{
				left = expr.left;
			}
			// always perform check as search could be an expression itself	
			if(left == search)
			{
				expr.left = replace;
			}
			res.left = left;
			
			
			// replace right part of the expression
			ParseTreeNode right;
			if(expr.getRightBranch() instanceof Expression)
			{
				right = replaceExpression((expr.getRightBranch()), search, replace);
			} 
			else 
			{
				right = expr.right;
			}
			// always perform check as search could be an expression itself
			if(right == search)
			{
				expr.right = replace;
			}
			res.right = right;
			
			return res;
		} 
		else 
		{
			if(expression == search)
			{
				return replace;
			} 
			else 
			{
				return expression;
			}
		}
	}
	
	/**
	 * Swaps the left and right branch of the expression if the 
	 * operator is commutative.
	 * 
	 * @result true, if the elements were swapped, false otherwise.
	 */
	public boolean swap()
	{	
		// if operator allows exchange
		if(this.op.isCommutative())
		{
			// swap left and right branch
			ParseTreeNode tmp = this.left;
			this.left = this.right;
			this.right = tmp;
			return true;
		}
		return false;
	}
	
	@Override
	public String getNodeName() 
	{
		return "EXPRESSION";
	}

	@Override
	public String getNodeContents() 
	{
		StringBuilder bld = new StringBuilder();
		
		bld.append(this.left == null ? "null" : "(" + this.left.getNodeContents());
		bld.append(' ').append(this.op == null ? "?" : this.op.getSymbol()).append(' ');
		bld.append(this.right == null ? "null" : this.right.getNodeContents() + ")");
		
		return bld.toString();	
	}

	@Override
	public Iterator<? extends ParseTreeNode> getChildren() 
	{
		ArrayList<ParseTreeNode> list = new ArrayList<ParseTreeNode>();
		
		// add left hand side
		if (this.left != null) {
			list.add(this.left);
		}
		
		// add right hand side
		if (this.right != null) {
			list.add(this.right);
		}
		
		return list.iterator();
	}

	@Override
	public int getNumberOfChildren() 
	{
		int children = 0;
		
		children += this.left == null ? 0 : 1;
		children += this.right == null ? 0 : 1;
		
		return children;
	}

	@Override
	public boolean isIdenticalTo(ParseTreeNode node) 
	{
		// probably fails for unreduced expressions but the parser should take care of these
		if(node != null && node instanceof Expression)
		{
			Expression other = (Expression) node;
			if(this.op == other.op)
			{
				if(this.op.isCommutative())
				{
					return ( (this.left.isIdenticalTo(other.left) && 
								this.right.isIdenticalTo(other.right)) 
							|| (this.left.isIdenticalTo(other.right) &&
								this.right.isIdenticalTo(other.left))
							);
				}
				else 
				{
					return (this.left.isIdenticalTo(other.left) && this.right.isIdenticalTo(other.right));
				}
			}
		}
		return false;
	}
	
	@Override
	public String toString()
	{	
		return getNodeContents();
	}
	
}
