package de.tuberlin.dima.minidb.optimizer.generator.util;

import java.util.HashMap;
import java.util.Map;

import de.tuberlin.dima.minidb.optimizer.FetchPlanOperator;
import de.tuberlin.dima.minidb.optimizer.FilterPlanOperator;
import de.tuberlin.dima.minidb.optimizer.GroupByPlanOperator;
import de.tuberlin.dima.minidb.optimizer.IndexLookupPlanOperator;
import de.tuberlin.dima.minidb.optimizer.MergeJoinPlanOperator;
import de.tuberlin.dima.minidb.optimizer.NestedLoopJoinPlanOperator;
import de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator;
import de.tuberlin.dima.minidb.optimizer.OptimizerPlanVisitor;
import de.tuberlin.dima.minidb.optimizer.SortPlanOperator;
import de.tuberlin.dima.minidb.optimizer.TableScanPlanOperator;
import de.tuberlin.dima.minidb.semantics.BaseTableAccess;
import de.tuberlin.dima.minidb.semantics.Column;
import de.tuberlin.dima.minidb.semantics.ProducedColumn;
import de.tuberlin.dima.minidb.semantics.Relation;
import de.tuberlin.dima.minidb.semantics.predicate.LocalPredicate;
import de.tuberlin.dima.minidb.semantics.predicate.LocalPredicateAtom;
import de.tuberlin.dima.minidb.semantics.predicate.LocalPredicateBetween;
import de.tuberlin.dima.minidb.semantics.predicate.LocalPredicateConjunct;

public class PhysicalPlanAssemblyDumper implements OptimizerPlanVisitor
{
	private char currCvar = 'a';
	
	private Map<OptimizerPlanOperator, String> subplanVars = new HashMap<OptimizerPlanOperator, String>();
	private Map<Integer, String> relationVars = new HashMap<Integer, String>();
	
	
	public void dumpAssembly(OptimizerPlanOperator operator)
	{
		operator.accept(this);
	}


	@Override
	public void preVisit(OptimizerPlanOperator operator)
	{
	}


	@Override
	public void postVisit(OptimizerPlanOperator operator)
	{
		if (operator instanceof FetchPlanOperator)
		{
			dumpAssemblyCode((FetchPlanOperator) operator);
		}
		else if (operator instanceof FilterPlanOperator)
		{
			dumpAssemblyCode((FilterPlanOperator) operator);
		}
		else if (operator instanceof GroupByPlanOperator)
		{
			dumpAssemblyCode((GroupByPlanOperator) operator);
		}
		else if (operator instanceof IndexLookupPlanOperator)
		{
			dumpAssemblyCode((IndexLookupPlanOperator) operator);
		}
		else if (operator instanceof MergeJoinPlanOperator)
		{
			dumpAssemblyCode((MergeJoinPlanOperator) operator);
		}
		else if (operator instanceof NestedLoopJoinPlanOperator)
		{
			dumpAssemblyCode((NestedLoopJoinPlanOperator) operator);
		}
		else if (operator instanceof SortPlanOperator)
		{
			dumpAssemblyCode((SortPlanOperator) operator);
		}
		else if (operator instanceof TableScanPlanOperator)
		{
			dumpAssemblyCode((TableScanPlanOperator) operator);
		}
		else
		{
			System.out.println("// TODO: Unsupported dump for '" + operator.getName() + "' operator");
		}
	}
	
	private void dumpAssemblyCode(FetchPlanOperator operator)
	{
		System.out.println("// fetch subplan");
		
		// assign a variable for this subplan
		String subplanVar = new Character(this.currCvar++).toString();
		String childVar = this.subplanVars.get(operator.getChild());
		
		System.out.println(String.format("BaseTableAccess %s_table_access = %s;", subplanVar, getBaseTableAccessVar(operator.getTableAccess())));
		
		// dump the produced columns
		Column[] columns = operator.getReturnedColumns();
		System.out.println(String.format("Column[] %s_columns = new Column[%d];", subplanVar, columns.length));
		for(int i = 0; i < columns.length; i++)
		{
			String relation = String.format("%s_table_access", subplanVar);
			String dataType = String.format("DataType.get(BasicType.%s, %d)", columns[i].getDataType().toString(), columns[i].getDataType().getLength());
			int index = columns[i].getColumnIndex();
			System.out.println(String.format("%s_columns[%d] = new Column(%s, %s, %s);", subplanVar, i, relation, dataType, index));
		}
		

		System.out.println(String.format("FetchPlanOperator %1$s = new FetchPlanOperator(%2$s, %1$s_table_access, %1$s_columns);", subplanVar, childVar));
		System.out.println("");
		
		this.subplanVars.put(operator, subplanVar);
	}
	
	private void dumpAssemblyCode(FilterPlanOperator operator)
	{
		System.out.println("// filter subplan");
		
		// assign a variable for this subplan
		String subplanVar = new Character(this.currCvar++).toString();
		String childVar = this.subplanVars.get(operator.getChild());

		dumpPredicateAssemblyCode(String.format("%s_pred", subplanVar), String.format("null /* FIXME */", subplanVar), operator.getSimplePredicate());

		System.out.println(String.format("FilterPlanOperator %1$s = new FilterPlanOperator(%2$s, %1$s_pred);", subplanVar, childVar));
		System.out.println("");
		
		this.subplanVars.put(operator, subplanVar);
	}
	
	private void dumpAssemblyCode(GroupByPlanOperator operator)
	{
		System.out.println("// group by subplan");
		
		// assign a variable for this subplan
		String subplanVar = new Character(this.currCvar++).toString();
		String childVar = this.subplanVars.get(operator.getChild());
		
		ProducedColumn[] prod_cols = operator.getProducedColumns();
		System.out.println(String.format("ProducedColumn[] %s_prod_columns = new ProducedColumn[%d];", subplanVar, prod_cols.length));
		for(int i = 0; i < prod_cols.length; i++)
		{
			ProducedColumn prod_col = prod_cols[i];
			
			String prefix = String.format("%s_prod_column_%d", subplanVar, i);

			System.out.println(String.format("Relation %s_relation = %s;", prefix, getRelationVar(prod_col.getRelation())));
			System.out.println(String.format("DataType %s_data_type = DataType.get(BasicType.%s, %d);", prefix, prod_col.getDataType().toString(), prod_col.getDataType().getLength()));
			System.out.println(String.format("int %s_col_index = %d;", prefix, prod_col.getColumnIndex()));
			System.out.println(String.format("String %s_col_alias = \"%s\";", prefix, prod_col.getColumnAliasName()));
			System.out.println(String.format("OutputColumn %s_parsed_col = null; //TODO", prefix));
			System.out.println(String.format("OutputColumn.AggregationType %s_agg_fun = OutputColumn.AggregationType.%s;", prefix, prod_col.getAggregationFunction().toString()));
			
			System.out.println(String.format("%1$s_prod_columns[%3$d] = new ProducedColumn(%2$s_relation, %2$s_data_type, %2$s_col_index, %2$s_col_alias, %2$s_parsed_col, %2$s_agg_fun);", subplanVar, prefix, i));
		}
		System.out.println("");
		
		int[] group_col_indices = operator.getGroupColIndices();
		System.out.println(String.format("int[] %s_group_col_indices = new int[%d];", subplanVar, group_col_indices.length));
		for(int i = 0; i < group_col_indices.length; i++)
		{
			System.out.println(String.format("%1$s_group_col_indices[%2$d] = %3$d;", subplanVar, i, group_col_indices[i]));
		}
		System.out.println("");
		
		int[] agg_col_indices = operator.getAggColIndices();
		System.out.println(String.format("int[] %s_agg_col_indices = new int[%d];", subplanVar, agg_col_indices.length));
		for(int i = 0; i < agg_col_indices.length; i++)
		{
			System.out.println(String.format("%1$s_agg_col_indices[%2$d] = %3$d;", subplanVar, i, agg_col_indices[i]));
		}
		System.out.println("");
		
		
		System.out.println(String.format("GroupByPlanOperator %1$s = new GroupByPlanOperator(%2$s, %1$s_prod_columns, %1$s_group_col_indices, %1$s_agg_col_indices, %3$d);", subplanVar, childVar, operator.getOutputCardinality()));
		System.out.println("");
		
		this.subplanVars.put(operator, subplanVar);
	}
	
	private void dumpAssemblyCode(IndexLookupPlanOperator operator)
	{
		System.out.println("// index scan subplan");
		
		// assign a variable for this subplan
		String subplanVar = new Character(this.currCvar++).toString();
		
		if (operator.isCorrelated())
		{
			// IndexDescriptor index, BaseTableAccess table, int correlatedColumnIndex, long cardinalityPerAccess
			
    		System.out.println(String.format("IndexDescriptor %s_index = dbInstance.getCatalogue().getIndex(\"%s\");", subplanVar, operator.getIndex().getName()));
    		System.out.println(String.format("BaseTableAccess %s_table_access = %s;", subplanVar, getBaseTableAccessVar(operator.getTableAccess())));
    		System.out.println(String.format("int %s_corr_index = %d;", subplanVar, operator.getCorrelatedColumnIndex()));

    		System.out.println(String.format("IndexLookupPlanOperator %1$s = new IndexLookupPlanOperator(%1$s_index, %1$s_table_access, %1$s_corr_index, %2$d);", subplanVar, operator.getOutputCardinality()));
    		System.out.println("");
		}
		else
		{	
    		System.out.println(String.format("IndexDescriptor %s_index = dbInstance.getCatalogue().getIndex(\"%s\");", subplanVar, operator.getIndex().getName()));
    		System.out.println(String.format("BaseTableAccess %s_table_access = %s;", subplanVar, getBaseTableAccessVar(operator.getTableAccess())));
    		dumpPredicateAssemblyCode(String.format("%s_pred", subplanVar), String.format("%s_table_access", subplanVar), operator.getLocalPredicate());
    		
    		System.out.println(String.format("IndexLookupPlanOperator %1$s = new IndexLookupPlanOperator(%1$s_index, %1$s_table_access, %1$s_pred, %2$d);", subplanVar, operator.getOutputCardinality()));
    		System.out.println("");
		}
		
		this.subplanVars.put(operator, subplanVar);
	}
	
	private void dumpAssemblyCode(MergeJoinPlanOperator operator)
	{
		System.out.println("// merge join subplan");
		
		// assign a variable for this subplan
		String subplanVar = new Character(this.currCvar++).toString();
		String leftChildVar = this.subplanVars.get(operator.getLeftChild());
		String rightChildVar = this.subplanVars.get(operator.getRightChild());
		
		// dump the produced columns
		System.out.println(String.format("int[] %s_lj_cols = new int[%d];", subplanVar, operator.leftJoinColumns.length));
		for(int i = 0; i < operator.leftJoinColumns.length; i++)
		{
			System.out.println(String.format("%s_lj_cols[%d] = %d;", subplanVar, i, operator.leftJoinColumns[i]));
		}
		
		System.out.println(String.format("int[] %s_rj_cols = new int[%d];", subplanVar, operator.rightJoinColumns.length));
		for(int i = 0; i < operator.rightJoinColumns.length; i++)
		{
			System.out.println(String.format("%s_rj_cols[%d] = %d;", subplanVar, i, operator.rightJoinColumns[i]));
		}
		
		System.out.println(String.format("int[] %s_lc_map = new int[%d];", subplanVar, operator.leftOutColMap.length));
		for(int i = 0; i < operator.leftOutColMap.length; i++)
		{
			System.out.println(String.format("%s_lc_map[%d] = %d;", subplanVar, i, operator.leftOutColMap[i]));
		}
		
		System.out.println(String.format("int[] %s_rc_map = new int[%d];", subplanVar, operator.rightOutColMap.length));
		for(int i = 0; i < operator.rightOutColMap.length; i++)
		{
			System.out.println(String.format("%s_rc_map[%d] = %d;", subplanVar, i, operator.rightOutColMap[i]));
		}
		
		System.out.println(String.format("MergeJoinPlanOperator %s = new MergeJoinPlanOperator(%s, %s, null, %s_lj_cols, %s_rj_cols, %s_lc_map, %s_rc_map, %d);", subplanVar, leftChildVar, rightChildVar, subplanVar, subplanVar, subplanVar, subplanVar, operator.getOutputCardinality()));
		System.out.println("");
		
		this.subplanVars.put(operator, subplanVar);
	}

	private void dumpAssemblyCode(NestedLoopJoinPlanOperator operator)
	{
		System.out.println("// nested loop join subplan");
		
		// assign a variable for this subplan
		String subplanVar = new Character(this.currCvar++).toString();
		String leftChildVar = this.subplanVars.get(operator.getLeftChild());
		String rightChildVar = this.subplanVars.get(operator.getRightChild());
		
		System.out.println(String.format("int[] %s_oc_map= new int[%d];", subplanVar, operator.outerOutColMap.length));
		for(int i = 0; i < operator.outerOutColMap.length; i++)
		{
			System.out.println(String.format("%s_oc_map[%d] = %d;", subplanVar, i, operator.outerOutColMap[i]));
		}
		
		System.out.println(String.format("int[] %s_ic_map= new int[%d];", subplanVar, operator.innerOutColMap.length));
		for(int i = 0; i < operator.innerOutColMap.length; i++)
		{
			System.out.println(String.format("%s_ic_map[%d] = %d;", subplanVar, i, operator.innerOutColMap[i]));
		}
		
		System.out.println(String.format("NestedLoopJoinPlanOperator %1$s = new NestedLoopJoinPlanOperator(%2$s, %3$s, null, %1$s_oc_map, %1$s_ic_map, %4$d);", subplanVar, leftChildVar, rightChildVar, operator.getOutputCardinality()));
		System.out.println("");
		
		this.subplanVars.put(operator, subplanVar);
	}
	
	private void dumpAssemblyCode(SortPlanOperator operator)
	{
		System.out.println("// sort subplan");
		
		// assign a variable for this subplan
		String subplanVar = new Character(this.currCvar++).toString();
		String childVar = this.subplanVars.get(operator.getChild());
		
		// dump the produced columns
		int[] sortColumnIndices = operator.getSortColumnIndices();
		System.out.println(String.format("int[] %s_col_indices = new int[%d];", subplanVar, sortColumnIndices.length));
		for(int i = 0; i < sortColumnIndices.length; i++)
		{
			System.out.println(String.format("%s_col_indices[%d] = %d;", subplanVar, i, sortColumnIndices[i]));
		}
		
		// dump the sort ascending boolean flags
		boolean[] sortAscending = operator.getSortAscending();
		System.out.println(String.format("boolean[] %s_sort_asc = new boolean[%d];", subplanVar, sortAscending.length));
		for(int i = 0; i < sortAscending.length; i++)
		{
			System.out.println(String.format("%s_sort_asc[%d] = %b;", subplanVar, i, sortAscending[i]));
		}
		
		System.out.println(String.format("SortPlanOperator %s = new SortPlanOperator(%s, %s_col_indices, %s_sort_asc);", subplanVar, childVar, subplanVar, subplanVar));
		System.out.println("");
		
		this.subplanVars.put(operator, subplanVar);
	}
	
	private void dumpAssemblyCode(TableScanPlanOperator operator)
	{
		System.out.println("// table scan (" + operator.getTable().getTableName() + ")");
		
		// assign a variable for this subplan
		String subplanVar = new Character(this.currCvar++).toString();
		
		// dump the base table access
		System.out.println(String.format("BaseTableAccess %s_table_access = %s;", subplanVar, getBaseTableAccessVar(operator.getTableAccess())));
		
		// dump the produced columns
		Column[] columns = operator.getReturnedColumns();
		System.out.println(String.format("Column[] %s_columns = new Column[%d];", subplanVar, columns.length));
		for(int i = 0; i < columns.length; i++)
		{
			String relation = String.format("%s_table_access", subplanVar);
			String dataType = String.format("DataType.get(BasicType.%s, %d)", columns[i].getDataType().toString(), columns[i].getDataType().getLength());
			int index = columns[i].getColumnIndex();
			System.out.println(String.format("%s_columns[%d] = new Column(%s, %s, %s);", subplanVar, i, relation, dataType, index));
		}
		
		System.out.println(String.format("TableScanPlanOperator %s = new TableScanPlanOperator(%s_table_access, %s_columns);", subplanVar, subplanVar, subplanVar));
		
		if (operator.getPredicate() != null)
		{
			dumpPredicateAssemblyCode(String.format("%s_pred", subplanVar), String.format("%s_table_access", subplanVar), operator.getPredicate());
			System.out.println(String.format("%1$s.setPredicate(%1$s_pred);", subplanVar));
		}
		
		System.out.println("");
		
		this.subplanVars.put(operator, subplanVar);
	}
	
	private String getRelationVar(Relation relation)
	{
		if (this.relationVars.containsKey(relation.getID()))
		{
			return this.relationVars.get(relation.getID());
		}
		else
		{
			throw new RuntimeException("Unknown relation #" + relation.getID());
		}
	}
	
	private String getBaseTableAccessVar(BaseTableAccess tableAccess)
	{
		if (!this.relationVars.containsKey(tableAccess.getID()))
		{
			System.out.println(String.format("BaseTableAccess relation_%d = new BaseTableAccess(dbInstance.getCatalogue().getTable(\"%s\"));", tableAccess.getID(), tableAccess.getTable().getTableName()));
			System.out.println(String.format("relation_%d.setID(%d);", tableAccess.getID(), tableAccess.getID()));
			System.out.println(String.format("relation_%d.setOutputCardinality(%d);", tableAccess.getID(), tableAccess.getOutputCardinality()));
			
			this.relationVars.put(tableAccess.getID(), String.format("relation_%d", tableAccess.getID()));
		}
		
		return this.relationVars.get(tableAccess.getID());
	}
	
	private void dumpPredicateAssemblyCode(String predVar, String baseTableAccessVar, LocalPredicate predicate)
	{
		if (predicate instanceof LocalPredicateAtom)
		{
			dumpPredicateAssemblyCode(predVar, baseTableAccessVar, (LocalPredicateAtom) predicate);
		}
		else if (predicate instanceof LocalPredicateBetween)
		{
			dumpPredicateAssemblyCode(predVar, baseTableAccessVar, (LocalPredicateBetween) predicate);
		}
		else if (predicate instanceof LocalPredicateConjunct)
		{
			dumpPredicateAssemblyCode(predVar, baseTableAccessVar, (LocalPredicateConjunct) predicate);
		}
	}
	
	private void dumpPredicateAssemblyCode(String predVar, String baseTableAccessVar, LocalPredicateAtom predicate)
	{
		// parsed predicate
		System.out.println(String.format("Predicate %s_parsed_pred = createPredicate(\"%s\", false);", predVar, predicate.getParsedPredicate().toString()));
		// column
		Column column = predicate.getColumn();
		String dataType = String.format("DataType.get(BasicType.%s, %d)", column.getDataType().toString(), column.getDataType().getLength());
		int index = column.getColumnIndex();
		System.out.println(String.format("Column %s_column = new Column(%s, %s, %s);", predVar, baseTableAccessVar, dataType, index));
		// literal
		System.out.println(String.format("DataField %s_literal = new DataField(\"%s\"); //FIXME", predVar, predicate.getLiteral().toString()));
		// local predicate atom constructor
		System.out.println(String.format("LocalPredicateAtom %1$s = new LocalPredicateAtom(%1$s_parsed_pred, %1$s_column, %1$s_literal);", predVar));
	}
	
	private void dumpPredicateAssemblyCode(String predVar, String baseTableAccessVar, LocalPredicateBetween predicate)
	{
		// parsed predicate
		System.out.println(String.format("Predicate %s_lb_pred = createPredicate(\"%s\", false);", predVar, predicate.getLowerBound().toString()));
		System.out.println(String.format("Predicate %s_ub_pred = createPredicate(\"%s\", false);", predVar, predicate.getUpperBound().toString()));
		// column
		Column column = predicate.getColumn();
		String dataType = String.format("DataType.get(BasicType.%s, %d)", column.getDataType().toString(), column.getDataType().getLength());
		int index = column.getColumnIndex();
		System.out.println(String.format("Column %s_column = new Column(%s, %s, %s);", predVar, baseTableAccessVar, dataType, index));
		// literal
		System.out.println(String.format("DataField %s_lb_literal = new DataField(\"%s\"); //FIXME", predVar, predicate.getLowerBoundLiteral().toString()));
		System.out.println(String.format("DataField %s_up_literal = new DataField(\"%s\"); //FIXME", predVar, predicate.getUpperBoundLiteral().toString()));
		// local predicate atom constructor
		System.out.println(String.format("LocalPredicateBetween %1$s = new LocalPredicateBetween(%1$s_column, %1$s_lb_pred, %1$s_ub_pred, %1$s_lb_literal, %1$s_up_literal);", predVar));
	}
	
	private void dumpPredicateAssemblyCode(String predVar, String baseTableAccessVar, LocalPredicateConjunct predicate)
	{
		System.out.println(String.format("LocalPredicateConjunct %s_pred = new LocalPredicateConjunct(); //TODO", predVar));
	}
}
