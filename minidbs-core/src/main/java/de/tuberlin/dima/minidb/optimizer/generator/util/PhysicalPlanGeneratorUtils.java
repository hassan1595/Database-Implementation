package de.tuberlin.dima.minidb.optimizer.generator.util;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import de.tuberlin.dima.minidb.catalogue.Catalogue;
import de.tuberlin.dima.minidb.catalogue.IndexDescriptor;
import de.tuberlin.dima.minidb.optimizer.AbstractJoinPlanOperator;
import de.tuberlin.dima.minidb.optimizer.FetchPlanOperator;
import de.tuberlin.dima.minidb.optimizer.FilterPlanOperator;
import de.tuberlin.dima.minidb.optimizer.IndexLookupPlanOperator;
import de.tuberlin.dima.minidb.optimizer.InterestingOrder;
import de.tuberlin.dima.minidb.optimizer.OptimizerException;
import de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator;
import de.tuberlin.dima.minidb.optimizer.OrderedColumn;
import de.tuberlin.dima.minidb.optimizer.RequestedOrder;
import de.tuberlin.dima.minidb.optimizer.SortPlanOperator;
import de.tuberlin.dima.minidb.optimizer.cardinality.CardinalityEstimator;
import de.tuberlin.dima.minidb.parser.Predicate;
import de.tuberlin.dima.minidb.semantics.BaseTableAccess;
import de.tuberlin.dima.minidb.semantics.Column;
import de.tuberlin.dima.minidb.semantics.Relation;
import de.tuberlin.dima.minidb.semantics.predicate.JoinPredicate;
import de.tuberlin.dima.minidb.semantics.predicate.JoinPredicateAtom;
import de.tuberlin.dima.minidb.semantics.predicate.JoinPredicateConjunct;
import de.tuberlin.dima.minidb.semantics.predicate.LocalPredicate;
import de.tuberlin.dima.minidb.semantics.predicate.LocalPredicateAtom;
import de.tuberlin.dima.minidb.semantics.predicate.LocalPredicateBetween;

public class PhysicalPlanGeneratorUtils
{
	/**
	 * Constant indicating whether pruning routine trace should be printed out.
	 */
	private static final boolean PRINT_PRUNING_TRACE = true;
	
	/**
	 * Helper to sort candidate plans for the pruning phase.
	 */
	public static final Comparator<OptimizerPlanOperator> PRUNE_SORTER = new OutOrderColsCountSorter();

	// ------------------------------------------------------------------------
	//                  Utility Methods for Operators
	// ------------------------------------------------------------------------
	
	/**
	 * Tries to create an index scan for a given predicate and a list of available indexes. 
	 * This function searches the list of available indexes and if it finds an index on the
	 * predicates column, it creates an index scan for it. If it does not find a suitable
	 * index, it returns null. 
	 * 
	 * The predicate must be either a <tt>OptimizerLocalPredicateAtom</tt> or a
	 * <tt>OptimizerLocalPredicateBetween</tt>. An atom predicate may not have an inequality
	 * operator.
	 * 
	 * @param table The table access for which this function searches for a suitable index access.
	 * @param cardinality The cardinality after the application of the given predicate.
	 * @param pred The predicate to be represented by the index access.
	 * @param predCol The index of the column that the predicate operates on.
	 * @param indexes The list of available indexes for the accessed table.
	 * @return An index scan operator for the given predicate, or null.
	 */
	public static IndexLookupPlanOperator createIndexLookup(
			BaseTableAccess table, long cardinality,
			LocalPredicate pred, int predCol, List<IndexDescriptor> indexes)
	{
		for (IndexDescriptor id : indexes) {
			if (id.getSchema().getColumnNumber() == predCol) {
				// that is our index
				if (pred instanceof LocalPredicateAtom) {
					return new IndexLookupPlanOperator(id, table, (LocalPredicateAtom) pred, cardinality);
				}
				else if (pred instanceof LocalPredicateBetween) {
					return new IndexLookupPlanOperator(id, table, (LocalPredicateBetween) pred, cardinality);
				}
			}
		}
		return null;
	}
	
	/**
	 * Takes a join predicate and checks if its join columns (right and left-hand side) are
	 * already contained in the list of columns that the right and left hand side produces.
	 * If they are not contained, they are added to the list of columns. Also, the position
	 * of the join columns in the array of required columns from the left and right-hand side
	 * are set.
	 * <p>
	 * In addition, the method returns a copy of the given join predicate, where the column
	 * indices are no longer those in the base table, but those in the input tuples.
	 *  
	 * @param atom The join predicate for which to check the columns.
	 * @param leftColumns The columns currently required from the left join child.
	 * @param rightColumns The columns currently required from the right join child.
	 * @param leftTables The tables produced by the left join child.
	 * @param rightTables The tables produced by the right join child.
	 * @param leftColIndices The array into which to place the position of the left
	 *                       join key column in the tuple.
	 * @param rightColIndices The array into which to place the position of the right
	 *                       join key column in the tuple.
	 * @param pos The position to place the key column indices for the two arrays above.
	 * @return
	 */
	public static JoinPredicateAtom addKeyColumnsAndAdaptPredicate(
			JoinPredicateAtom atom,
			List<Column> leftColumns, List<Column> rightColumns,
			Set<Relation> leftTables, Set<Relation> rightTables,
			int[] leftColIndices, int[] rightColIndices, int pos)
	{
		// --------------------------------------------------------------------
		//                  find the column for the left side
		// --------------------------------------------------------------------
		
		Column leftJoinCol = atom.getLeftHandColumn();
		int leftColIndex = leftColumns.size();
		
		// search for the key column
		for (int i = 0; i < leftColumns.size(); i++) {
			Column col = leftColumns.get(i);
			if (col.equals(leftJoinCol)) {
				// this is the key column
				leftColIndex = i;
			}
		}
		
		if (leftColIndex == leftColumns.size()) {
			// column was not found
			leftColumns.add(leftJoinCol);
			
		}
		
		leftColIndices[pos] = leftColIndex;
		
		// --------------------------------------------------------------------
		//                  find the column for the right side
		// --------------------------------------------------------------------
		
		Column rightJoinCol = atom.getRightHandColumn();
		
		int rightColIndex = rightColumns.size();
		
		// search for the key column
		for (int i = 0; i < rightColumns.size(); i++) {
			Column col = rightColumns.get(i);
			if (col.equals(rightJoinCol)) {
				// this is the key column
				rightColIndex = i;
			}
		}
		
		if (rightColIndex == rightColumns.size()) {
			// column was not found
			rightColumns.add(rightJoinCol);
		}
		
		rightColIndices[pos] = rightColIndex;
		
		return new JoinPredicateAtom(atom.getParsedPredicate(), 
				atom.getLeftHandOriginatingTable(),
				atom.getRightHandOriginatingTable(),
				new Column(leftJoinCol.getRelation(), leftJoinCol.getDataType(), leftColIndex),
				new Column(rightJoinCol.getRelation(), rightJoinCol.getDataType(), rightColIndex));
	}
	
	/**
	 * Checks if the given join predicate allows to be evaluated through a Merge-Join.
	 * The current Merge-Join implementation supports only equality predicates.
	 * 
	 * @param predicate The predicate to be checked. 
	 * @return true, if the predicate can be evaluated through a Merge-Join, false if not.
	 */
	public static boolean isMergeJoinPossible(JoinPredicate predicate)
	{
		if (predicate == null) {
			// cartesian join, not possible
			return false;
		}
		if (predicate instanceof JoinPredicateAtom) {
			return ((JoinPredicateAtom) predicate).getParsedPredicate().getOp() == 
				Predicate.Operator.EQUAL;
		}
		else {
			for (JoinPredicate p : 
				((JoinPredicateConjunct) predicate).getConjunctiveFactors())
			{
				if (!isMergeJoinPossible(p)) {
					return false;
				}
			}
			return true;
		}
	}
	
	/**
	 * Checks if the plan rooted at the given operator satisfies an order requirement. If it
	 * does, it is returned as it is. If it does not, a <tt>SortPlanOperator</tt> is added
	 * to produce the required order. 
	 * 
	 * @param pop The plan to check and add the sort to.
	 * @param requestedOrder The order that is expected.
	 * @param sortColumns The indices of the columns to sort after, if a sort needs to be added.
	 * @param direction The array of flags for the sort direction. True indicates ascending, false
	 *                  indicates descending.
	 * @return A plan based on the given plan that satisfies the given order requirement. 
	 */
	public static OptimizerPlanOperator addSortIfNecessary(
			OptimizerPlanOperator pop, RequestedOrder[] requestedOrder,
			int[] sortColumns, boolean[] direction)
	{
		// check if the operator has the required output properties
		OrderedColumn[] producedOrder = pop.getColumnOrder();
		
		if (producedOrder != null && producedOrder.length >= requestedOrder.length) {
			boolean match = true;
			for (int i = 0; i < requestedOrder.length; i++) {
				if (!requestedOrder[i].isMetBy(producedOrder[i])) {
					match = false;
					break;
				}
			}
			if (match) {
				return pop;
			}
		}
		
		// we need an extra sort operator
		return new SortPlanOperator(pop, sortColumns, direction);
	}
	
	
	// --------------------------------------------------------------------------------------------
	//                                   Pruning and comparison
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Prunes the given plans with respect to costs and interesting orders. The method first
	 * determines the plan that has the cheapest cumulative costs. For each of the given
	 * interesting orders, one other more expensive plan may be kept when its produced order
	 * matches the interesting order and if the cost delta, by which it is more expensive than
	 * the cheapest plan, is within the limit of the interesting order.
	 * 
	 * @param plans The array of plans to be pruned.
	 * @param orders The array of interesting orders used during pruning
	 * @return An array with all plans that are cheapest or cheapest among those that
	 *         satisfy one of the given interesting orders.
	 */
	public static OptimizerPlanOperator[] prunePlans(OptimizerPlanOperator[] plans,
			InterestingOrder[] orders)
	{
		// first, sort the plan by the number of columns in the output order
		Arrays.sort(plans, PRUNE_SORTER);
		
		
		if (PRINT_PRUNING_TRACE)
		{
			if (orders != null && orders.length > 0)
			{
    			System.out.println("------------------------------------------");
        		System.out.println("- INTERESTING ORDERS ");
        		System.out.println("------------------------------------------");
        		
        		for (InterestingOrder o : orders)
        		{
        			System.out.println(" ORDER: " + o.toString());
        		}
			}
			
			PhysicalPlanPrinter printer = new PhysicalPlanPrinter(true);
    		System.out.println("------------------------------------------");
    		System.out.println("- UNPRUNED PLANS ");
    		System.out.println("------------------------------------------");
    		for(OptimizerPlanOperator plan : plans)
    		{
    			printer.print(plan);
    			System.out.println("------------------------------------------");
    		}
		}
		
		// first go through the list and find the cheapest plan
		int cheapestIndex = -1;
		long cheapestCost = Long.MAX_VALUE;
		for (int i = 0; i < plans.length; i++) {
			OptimizerPlanOperator pop = plans[i];
			if (pop.getCumulativeCosts() < cheapestCost) {
				cheapestCost = pop.getCumulativeCosts();
				cheapestIndex = i;
			}
		}
		
		OptimizerPlanOperator bestPlan = plans[cheapestIndex];
		
		if (orders == null) {
			
			if (PRINT_PRUNING_TRACE)
			{
				PhysicalPlanPrinter printer = new PhysicalPlanPrinter(true);
	    		System.out.println("------------------------------------------");
	    		System.out.println("- PRUNED PLANS ");
	    		System.out.println("------------------------------------------");
    			printer.print(bestPlan);
	    		System.out.println("------------------------------------------");
			}
			
			return new OptimizerPlanOperator[] { bestPlan };
		}
		
		// remember the cheapest one
		OptimizerPlanOperator[] bestPlanForOrder = new OptimizerPlanOperator[orders.length];
		plans[cheapestIndex] = null;
		int numPlans = 1;
		
		// go over all other plans. check if there is an interesting order
		// and their additional cost within range
		for (int i = 0; i < plans.length; i++) {
			OptimizerPlanOperator currPlan = plans[i];
			if (currPlan == null) {
				continue;
			}
			
			// check if the plan has an interesting order
			OrderedColumn[] order = currPlan.getColumnOrder();
			if (order != null && order.length > 0) {
				// go over all interesting orders
				long costDiff = currPlan.getCumulativeCosts() - cheapestCost;
				
				for (int k = 0; k < orders.length; k++) {
					InterestingOrder io = orders[k];
					
					if (costDiff < io.getMaxCost() && io.isMetByOutputOrder(order)) {
						// the plan has this interesting order.
						OptimizerPlanOperator thisOrderPlan = bestPlanForOrder[k];
						if (thisOrderPlan == null) {
							bestPlanForOrder[k] = currPlan;
							numPlans++;
						}
						else if (thisOrderPlan.getCumulativeCosts() > currPlan.getCumulativeCosts()) {
							// replace the current plan for this interesting order
							bestPlanForOrder[k] = currPlan;
						}
					}
				} // end all interesting orders
			} // end if has an order
		}// end for all plans
		
		// make an array out of the plans
		OptimizerPlanOperator[] finalPlans;
		if (numPlans == 1) {
			finalPlans = new OptimizerPlanOperator[] { bestPlan };
		}
		else {
			finalPlans = new OptimizerPlanOperator[numPlans];
			finalPlans[0] = bestPlan;
			int pos = 1;
			outer: for (int i = 0; i < bestPlanForOrder.length; i++) {
				OptimizerPlanOperator bpfo = bestPlanForOrder[i];
				if (bpfo != null) {
					// check if we already had this one
					for (int k = 1; k < pos; k++) {
						if (finalPlans[k] == bpfo) {
							continue outer;
						}
					}
					finalPlans[pos++] = bpfo;
				}
			}
		}
		
		if (PRINT_PRUNING_TRACE)
		{
			PhysicalPlanPrinter printer = new PhysicalPlanPrinter(true);
    		System.out.println("------------------------------------------");
    		System.out.println("- PRUNED PLANS ");
    		System.out.println("------------------------------------------");
    		for(OptimizerPlanOperator plan : finalPlans)
    		{
    			if(plan != null) {
					printer.print(plan);
					System.out.println("------------------------------------------");
				}
				else{
    				throw new ArrayIndexOutOfBoundsException("Identical plans should be pruned");
				}
    		}
		}
		
		return finalPlans;
	}
	
	/**
	 * Utility class to sort plans by the number of interesting order columns.
	 * 
	 * TODO: part of OptimizerUtils
	 */
	public static final class OutOrderColsCountSorter implements Comparator<OptimizerPlanOperator>
	{
		/* (non-Javadoc)
		 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
		 */
		@Override
		public int compare(OptimizerPlanOperator o1, OptimizerPlanOperator o2)
		{
		    int c1 = o1.getColumnOrder() == null ? 0 : o1.getColumnOrder().length;
		    int c2 = o2.getColumnOrder() == null ? 0 : o2.getColumnOrder().length;
			return Integer.compare(c1, c2);
		}
	}

	/**
	 * Creates the inner side of an index-nested-loop join. This method returns a plan,
	 * if the given plan candidate is a table access (not a join plan itself) and if
	 * there is an index available on the join key columns and if the join predicate
	 * is an equality predicate.
	 * 
	 * @param cardEstimator The cardinality estimator required for subplan cardinality estimation
	 * @param planCandidate The sub-plan to create the index-nested-loop join inner plan for.
	 * @param pred The predicate from the join operator.
	 * @param cat The catalogue to search for suitable indexes.
	 * @param requiredColumns The columns that are required in the output.
	 * @param outerJoinColumns The positions of the join key columns in the tuple from 
	 *                         the outer loop. The inner is opened repeatedly, each time 
	 *                         correlated to the current outer tuple, whose join key can
	 *                         be accessed though those positions. 
	 * @return The inner side of an index-nested-loop join.
	 */
	public static OptimizerPlanOperator createIndexNestedLoopJoinInner(CardinalityEstimator cardEstimator,
				OptimizerPlanOperator planCandidate, JoinPredicate pred,
				Catalogue cat, Column[] requiredColumns,
				int[] outerJoinColumns)
	{
		// only possible, if the plan is an immediate table access
		if (!(planCandidate instanceof BaseTableAccess)) {
			return null;
		}
		BaseTableAccess baseTable = (BaseTableAccess) planCandidate;
		List<IndexDescriptor> indexes = cat.getAllIndexesForTable(baseTable.getTable().getTableName());
		
		OptimizerPlanOperator ixscan = null;
		
		if (pred instanceof JoinPredicateAtom)
		{
			// only one column in the join predicate
			JoinPredicateAtom atom = (JoinPredicateAtom) pred;
			if (atom.getParsedPredicate().getOp() != Predicate.Operator.EQUAL) {
				// not possible
				return null;
			}
			Column rightCol = atom.getRightHandColumn();
			
			for (int i = 0; i < indexes.size(); i++) {
				IndexDescriptor ix = indexes.get(i);
				if (ix.getSchema().getColumnNumber() == rightCol.getColumnIndex()) {
					// this index is a match!
					
					// create a fake abstract join to call the cardinality estimator
					// for the per access cardinality
					Relation outer = atom.getLeftHandOriginatingTable();
					
					long outerOriginalOutCard = outer.getOutputCardinality();
					long innerOriginalOutCard = baseTable.getOutputCardinality();
					
					outer.setOutputCardinality(1);
					baseTable.setOutputCardinality(baseTable.getTable().getStatistics().getCardinality());
					
					AbstractJoinPlanOperator fakeJoin = 
						new AbstractJoinPlanOperator(outer, baseTable, atom);
					cardEstimator.estimateJoinCardinality(fakeJoin);
					
					outer.setOutputCardinality(outerOriginalOutCard);
					baseTable.setOutputCardinality(innerOriginalOutCard);
					
					// create the access and any fetch we need
					ixscan = new IndexLookupPlanOperator(ix, baseTable, 
							outerJoinColumns[0], fakeJoin.getOutputCardinality());
					break;
				}
			}
		}
		
		// here comes the part to have an index intersection plan for the nested loops join
		// inner side.
//		else if (pred instanceof OptimizerJoinPredicateConjunct) {
//			OptimizerJoinPredicateConjunct conj = (OptimizerJoinPredicateConjunct) pred;
//			
//			// now build for each index the plan where it alone accesses the index and is followed by a fetch
//			// as well as the one where it is sorted after the RID and is then followed by a fetch
//			for (int i = 0; i < factors.length; i++) {
//				OptimizerLocalPredicate thisPred = factors[i];
//				
//				int colIndex = -1;
//				if (thisPred instanceof OptimizerLocalPredicateAtom) {
//					colIndex = ((OptimizerLocalPredicateAtom) thisPred).getColumnIndex();
//				}
//				else if (thisPred instanceof OptimizerLocalPredicateBetween) {
//					colIndex = ((OptimizerLocalPredicateBetween) thisPred).getColumnIndex();
//				}
//				else {
//					continue;
//				}
//				
//				// build the index access with fetch and the index access with sort and fetch
//				long thisCard = (long) (baseCardinality * thisPred.getSelectivity());
//				IndexScanPlanOperator ixscan = createIndexScan(tscan, thisCard, thisPred, colIndex, indexes);
//				if (ixscan != null) {
//					// create a copy of the predicate where the factor that is evaluated by the 
//					// index was dropped from the conjunction
//					OptimizerLocalPredicateConjunct filterConjunct = new OptimizerLocalPredicateConjunct();
//					float sel = 1.0f;
//					for (int k = 0; k < factors.length; k++) {
//						if (k != i) {
//							filterConjunct.addPredicate(factors[k]);
//							sel *= factors[k].getSelectivity();
//						}
//					}
//					filterConjunct.setSelectivity(sel);
//					
//					// create a FETCH operator to retrieve the other columns.
//					FetchPlanOperator unsortedFetch = new FetchPlanOperator(ixscan, requiredCols);
//					FilterPlanOperator unsortedFilter = new FilterPlanOperator(unsortedFetch, 
//							filterConjunct.getNumberOfPredicates() == 1 ? filterConjunct.getPredicates()[0] : filterConjunct, 
//							tscan.getOutputCardinality());
//					plans.add(unsortedFilter);
//					
//					// create a SORT operator that sorts the RIDs and a FETCH above.
//					SortPlanOperator sort = new SortPlanOperator(
//							ixscan, new int[] { 0 }, new boolean[] { true });
//					FetchPlanOperator sortedFetch = new FetchPlanOperator(sort, requiredCols);
//					FilterPlanOperator sortedFilter = new FilterPlanOperator(sortedFetch, 
//							filterConjunct.getNumberOfPredicates() == 1 ? filterConjunct.getPredicates()[0] : filterConjunct, 
//							tscan.getOutputCardinality());
//					plans.add(sortedFilter);
//				}
//				
//			}
//		}
		
		if (ixscan == null) {
			return null;
		}
		
		// now that we have the index access, add a FETCH
		// if the table scan applies also local predicates, we need to fetch their
		// columns as well and apply the local predicates in an additional FILTER
		LocalPredicate adjustedPred = null;
		
		if (baseTable.getPredicate() != null) {
			Map<Integer, Integer> givenCols = new HashMap<>();
			for (int i = 0; i < requiredColumns.length; i++) {
				givenCols.put(requiredColumns[i].getColumnIndex(), i);
			}
			adjustedPred = baseTable.getPredicate().createCopyAdjustedForProjectedTuple(givenCols);
			
			// if we have more columns for the predicates, add them
			if (givenCols.size() > requiredColumns.length) {
				// the predicates added required columns
				Column[] newCols = new Column[givenCols.size()];
				Iterator<Map.Entry<Integer, Integer>> iter = givenCols.entrySet().iterator();
				while (iter.hasNext()) {
					Map.Entry<Integer, Integer> entry = iter.next();
					int outpos = entry.getValue();
					if (outpos < requiredColumns.length) {
						newCols[outpos] = requiredColumns[outpos];
					}
					else {
						int colIdx = entry.getKey();
						newCols[outpos] = new Column(baseTable,
								baseTable.getTable().getSchema().getColumn(colIdx).getDataType(),
								colIdx);
					}
				}
				requiredColumns = newCols;
			}
		}
		
		// create a FETCH operator to retrieve the other columns.
		FetchPlanOperator fetch = new FetchPlanOperator(ixscan, baseTable, requiredColumns);

		// add the FILTER, if neccesary
		if (adjustedPred != null) {
			FilterPlanOperator filter = new FilterPlanOperator(fetch, adjustedPred);
			return filter;
		}
		else {
			return fetch;
		}
	}
}
