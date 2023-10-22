package de.tuberlin.dima.minidb.optimizer.joins.util;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import de.tuberlin.dima.minidb.parser.Predicate.Operator;
import de.tuberlin.dima.minidb.semantics.Column;
import de.tuberlin.dima.minidb.semantics.predicate.JoinPredicate;
import de.tuberlin.dima.minidb.semantics.predicate.JoinPredicateAtom;
import de.tuberlin.dima.minidb.semantics.predicate.JoinPredicateConjunct;

public class JoinOrderOptimizerUtils
{
	/**
	 * Filters out twin predicates from conjunctive join predicates, i.e. transforms 
	 * predicates of the form (A.x = B.x AND B.x = C.x) to the simplified (A.x = B.x) 
	 * semantically equivalent version.
	 * 
	 * @param joinPredicate the input JoinPredicate
	 * @return the filtered JoinPredicate
	 */
	public static JoinPredicate filterTwinPredicates(JoinPredicate joinPredicate)
    {
		if (!(joinPredicate instanceof JoinPredicateConjunct))
		{
			return joinPredicate;
		}
		
		JoinPredicateConjunct predicate = (JoinPredicateConjunct) joinPredicate;
		
		// two hash maps to store the associates of equi-join atoms  
		// from left to right and vise versa
		Map<Column, List<JoinPredicateAtom>> lAssociates = new HashMap<Column, List<JoinPredicateAtom>>();
		Map<Column, List<JoinPredicateAtom>> rAssociates = new HashMap<Column, List<JoinPredicateAtom>>();
		
		List<JoinPredicateAtom> conjunctiveFactors = predicate.getConjunctiveFactors();
		Collections.sort(conjunctiveFactors);
		
		for(JoinPredicateAtom p : conjunctiveFactors)
		{
			if (p.getParsedPredicate().getOp() != Operator.EQUAL)
			{
				continue;
			}
			
			Column lColumn = p.getLeftHandColumn();
			Column rColumn = p.getRightHandColumn();
			
			if (!lAssociates.containsKey(lColumn))
			{
				lAssociates.put(lColumn, new LinkedList<JoinPredicateAtom>());
			}
			if (!rAssociates.containsKey(rColumn))
			{
				rAssociates.put(rColumn, new LinkedList<JoinPredicateAtom>());
			}
			
			lAssociates.get(lColumn).add(p);
			rAssociates.get(rColumn).add(p);
		}
		
		JoinPredicateConjunct filteredPredicate = new JoinPredicateConjunct();
		
		// leave only the first associate of each equi-join predicate
		for(JoinPredicateAtom p : conjunctiveFactors)
		{
			if (p.getParsedPredicate().getOp() != Operator.EQUAL)
			{
				filteredPredicate.addJoinPredicate(p);
			}
			else
			{
				List<JoinPredicateAtom> lAssociatesForP = lAssociates.get(p.getLeftHandColumn());
				List<JoinPredicateAtom> rAssociatesForP = rAssociates.get(p.getRightHandColumn());
				
				boolean isFirstLeftAssociate = (lAssociatesForP.size() == 1 || lAssociatesForP.get(0) == p);
				boolean isFirstRightAssociate = (rAssociatesForP.size() == 1 || rAssociatesForP.get(0) == p);
				
				if (isFirstLeftAssociate && isFirstRightAssociate)
				{
					filteredPredicate.addJoinPredicate(p);
				}
			}
		}
		
		if (filteredPredicate.getConjunctiveFactors().size() > 1)
		{
			return filteredPredicate;
		}
		else
		{
			return filteredPredicate.getConjunctiveFactors().get(0);
		}
    }
}
