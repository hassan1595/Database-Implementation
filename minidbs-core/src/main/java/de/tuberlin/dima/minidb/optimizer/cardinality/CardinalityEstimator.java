/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package de.tuberlin.dima.minidb.optimizer.cardinality;

import de.tuberlin.dima.minidb.optimizer.AbstractJoinPlanOperator;
import de.tuberlin.dima.minidb.semantics.BaseTableAccess;

/**
 * @author Alexander Alexandrov (alexander.alexandrov@tu-berlin.de)
 *
 */
public interface CardinalityEstimator {
	
	/**
	 * The default selectivity for range predicates, used when no estimation can be
	 * made because of missing or inapplicable statistics.
	 */
	float DEFAULT_RANGE_PREDICATE_SELECTIVITY = 0.33333f;
	
	/**
	 * The minimal selectivity to be applied when range predicate selectivity fails
	 * due to inapplicable bounds.
	 */
	float MINIMAL_RANGE_PREDICATE_SELECTIVITY = 0.01f;
	
	/**
	 * The default selectivity estimate of an equi-join. Multiplied to the cardinality
	 * of the cross product.
	 */
	float DEFAULT_EQUI_JOIN_SELECTIVITY = 0.1f;
	
	/**
	 * The default selectivity for a join with any other predicate than an equality predicate.
	 */
	float DEFAULT_NON_EQUI_JOIN_SELECTIVITY = 0.33333f;

	/**
	 * Estimates the cardinality of a base table access. This method sets the
	 * <tt>input cardinality</tt> and the <tt>output cardinality</tt> of the
	 * scan operator.
	 * 
	 * @param tableAccess The operator with table access and predicates.
	 */
	void estimateTableAccessCardinality(BaseTableAccess tableAccess);

	/**
	 * Estimates the cardinality of a join operator. The join is an inner join.
	 * 
	 * @param operator The join operator for which to estimate the output cardinality.
	 */
	void estimateJoinCardinality(AbstractJoinPlanOperator operator);

}