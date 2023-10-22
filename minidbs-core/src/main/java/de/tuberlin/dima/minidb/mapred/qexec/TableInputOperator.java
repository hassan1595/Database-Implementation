package de.tuberlin.dima.minidb.mapred.qexec;

import de.tuberlin.dima.minidb.DBInstance;
import de.tuberlin.dima.minidb.catalogue.TableDescriptor;
import de.tuberlin.dima.minidb.catalogue.TableSchema;

/**
 * This is a very simple BlockProcessingOperator that performs no work and
 * only returns the name of an existing table. It is used to 
 * 
 * @author mheimel
 *
 */
public class TableInputOperator extends BulkProcessingOperator {

	public TableInputOperator(DBInstance instance, String input_table) {
		super(instance);
		result_table_name = input_table;
	}
	
	@Override
	protected TableSchema createResultSchema() {
		// Make sure that the table exists.
		TableDescriptor desc = instance.getCatalogue().getTable(result_table_name);
		if (desc == null) return null;
		// Now fetch the original schema.
		TableSchema original_schema = desc.getSchema();
		// And build a copy.
		TableSchema result_schema = new TableSchema(
				original_schema.getPageSize());
		for (int i=0; i < original_schema.getNumberOfColumns(); ++i) {
			result_schema.addColumn(original_schema.getColumn(i));
		}
		return result_schema;
	}

	@Override
	public boolean run() {
		// Don't do anything here - in particular don't try to allocate the
		// result table.
		return true;
	}

	@Override
	protected boolean operatorMain() {
		return true;	// Unused for this operator.
	}
	
}
