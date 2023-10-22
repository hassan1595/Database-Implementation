package de.tuberlin.dima.minidb.mapred.qexec.examples;

import java.io.IOException;

import org.apache.hadoop.io.Text;

import de.tuberlin.dima.minidb.DBInstance;
import de.tuberlin.dima.minidb.catalogue.TableSchema;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.mapred.qexec.BulkProcessingOperator;
import de.tuberlin.dima.minidb.mapred.qexec.HadoopOperator;

/**
 * An exemplary operator that does nothing but forward its child.
 
 * This example should demonstrate how to correctly set up a Hadoop operator.
 * 
 * @author mheimel
 *
 */
public class IdentityOperator extends HadoopOperator<Text, DataTuple> {

	private BulkProcessingOperator input;
	
	public IdentityOperator(DBInstance dbInstance, 
			BulkProcessingOperator input) throws IOException {
		super(dbInstance, input);
		this.input = input;
	}

	@Override
	protected TableSchema createResultSchema() {
		return input.getResultSchema();
	}
	
}
