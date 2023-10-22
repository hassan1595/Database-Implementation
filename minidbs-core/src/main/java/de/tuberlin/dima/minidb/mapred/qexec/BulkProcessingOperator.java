package de.tuberlin.dima.minidb.mapred.qexec;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import de.tuberlin.dima.minidb.DBInstance;
import de.tuberlin.dima.minidb.catalogue.TableDescriptor;
import de.tuberlin.dima.minidb.catalogue.TableSchema;
import de.tuberlin.dima.minidb.io.manager.BufferPoolException;
import de.tuberlin.dima.minidb.io.tables.TableResourceManager;

/**
 * Abstract class that implements an operator that processes a complete table 
 * when being called.
 * 
 * Children can be registered by calling addChild(...).
 * 
 * The operator can be started by calling run().
 * 
 * @author mheimel
 *
 */
public abstract class BulkProcessingOperator {
	
	/**
	 * Instance of the MiniDBs we are running in.
	 */
	protected DBInstance instance;
	public BulkProcessingOperator(DBInstance instance) {
		this.instance = instance;
		// Select a new random table name.
		this.result_table_name = "result_" + new Random().nextInt();	
	}
	
	protected List<BulkProcessingOperator> children = 
			new LinkedList<BulkProcessingOperator>();
	
	/**
	 * Utility function to add a child to the operator.
	 * 
	 * @param child
	 */
	public void addChild(BulkProcessingOperator child) {
		children.add(child);
	}
	
	/**
	 * Run all children and allocate the result table.
	 * 
	 * @return True iff everything went right, false otherwise.
	 * @throws BufferPoolException 
	 * @throws IOException 
	 */
	public boolean run() throws IOException, BufferPoolException {
		// Start all children.
		for (BulkProcessingOperator child : children) {
			if (!child.run()) return false;
		}
		// Now allocate our result table.
		allocateResultTable();
		// Now run this operator.
		return operatorMain();
	}
	
	/**
	 * Name of the result table for this operator.
	 */
	protected String result_table_name;
	public String getResultTableName() {
		return result_table_name;
	}
	
	/**
	 * Helper function to allocate the result table.
	 * 
	 * @throws IOException 
	 * @throws BufferPoolException 
	 */
	private void allocateResultTable() throws IOException, BufferPoolException {
		TableSchema schema = getResultSchema();
		// Prepare a new table file in the tmp directory and ensure it is empty.
		File result_table_file = new File(System.getProperty("java.io.tmpdir"), 
				result_table_name + ".tbl");
		if (result_table_file.exists()) result_table_file.delete();
		// Now, allocate a new TableResourceManager.
		TableResourceManager res = TableResourceManager.createTable(
				result_table_file, schema);
		// And register the table.
		TableDescriptor desc = new TableDescriptor(
				result_table_name, result_table_file.getAbsolutePath());
		instance.getCatalogue().addTable(desc);
		int resID = instance.getCatalogue().reserveNextId();
		instance.getBufferPool().registerResource(resID, res);
		desc.setResourceProperties(res, resID);
	}
	
	/**
	 * Cache for the result table schema.
	 */
	private TableSchema result_schema;
	/**
	 * Used to fetch the result schema of this operator.
	 * 
	 * @return Schema of the table that is produced by this operator.
	 */
	public final TableSchema getResultSchema() {
		if (result_schema == null)
			result_schema = createResultSchema();
		return result_schema;
	}
	
	/**
	 * This function has to be implemented by all deriving classes and has
	 * to return the schema of the table that is produced by this operator.
	 * 
	 * It guarantees that the BlockProcessing operator can allocate the correct
	 * result table to store results.
	 * 
	 * @return Schema of the table that is produced by this operator.
	 */
	protected abstract TableSchema createResultSchema();
	
	/**
	 * This function has to be implemented by all deriving classes.
	 * It is used to implement the main operator functionality.
	 * @return True iff the operator finished succesfully.
	 */
	protected abstract boolean operatorMain();

}
