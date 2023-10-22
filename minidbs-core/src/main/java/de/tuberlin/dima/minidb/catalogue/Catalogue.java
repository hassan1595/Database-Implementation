package de.tuberlin.dima.minidb.catalogue;


import java.beans.ExceptionListener;
import java.beans.XMLDecoder;
import java.beans.XMLEncoder;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import de.tuberlin.dima.minidb.Constants;
import de.tuberlin.dima.minidb.catalogue.beans.IndexDescriptorBean;
import de.tuberlin.dima.minidb.catalogue.beans.TableDescriptorBean;
import de.tuberlin.dima.minidb.catalogue.beans.TableStatisticsBean;


/**
 * This class describes the catalogue, which contains descriptors for all tables and
 * indexes available to the system.
 * The catalogue persists these descriptors, or rather the none transient fields. Transient
 * fields are the resource managers and the internal IDs, which have to be recreated or
 * reassigned when the DBMS starts.
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class Catalogue
{	
	// --------------------------------------------------------------------------------------------
	//                           Contained Tables and Indexes
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Map giving access to all table descriptors via the table name.
	 */
	private Map<String, TableDescriptor> tables;
	
	/**
	 * The map giving access to the indexes via their name.
	 */
	private Map<String, IndexDescriptor> indexes;
	
	/**
	 * The map giving access to the indexes depending on which table they belong to.
	 */
	private Map<String, List<IndexDescriptor>> indexesPerTable;
	
	/**
	 * The highest yet encountered resource-ID.
	 */
	private int highestResourceId = 1;
	
	/**
	 * Adds a table descriptor to this catalogue. The table can be accessed through its
	 * name afterwards.
	 * 
	 * @param descr The table descriptor to be added to the catalogue.
	 */
	public synchronized void addTable(TableDescriptor descr)
	{
		this.tables.put(descr.getTableName().toLowerCase(Constants.CASE_LOCALE), descr);
	}
	
	/**
	 * Gets the descriptor for a table from the catalogue by the table name.
	 * 
	 * @param tableName The name of the table to be looked up.
	 * @return The descriptor for the table with the given name, or null, if the table
	 *         is not known by the catalogue.
	 */
	public synchronized TableDescriptor getTable(String tableName)
	{
		tableName = tableName.toLowerCase(Constants.CASE_LOCALE);
		return this.tables.get(tableName);
	}
	
	/**
	 * Removes the descriptor for the table with the given name from the catalogue. If no
	 * table with that name is known by the catalogue, nothing happens.
	 * 
	 * @param tableName The name of the table to be removed.
	 * @return The table descriptor that was removed, or null, if the table was not contained.
	 */
	public synchronized TableDescriptor removeTable(String tableName)
	{
		tableName = tableName.toLowerCase(Constants.CASE_LOCALE);
		return this.tables.remove(tableName);
	}
	
	/**
	 * Gets an iterator over all tables known by this catalogue.
	 * 
	 * @return An iterator over all tables known by this catalogue.
	 */
	public synchronized Iterator<TableDescriptor> getAllTables()
	{
		return this.tables.values().iterator();
	}
	
	/**
	 * Adds an index descriptor to this catalogue. The index descriptor is afterwards
	 * available through its name and the table that the index was created on.
	 * 
	 * @param descr The index descriptor to be added to the catalogue.
	 */
	public synchronized void addIndex(IndexDescriptor descr)
	{
		// add to the 'by-name' map
		this.indexes.put(descr.getName().toLowerCase(Constants.CASE_LOCALE), descr);
		
		// add to the 'by-table' map
		String tableName = descr.getIndexedTableName().toLowerCase(Constants.CASE_LOCALE);
		List<IndexDescriptor> list = this.indexesPerTable.get(tableName);
		if (list == null) {
			list = new ArrayList<IndexDescriptor>();
			this.indexesPerTable.put(tableName, list);
		}
		list.add(descr);
	}
	
	/**
	 * Gets the descriptor for the index with the given name.
	 * 
	 * @param indexName The name of the index to get the descriptor for.
	 * @return The descriptor for the index with the given name, or null, if no such index
	 *         is known by the catalogue.
	 */
	public synchronized IndexDescriptor getIndex(String indexName)
	{
		indexName = indexName.toLowerCase(Constants.CASE_LOCALE);
		return this.indexes.get(indexName);
	}
	
	/**
	 * Gets all indexes created on the table with the given name that are known to the
	 * catalogue. If no index for the table with the given name is known to the catalogue,
	 * this method returns an empty list.
	 * 
	 * @param tableName The name of the table to get the indexes for.
	 * @return A list with all indexes on the table with the given name.
	 */
	public synchronized List<IndexDescriptor> getAllIndexesForTable(String tableName)
	{
		tableName = tableName.toLowerCase(Constants.CASE_LOCALE);
		List<IndexDescriptor> l = this.indexesPerTable.get(tableName);
		if (l == null) {
			return Collections.<IndexDescriptor>emptyList();
		}
		else {
			return new ArrayList<IndexDescriptor>(l);
		}
	}
	
	/**
	 * Removes the index descriptor for the index with the given name from the catalogue.
	 * 
	 * @param indexName The name of the index whose descriptor is to be removed.
	 * @return The removed index descriptor, or null, if no index with the given name is
	 *         known to the catalogue.
	 */
	public synchronized IndexDescriptor removeIndex(String indexName)
	{
		// remove from the 'by-name' map
		indexName = indexName.toLowerCase(Constants.CASE_LOCALE);
		IndexDescriptor descr = this.indexes.remove(indexName);
		
		// remove from the 'by-table' map
		if (descr != null) {
			String tableName = descr.getIndexedTableName().toLowerCase(Constants.CASE_LOCALE);
			List<IndexDescriptor> list = this.indexesPerTable.get(tableName);
			
			if (list == null) {
				throw new RuntimeException("The catalogue has come to an inconsistent state.");
			}
			
			list.remove(descr);
			if (list.isEmpty()) {
				this.indexesPerTable.remove(tableName);
			}
		}
		
		// return the removed descriptor
		return descr;
	}
	
	/**
	 * Removes from the catalogue all indexes that were created on the table with the given
	 * name.
	 *  
	 * @param tableName The name of the table whose indexes are to be removed.
	 * @return A list containing all index descriptors that were removed from the catalogue.
	 */
	public synchronized List<IndexDescriptor> removeAllIndexesForTable(String tableName)
	{
		// remove from the 'by-table' map
		tableName = tableName.toLowerCase(Constants.CASE_LOCALE);
		List<IndexDescriptor> list = this.indexesPerTable.get(tableName);
		
		if (list == null) {
			// no indexes contained for the table
			return Collections.<IndexDescriptor>emptyList();
		}
		else {
			// remove from the 'by-name' map
			for (int i = 0; i < list.size(); i++) {
				String name = list.get(i).getName().toLowerCase(Constants.CASE_LOCALE);
				IndexDescriptor removed = this.indexes.remove(name);
				if (removed == null) {
					throw new RuntimeException("The catalogue has come to an inconsistent state.");
				}
			}
			
			return list;
		}
	}
	
	/**
	 * Returns an iterator over all indexes known by this catalogue.
	 * 
	 * @return An iterator over all indexes.
	 */
	public synchronized Iterator<IndexDescriptor> getAllIndexes()
	{
		return this.indexes.values().iterator();
	}
	
	/**
	 * Gets the next resource id to be used.
	 * 
	 * @return The next resource id to be used.
	 */
	public synchronized int reserveNextId()
	{
		this.highestResourceId++;
		return this.highestResourceId;
	}
	
	
	// --------------------------------------------------------------------------------------------
	//                                   Instantiation
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Creates a new empty catalogue containing no table- or index schemas.
	 */
	protected Catalogue() 
	{
		this.tables = new HashMap<String, TableDescriptor>();
		this.indexes = new HashMap<String, IndexDescriptor>();
		this.indexesPerTable = new HashMap<String, List<IndexDescriptor>>();
	}
	
	
	// --------------------------------------------------------------------------------------------
	//                         Serialization / Deserialization
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Persists the catalogue in XML format to a file as described by the given <tt>File</tt>
	 * object. All previous contents of the file is overwritten.
	 * 
	 * @param xmlCatalogueFile The abstract path to the file that the catalogue will be
	 *                         persisted to.
	 * @throws FileNotFoundException Thrown, when the file described by the given abstract path
	 *                               cannot be opened/created because the path to it is not found.
	 * @throws IOException Thrown, if an I/O problem occurred while opening the file or writing
	 *                     to it.
	 */
	public synchronized void writeCatalogue(File xmlCatalogueFile) throws IOException
	{
		// create an XML encoder to write to the target file
		// overwrite the whole file
		XMLEncoder encoder = new XMLEncoder(new BufferedOutputStream(
				new FileOutputStream(xmlCatalogueFile)));
		
		// go over all tables and write persistent fields
		Iterator<TableDescriptor> tableIter = getAllTables();
		while (tableIter.hasNext()) {
			TableDescriptor entry = tableIter.next();
			
			// we create a bean with the relevant 
			TableDescriptorBean bean = new TableDescriptorBean();
			bean.setTableName(entry.getTableName());
			bean.setTableFileName(entry.getFileName());
			
			if (entry.getStatistics() != null) {
				bean.setStatistics(new TableStatisticsBean(entry.getStatistics()));
			}
			else {
				bean.setStatistics(new TableStatisticsBean());
			}
			
			// write the bean
			encoder.writeObject(bean);
		}
		
		// go over all indexes and write persistent fields
		Iterator<IndexDescriptor> indexIter = getAllIndexes();
		while (indexIter.hasNext()) {
			IndexDescriptor entry = indexIter.next();
			
			// we create a bean with the relevant 
			IndexDescriptorBean bean = new IndexDescriptorBean();
			bean.setIndexName(entry.getName());
			bean.setIndexFileName(entry.getFileName());
			bean.setIndexedTableName(entry.getIndexedTableName());
			bean.setStatistics(entry.getStatistics());
			
			// write the bean
			encoder.writeObject(bean);
		}
		
		encoder.close();
	}
	
	
	/**
	 * Loads the catalogue from an XML file. The format of the XML file is a sequence of
	 * <tt>TableDescriptorBean</tt> and <tt>IndexDescriptorBean</tt>, persisted through the
	 * Java Bean XML persistence API. The returned catalogue contains only names, paths and
	 * statistics. The runtime structures will yet have to be initialized.
	 * 
	 * @param xmlCatalogueFile The file that the catalogue is stored in.
	 * @return An instance of the catalogue, containing the descriptors for the tables and indexes
	 *         with no transient runtime fields, but only names, paths and statistics. 
	 * @throws FileNotFoundException Thrown, when the file described by the given abstract path
	 *                               cannot be opened because the path to it is not found.
	 * @throws IOException Thrown, if an I/O problem occurred while opening the file or reading
	 *                     from it.
	 * @throws CatalogueFormatException Thrown, if the contents of the file did not describe a
	 *                                  well formed catalogue.
	 */
	public static Catalogue loadCatalogue(InputStream xmlCatalogueFile)
	throws FileNotFoundException, IOException, CatalogueFormatException
	{
		// create an exception listener to intercept exceptions
		DeserializationExceptionListener listener = new DeserializationExceptionListener();
		
		// lists to collect the read information
		List<TableDescriptorBean> tables = new ArrayList<TableDescriptorBean>();
		List<IndexDescriptorBean> indexes = new ArrayList<IndexDescriptorBean>();
		XMLDecoder decoder = new XMLDecoder(xmlCatalogueFile, Catalogue.class, listener);
		// create a decoder for the XML file
		try {
		
			// first, read all the catalogue into the lists
		
			while (true) {
				// get the next object from the catalogue file
				Object o = decoder.readObject();
				if (o instanceof TableDescriptorBean) {
					// table info
					tables.add((TableDescriptorBean) o);
				}
				else if (o instanceof IndexDescriptorBean) {
					// index info
					indexes.add((IndexDescriptorBean) o);
				}
				else {
					String name = o == null ? "Null Object" : o.getClass().getName();
					throw new CatalogueFormatException("Unrecognized data element of type '" +
							name + "'.");
				}
			}
		}
		catch (ArrayIndexOutOfBoundsException aioobex) {
			// we are done reading, do nothing
		}finally{
			decoder.close();
		}
		
		// check if all went well
		if (listener.getException() != null) {
			throw new CatalogueFormatException("Failed to create described data objects.",
					listener.getException());
		}
		
		// create the catalogue and add the tables and indexes
		Catalogue cat = new Catalogue();
		
		for (int i = 0; i < tables.size(); i++) {
			TableDescriptorBean bean = tables.get(i);
			TableDescriptor td = new TableDescriptor(bean.getTableName(),
					bean.getTableFileName(), bean.getStatistics());
			cat.addTable(td);
		}
		
		for (int i = 0; i < indexes.size(); i++) {
			IndexDescriptorBean bean = indexes.get(i);
			IndexDescriptor id = new IndexDescriptor(bean.getIndexName(),
					bean.getIndexedTableName(), bean.getIndexFileName(),
					bean.getStatistics());
			cat.addIndex(id);
		}
		
		return cat;
	}
	
	/**
	 * Helper class to catch exceptions during deserialization.
	 */
	private static class DeserializationExceptionListener implements ExceptionListener
	{
		/**
		 * The first exception caught by this listener or null, if none occurred yet.
		 */
		private Exception exception;
		
		/**
		 * Gets this listener's exception.
		 * 
		 * @return The first exception caught by this listener or null, if none occurred yet.
		 */
		public Exception getException()
		{
			return this.exception;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.beans.ExceptionListener#exceptionThrown(java.lang.Exception)
		 */
		@Override
		public void exceptionThrown(Exception e)
		{
			if (this.exception == null) {
				this.exception = e;
			}
		}
	}

}
