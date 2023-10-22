package de.tuberlin.dima.minidb.io.index;


/**
 * An enumeration for the different types of pages that can occur in the B+-Tree index.
 */
public enum BTreeIndexPageType
{
	/**
	 * Enumeration element indicating a page for an inner node in the tree.
	 */
	INNER_NODE_PAGE,
	
	/**
	 * Enumeration element indicating a page for a leaf node in the tree.
	 */
	LEAF_PAGE;
}