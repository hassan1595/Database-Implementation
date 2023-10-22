package de.tuberlin.dima.minidb.io.index;


import de.tuberlin.dima.minidb.Constants;
import de.tuberlin.dima.minidb.catalogue.IndexSchema;
import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataType;
import de.tuberlin.dima.minidb.core.IntField;
import de.tuberlin.dima.minidb.io.cache.PageExpiredException;
import de.tuberlin.dima.minidb.io.cache.PageFormatException;


/**
 * Implementation of an inner-node page of a B-Tree index.
 * 
 * Once a page for the inner node has been initialized, the first content is added to the
 * inner node in either of the two ways:
 * <ul>
 *   <li>The node contents is set to a single key with two pointers. That happens when the
 *       node is designated to be a root node.</li>
 *   <li>A number of keys/pointers is moved to the page from another node that
 *       encountered an overflow.</li>
 * </ul>  
 * 
 * The basic inner node page implies the following header:
 * <ul>
 *   <li>Bytes 0 - 3 are an INT (little endian) holding the magic number for index pages.</li>
 *   <li>Bytes 4 - 7 are an INT (little endian) holding the page number.</li>
 *   <li>Bytes 8 - 11 are an INT (little endian) holding type information that identifies the
 *       page as a inner node page. The specific value to be held for leaf pages is 1.</li>
 *   <li>Bytes 12 - 15 are an INT (little endian) holding the number of keys in the node.</li> 
 * </ul> 
 * When the class implementing this interface is instantiated, it should verify the header. 
 * 
 * Many of the defined methods may throw <code>IndexFormatCorruptException</code> when encountering that
 * index conditions (such as the sorted order) are violated. The methods should however not explicitly
 * check everything, but only throw the exception when stumbling upon a condition that is obviously
 * due to violation of the index conditions.
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class BTreeInnerNodePage implements BTreeIndexPage
{
	// ------------------------------------------------------------------------
	//                         public constants
	// ------------------------------------------------------------------------
	
	/**
	 * The constant indicating a inner node page when found in the header.
	 */
	public static final int HEADER_TYPE_VALUE = 1;
	
	
	// ------------------------------------------------------------------------
	//                         private constants
	// ------------------------------------------------------------------------
	
	/**
	 * Size of the header in this page.
	 */
	private static final int HEADER_SIZE = BTreeIndexPage.INDEX_PAGE_HEADER_SIZE;
	
	/**
	 * The offset of the field holding the page number.
	 */
	private static final int HEADER_PAGE_NUMBER_OFFSET = 4;
	
	/**
	 * The offset of the field holding the entries counter.
	 */
	private static final int HEADER_NUM_KEYS_OFFSET = 12;
	
	/**
	 * The width of the page number.
	 */
	private static final int PAGE_NUMBER_WIDTH = DataType.intType().getNumberOfBytes();


	
	// ------------------------------------------------------------------------
	//                             attributes
	// ------------------------------------------------------------------------
	
	/**
	 * The buffer containing the binary page data.
	 */
	private final byte[] buffer;
	
	/**
	 * The data type of the key.
	 */
	private final DataType keyType;
	
	/**
	 * The maximal number of keys in the node.
	 */
	private final int maxKeys;
	
	/**
	 * The width of a key in bytes.
	 */
	private final int keyWidth;
	
	/**
	 * The offset to the point where the sequence of pointers begins.
	 */
	private final int pointerSequenceOffset;
	
	/**
	 * The current number of keys.
	 */
	private int numKeys;
	
	/**
	 * Flag indicating this index page has unique keys
	 */
	private final boolean unique;
	
	/**
	 * A flag describing if the contents of the page has been modified since its creation.
	 */
	private boolean modified;
	
	/**
	 * Flag marking this cacheable data object as expired.
	 */
	private boolean expired;
	
	
	// ------------------------------------------------------------------------
	//                           constructors & set up
	// ------------------------------------------------------------------------
	
	/**
	 * Creates a new {@link BTreeInnerNodePage} around the binary data in the given buffer using
	 * the information in the given schema.
	 *  
	 * @param schema The schema of the index to create the page for. 
	 * @param buffer The buffer containing the binary data.
	 * @throws PageFormatException Thrown if the page header contained illegal data.
	 */
	public BTreeInnerNodePage(IndexSchema schema, byte[] buffer)
	throws PageFormatException
	{
		this.buffer = buffer;
		this.keyType = schema.getIndexedColumnSchema().getDataType();
		this.maxKeys = schema.getFanOut();

		this.keyWidth = this.keyType.getNumberOfBytes();

		this.pointerSequenceOffset = HEADER_SIZE + (this.maxKeys * this.keyWidth);

		this.numKeys = IntField.getIntFromBinary(buffer, HEADER_NUM_KEYS_OFFSET);
		if (this.numKeys < 0) {
			throw new PageFormatException("Index page header contained a negative count of keys.");
		}

		this.unique = schema.isUnique();
		this.modified = false;
		this.expired = false;
	}
	
	// ------------------------------------------------------------------------
	//                     generic cacheable data behavior
	// ------------------------------------------------------------------------
	
	/*
	 * (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.io.cache.CacheableData#hasBeenModified()
	 */
	@Override
	public boolean hasBeenModified()
	{
		return this.modified;
	}

	/*
	 * (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.io.cache.CacheableData#markExpired()
	 */
	@Override
	public void markExpired()
	{
		this.expired = true;
	}

	/*
	 * (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.io.cache.CacheableData#isExpired()
	 */
	@Override
	public boolean isExpired()
	{
		return this.expired;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.tuberlin.dima.minidb.io.cache.CacheableData#getPageID()
	 */
	@Override
	public int getPageNumber()
	{
		if (Constants.DEBUG_CHECK && this.expired) {
			throw new PageExpiredException();
		}
		
		return IntField.getIntFromBinary(this.buffer, HEADER_PAGE_NUMBER_OFFSET);
	}
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.io.cache.CacheableData#getBuffer()
	 */
	@Override
	public byte[] getBuffer()
	{
		return this.buffer;
	}

	// ------------------------------------------------------------------------
	//                                  keys
	// ------------------------------------------------------------------------

	/**
	 * Gets the number of keys in this node.
	 * 
	 * @return The number of keys.
	 */
	public int getNumberOfKeys()
	{
		return this.numKeys;
	}

	/**
	 * Gets the first (lowest) key on the page, or null, if the page is empty.
	 * 
	 * @return The first key on the page.
	 */
	public DataField getFirstKey()
	{
		if (Constants.DEBUG_CHECK && this.expired) {
			throw new PageExpiredException();
		}
		
		return this.numKeys == 0 ? null : this.keyType.getFromBinary(this.buffer, HEADER_SIZE, this.keyWidth);
	}

	/**
	 * Gets the last (highest) key on the page, or null, if the page is empty.
	 * 
	 * @return The last key on the page.
	 */
	public DataField getLastKey()
	{
		if (Constants.DEBUG_CHECK && this.expired) {
			throw new PageExpiredException();
		}
		
		return this.numKeys == 0 ? null : this.keyType.getFromBinary(this.buffer, (this.numKeys - 1) * this.keyWidth + HEADER_SIZE, this.keyWidth);
	}

	/**
	 * Gets the key at the given position [0 - k-1], where k is the current number of
	 * entries in the B-Tree.
	 * 
	 * @param position The position of the key to get.
	 * @return The key at the given position.
	 * @throws IndexOutOfBoundsException Thrown, if the position is out of range, i.e.
	 *                                  negative or larger than the number of keys on
	 *                                  the page.
	 */
	public DataField getKey(int position)
	{
		if (Constants.DEBUG_CHECK && this.expired) {
			throw new PageExpiredException();
		}
		
		if (position < 0 || position >= this.numKeys) {
			throw new IndexOutOfBoundsException("Key position '" + position + "' is out of range [0, " + this.numKeys + ").");
		}
		else {
			return uncheckedGetKey(position);
		}
	}

	/**
	 * Sets the key at the given position to the given value.
	 * The position is [0 - k-1], where k is the current number of entries in the B-Tree.
	 * 
	 * @param newKeyValue The new value for the key.
	 * @param position The position of the key to set to the new value.
	 * @throws IndexOutOfBoundsException Thrown, if the position is out of range, i.e.
	 *                                  negative or larger than the number of keys on
	 *                                  the page.
	 */
	public void setKey(DataField newKeyValue, int position)
	{
		if (Constants.DEBUG_CHECK && this.expired) {
			throw new PageExpiredException();
		}
		
		if (position < 0 || position >= this.numKeys) {
			throw new IndexOutOfBoundsException("Key position " + position + " is out of range [0, " + this.numKeys + ").");
		}
		newKeyValue.encodeBinary(this.buffer, position * this.keyWidth + HEADER_SIZE);
		this.modified = true;
	}
	
	
	// ------------------------------------------------------------------------
	//                                 pointers
	// ------------------------------------------------------------------------

	/**
	 * Gets the pointer at the given position [0 - k], where k is the current number of
	 * keys in the node.
	 * 
	 * @param position The position of the pointer to get.
	 * @return The pointer at the given position.
	 * @throws IndexOutOfBoundsException Thrown, if the position is out of range, i.e.
	 *                                  negative or larger than the number of pointers on
	 *                                  the page.
	 */
	public int getPointer(int position)
	{
		if (Constants.DEBUG_CHECK && this.expired) {
			throw new PageExpiredException();
		}
		
		if (position < 0 || position > this.numKeys) {
			throw new IndexOutOfBoundsException("Pointer position '" + position +
					"' is out of range [0, " + this.numKeys + "]");
		}
		else {
			return uncheckedGetPointer(position);
		}
	}
	
	/**
	 * Gets the page number of the child page through which the leaf page containing the
	 * key will be reached.
	 * 
	 * The method finds the pointer associated with lowest key k' in the sequence of
	 * keys that is greater or equal to the given key. If there is no such key, then 
	 * the last pointer is returned.
	 * 
	 * Example: suppose that the node contains the keys <tt>3, 7, 9</tt> and the key to
	 * look for is <tt>5</tt>. The key that is relevant is <tt>7</tt>, which is at
	 * position 1 (counting from 0), so the pointer at that position is returned. If the
	 * given key would be 12, then the last pointer (position 3) would be returned.
	 * 
	 * @param key The key that is sought.
	 * @return The number of the page that leads to that key entry.
	 */
	public int getChildPageForKey(DataField key)
	{
		if (Constants.DEBUG_CHECK && this.expired) {
			throw new PageExpiredException();
		}
		
		int pos = getPointerPositionForKey(key);
		if (pos == -1) {
			// no key contained on page, notify
			throw new IllegalStateException("Attempting to lookup the child on an empty page.");
		}
		else {
			return uncheckedGetPointer(pos);
		}
	}

	/**
	 * Gets the page number of the child page through which the leaf page containing the
	 * key will be reached and the key that is associated with the pointer. 
	 * In addition, the position where the key and pointer were found is included.
	 * 
	 * Example: If the node page is set up like <tt>p0, 4, p1, 18, p2, 87, p3</tt> and
	 * the sought key is 33 then the returned structure contains key <tt>87</tt>, the pointer
	 * <tt>p2</tt> and position <tt>2</tt>.
	 * 
	 * The conditions are the same as for the <code>getChildPageForKey()</code> method.
	 * 
	 * If the sought key is greater than the highest key, the returned object has a null as key
	 * (not the NULL value), the last pointer and a position equal to the number of keys in the 
	 * node. 
	 * 
	 * @param key The key that is sought.
	 * @return The key, page number and pointer position packed into an object.
	 */
	public KeyPageNumberPosition getChildWithKeyAndPosition(DataField key)
	{
		if (Constants.DEBUG_CHECK && this.expired) {
			throw new PageExpiredException();
		}
		
		int pos = getPointerPositionForKey(key);
		if (pos == -1) {
			// no key contained on page, notify
			throw new IllegalStateException("Attempting to lookup the child on an empty page.");
		}
		else if (pos == this.numKeys) {
			return new KeyPageNumberPosition(null, uncheckedGetPointer(pos), pos);
		}
		else {
			return new KeyPageNumberPosition(uncheckedGetKey(pos), uncheckedGetPointer(pos), pos);
		}
	}
	
	
	// ------------------------------------------------------------------------
	//                          Initialization
	// ------------------------------------------------------------------------

	/**
	 * Initializes the contents of the node to be a single key and two corresponding
	 * pointers. This method represents the first way of creating first contents for
	 * a inner node: The creation as a newly spawned root node.
	 * 
	 * Previous contents in the node will be ignored and simply overwritten.
	 *  
	 * @param key The only key contained the node at first.
	 * @param firstPointer The pointer to pages for keys smaller and equal to the given key.
	 * @param secondPointer The pointer to pages for keys greater than the given key.
	 */
	public void initRootState(DataField key, int firstPointer, int secondPointer)
	{
		if (Constants.DEBUG_CHECK && this.expired) {
			throw new PageExpiredException();
		}
		
		// store the key
		key.encodeBinary(this.buffer, HEADER_SIZE);
		IntField.encodeIntAsBinary(firstPointer, this.buffer, this.pointerSequenceOffset);
		IntField.encodeIntAsBinary(secondPointer, this.buffer, this.pointerSequenceOffset + PAGE_NUMBER_WIDTH);

		this.numKeys = 1;
		persistNumKeys();
		this.modified = true;
	}

	/**
	 * Moves the last num keys and pointers (= page numbers) to the new page.
	 * The old page remains valid that way (numPointer = numKeys + 1) and in order to
	 * create a valid inner node in the new page, the first of the moved keys is dropped.
	 * The dropped key is returned from the method instead.
	 * <p>
	 * This method represents the second variant that contents may first be added to an
	 * inner node - by creating the node to hold the second half of another node
	 * after a node overflow and split.
	 * <p>
	 * Previous contents of the node will be ignored and overwritten. After the call to this
	 * function, the newPage will contain exactly <code>num - 1</code> keys and
	 * <code>num</code> pointers. 
	 * 
	 * @param newPage The page to move the (key/pointer) pairs to.
	 * @param num The number of pairs to move.
	 * @return The value of the key that was dropped in order to create a valid new page.
	 * @throws PageFormatException Thrown, if the page was identified to be corrupted or
	 *                             inconsistent in any way that prevents consistent processing.
	 */
	public DataField moveLastToNewPage(BTreeInnerNodePage newInnerNode, int num) throws PageFormatException
	{
		if (Constants.DEBUG_CHECK && this.expired) {
			throw new PageExpiredException();
		}
		
		// checks
		if (num < 2) {
			throw new IllegalArgumentException("The number of keys to move must be at least 2");
		}
		else if (this.numKeys == 0) {
			throw new IllegalStateException("Method moveLastToNewPage(...) is not applicable to empty nodes.");
		}
		else if (num > this.numKeys) {
			throw new IllegalArgumentException("Cannot move " + num + " keys/pointers. Not enough keys/pointers on this node.");
		}
		else if (this.keyType != newInnerNode.keyType || this.keyWidth != newInnerNode.keyWidth) {
			throw new IllegalArgumentException("Incompatible nodes from indexes of a different schema.");
		}

		// copy the num - 1 keys to the new node
		System.arraycopy(this.buffer, (this.numKeys - num + 1) * this.keyWidth + HEADER_SIZE, newInnerNode.buffer, HEADER_SIZE, (num - 1) * this.keyWidth);
		// copy the num pointers to the new node
		System.arraycopy(this.buffer, (this.numKeys - num + 1) * PAGE_NUMBER_WIDTH + this.pointerSequenceOffset, newInnerNode.buffer,
				this.pointerSequenceOffset, num * PAGE_NUMBER_WIDTH);

		// update our header
		this.numKeys -= num;
		persistNumKeys();
		this.modified = true;

		// update the new page's header
		newInnerNode.numKeys = num - 1;
		IntField.encodeIntAsBinary(num - 1, newInnerNode.buffer, HEADER_NUM_KEYS_OFFSET);
		newInnerNode.modified = true;

		// return the one key that is too much in this node
		return uncheckedGetKey(this.numKeys);
	}
	
	
	// ------------------------------------------------------------------------
	//                                Inserts
	// ------------------------------------------------------------------------

	/**
	 * Inserts a (key/pointer) pair into the node. Valid positions are between 
	 * and including <tt>0</tt> .. <tt>getNumberOfKeys()</tt>.
	 * If the given key ends up at position <code>i</code> in the
	 * sequence of keys, then the pointer will end at position <code>i+1</code>.
	 * 
	 * In the case that the key is already contained in the sequence (possibly
	 * multiple times), the key is placed before all the existing keys and the pointer
	 * is placed correspondingly.
	 * 
	 * The method returns false, if the insert was not possible because the
	 * node if full.
	 * 
	 * NOTE: An insertion such that the pointer becomes the first pointer is not possible
	 * with this method, but is also not required during operations that grow the B-Tree.
	 * 
	 * @param key The key to be inserted.
	 * @param pageNumber The page number that is the pointer to the child page.
	 * @return true, if the insert was successful, false if the node was full.
	 * @throws PageFormatException Thrown, if the page was identified to be corrupted or
	 *                             inconsistent in any way that prevents consistent processing.
	 */
	public boolean insertKeyPageNumberPair(DataField key, int pageNumber)
	{
		return insertKeyPageNumberPairAtPosition(key, pageNumber, getInsertPositionForKey(key));
	}

	/**
	 * Inserts a (key/pointer) pair at the given position. 
	 * If the given key is put to position <code>i</code> in the
	 * sequence of keys, then the pointer will be put to position <code>i+1</code>
	 * in the sequence of pointers.
	 * <p>
	 * The method returns false, if the insert was not possible because the
	 * node if full.
	 * <p>
	 * NOTE: An insertion such that the pointer becomes the first pointer is not possible
	 * with this method, but is also not required during operations that grow the B-Tree. 
	 * 
	 * @param key The key to be inserted.
	 * @param pageNumber The page number that is the pointer to the child page.
	 * @return true, if the insert was successful, false if the node was full.
	 * @throws PageFormatException Thrown, if the page was identified to be corrupted or
	 *                             inconsistent in any way that prevents consistent processing.
	 * @throws IndexOutOfBoundsException Thrown, if the position is not within range
	 *                                   of the number of contained keys.
	 */
	public boolean insertKeyPageNumberPairAtPosition(DataField key, int pageNumber, int keyPosition)
	{
		if (Constants.DEBUG_CHECK && this.expired) {
			throw new PageExpiredException();
		}
		
		// range check
		if (keyPosition < 0 || keyPosition > this.numKeys) {
			throw new IndexOutOfBoundsException("Key position '" + keyPosition + "' is out of range [0, " + this.numKeys + "].");
		}

		// check if there is space left
		if (this.numKeys == this.maxKeys) {
			return false;
		}

		// move all keys / pointers one position to the right
		if (keyPosition < this.numKeys) {
			System.arraycopy(this.buffer, keyPosition * this.keyWidth + HEADER_SIZE, this.buffer, (keyPosition + 1) * this.keyWidth + HEADER_SIZE,
					(this.numKeys - keyPosition) * this.keyWidth);
			System.arraycopy(this.buffer, (keyPosition + 1) * PAGE_NUMBER_WIDTH + this.pointerSequenceOffset, this.buffer, (keyPosition + 2)
					* PAGE_NUMBER_WIDTH + this.pointerSequenceOffset, (this.numKeys - keyPosition) * PAGE_NUMBER_WIDTH);
		}

		// insert the key and the page number
		key.encodeBinary(this.buffer, keyPosition * this.keyWidth + HEADER_SIZE);
		IntField.encodeIntAsBinary(pageNumber, this.buffer, (keyPosition + 1) * PAGE_NUMBER_WIDTH + this.pointerSequenceOffset);

		this.numKeys++;
		persistNumKeys();
		this.modified = true;
		return true;
	}

	/**
	 * This method returns the position at which the key would be inserted,
	 * regardless of the fact whether there is space in the node.
	 * 
	 * In the case that the key is already contained in the sequence (possibly
	 * multiple times), the position of the first occurrence of the key is returned.
	 * 
	 * If <code>n</code> is the degree (order) of the index, this method returns values
	 * between and including <code>0</code> and <code>n-1</code>.
	 *  
	 * @param key The key to find the position for.
	 * @return The position where the key would be inserted.
	 * @throws PageFormatException Thrown, if the page was identified to be corrupted or
	 *                             inconsistent in any way that prevents consistent processing.
	 */
	public int getInsertPositionForKey(DataField key)
	{
		if (Constants.DEBUG_CHECK && this.expired) {
			throw new PageExpiredException();
		}
		
		int pos = getPointerPositionForKey(key);
		if (pos == -1) {
			throw new IllegalStateException("The method cannot be executed on an empty node");
		}
		else {
			return pos;
		}
	}
	
	// ------------------------------------------------------------------------
	//                              Deletion
	// ------------------------------------------------------------------------

	/**
	 * Deletes the key and pointer at the given position. If the position is
	 * <code>i</code> then key <code>i</code> and pointer <code>i+1</code> will
	 * be deleted. That implies that pointer 0 cannot be deleted, which is an operation
	 * that does not happen during B-Tree reorganization.
	 * 
	 * @param keyPosition The position of the key to delete.
	 */
	public void deleteKeyAndPointer(int keyPosition)
	{
		// range check
		if (keyPosition < 0 || keyPosition >= this.numKeys) {
			throw new IndexOutOfBoundsException("Key position '" + keyPosition + "' is out of range [0, " + this.numKeys + ").");
		}

		// move all keys / pointers from behind the relevant position to the front
		if (keyPosition < this.numKeys - 1) {
			System.arraycopy(this.buffer, (keyPosition + 1) * this.keyWidth + HEADER_SIZE, this.buffer, keyPosition * this.keyWidth + HEADER_SIZE, (this.numKeys
					- keyPosition - 1)
					* this.keyWidth);
			System.arraycopy(this.buffer, (keyPosition + 2) * PAGE_NUMBER_WIDTH + this.pointerSequenceOffset, this.buffer, (keyPosition + 1)
					* PAGE_NUMBER_WIDTH + this.pointerSequenceOffset, (this.numKeys - keyPosition - 1) * PAGE_NUMBER_WIDTH);
		}

		this.numKeys--;
		persistNumKeys();
		this.modified = true;
	}
	
	
	// ------------------------------------------------------------------------
	//                         Utility Methods
	// ------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		StringBuilder bld = new StringBuilder(8 * getNumberOfKeys() + 10);
		bld.append("[");
		
		for (int i = 0; i < getNumberOfKeys(); i++) {
			bld.append(getKey(i).encodeAsString()).append(", ");
		}
		if (bld.length() > 1) {
			bld.setLength(bld.length() - 2);
		}
		bld.append("]");
		return bld.toString();
	}
	
	/**
	 * Gets the key at the given position without checking any range constraints.
	 * 
	 * @param position The position of the key.
	 * @return The key at the given position.
	 */
	private final DataField uncheckedGetKey(int position)
	{
		return this.keyType.getFromBinary(this.buffer, position * this.keyWidth + HEADER_SIZE, this.keyWidth);
	}
	

	/**
	 * Gets the pointer page number at the given position without checking any range constraints.
	 * 
	 * @param position The position of the pointer.
	 * @return The pointer at the given position.
	 */
	private final int uncheckedGetPointer(int position)
	{
		return IntField.getIntFromBinary(this.buffer, (position * PAGE_NUMBER_WIDTH) + this.pointerSequenceOffset);
	}
	
	/**
	 * Finds the position of a key, or gives the insertion position.
	 * The algorithm is that of <code>Arrays.binarySearch()</code>.
	 * 
	 * @param key The key to search for.
	 * @return The position of the key or (-(insertion position) - 1), if the key
	 *         was not found.
	 */
	private final int binSearchForKey(DataField key)
	{		
		int low = 0;
		int high = this.numKeys - 1;
		
		while (low <= high) {
			// get middle element and compare to the search key
		    int mid = (low + high) >>> 1;
	        int midOffset = (mid * this.keyWidth) + HEADER_SIZE;
		    DataField midVal = this.keyType.getFromBinary(this.buffer, midOffset, this.keyWidth);
		    int cmp = midVal.compareTo(key);

		    // adjust next interval or return found
		    if (cmp < 0) {
		    	low = mid + 1;
		    }
		    else if (cmp > 0) {
		    	high = mid - 1;
		    }
		    else {
		    	return mid;
		    }
		}
		return -(low + 1);  // key not found.
	}
	
	/**
	 * Gets the position of the pointer that leads to the child relevant for the given key.
	 * 
	 * @param key The key to find the relevant child pointer for.
	 * @return The position of the pointer to the child that contains the first occurrence
	 *         of the given key.
	 */
	private final int getPointerPositionForKey(DataField key)
	{
		if (this.numKeys < 1) {
			return -1;
		}
		else {
			// do binary search for the key 
			int pos = binSearchForKey(key);
			if (pos < 0) {
				// key not found. the next larger key is relevant.
				// that next larger key is at the insertion position.
				return -(pos + 1);
			}
			else if (this.unique) {
				return pos;
			}
			else {
				// key found in a non-unique index.
				// track back to the first occurrence.
				DataField keyAtPos = getKey(pos);
				while (pos > 0 && getKey(pos - 1).equals(keyAtPos)) {
					pos--;
				}
				return pos;
			}			
		}
	}
	
	/**
	 * Persists the number-of-keys variable in the header field.
	 */
	private final void persistNumKeys()
	{
		IntField.encodeIntAsBinary(this.numKeys, this.buffer, HEADER_NUM_KEYS_OFFSET);
	}


	/**
	 * Simple class encapsulating a key, a page number (pointer) and the position where
	 * the key and pointer were found in the sequence of pointers in the node.
	 * The key is null if the pointer is the last in the sequence and serves hence all
	 * keys greater than the last key. 
	 */
	public static class KeyPageNumberPosition
	{
		/**
		 * The key that belongs to the pointer.
		 */
		private DataField key;
		
		/**
		 * The page number that is associated with entries for the key and all smaller keys.
		 */
		private int pageNumber;
		
		/**
		 * The position of the key and pointer.
		 */
		private int position;
	
		/**
		 * Generates a new object encapsulating a key, page number and the
		 * position where the key/pointer were found in the node.
		 * 
		 * @param key The key that belongs to the pointer.
		 * @param pageNumber The page number.
		 * @param position The position of the key/pointer.
		 */
		public KeyPageNumberPosition(DataField key, int pageNumber, int position)
		{
			this.key = key;
			this.pageNumber = pageNumber;
			this.position = position;
		}
		
		/**
		 * Gets the key that belongs to the pointer.
		 * 
		 * @return The key that belongs to the pointer.
		 */
		public DataField getKey()
		{
			return this.key;
		}
		
		/**
		 * Gets the page number pointer.
		 * 
		 * @return The page number.
		 */
		public int getPageNumber()
		{
			return this.pageNumber;
		}
	
		/**
		 * Gets the position of the key/pointer in the inner node.
		 * 
		 * @return The position of the pointer.
		 */
		public int getPosition()
		{
			return this.position;
		}
	}
}
