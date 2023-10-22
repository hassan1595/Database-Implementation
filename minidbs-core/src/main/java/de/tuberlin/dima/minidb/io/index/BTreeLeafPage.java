package de.tuberlin.dima.minidb.io.index;


import java.util.List;

import de.tuberlin.dima.minidb.Constants;
import de.tuberlin.dima.minidb.catalogue.IndexSchema;
import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataType;
import de.tuberlin.dima.minidb.core.DuplicateException;
import de.tuberlin.dima.minidb.core.IntField;
import de.tuberlin.dima.minidb.core.RID;
import de.tuberlin.dima.minidb.io.cache.PageExpiredException;
import de.tuberlin.dima.minidb.io.cache.PageFormatException;


/**
 * Implementation of a page that represent leaf pages of a B-Tree index.
 * 
 * The basic leaf page implies the following header:
 * <ul>
 *   <li>Bytes 0 - 3 are an INT (little endian) holding the magic number for index pages.</li>
 *   <li>Bytes 4 - 7 are an INT (little endian) holding the page number.</li>
 *   <li>Bytes 8 - 11 are an INT (little endian) holding type information that identifies the
 *       page as leaf node page. The specific value to be held for leaf pages is 2.</li>
 *   <li>Bytes 12 - 15 are an INT (little endian) holding the number of entries (key/RID) pairs
 *       in the leaf node.</li>
 *   <li>Bytes 16 - 19 are an INT (little endian) holding the page number of the linked next leaf
 *       page. If no next page is linked, this field holds -1</li>
 *   <li>Bytes 20 - 23 are an INT (little endian) holding miscellaneous flags. The least significant
 *       bit represents the flag to indicate that the value last key is also found on the next page.</li> 
 * </ul> 
 * 
 * Many of the defined methods may throw an {@link IndexFormatCorruptException} when determining that
 * index conditions (such as the sorted order) are violated. The methods should however not explicitly
 * check everything, but only throw the exception when stumbling upon a condition that is obviously
 * due to violation of the index conditions.
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class BTreeLeafPage implements BTreeIndexPage
{
	// ------------------------------------------------------------------------
	//                         public constants
	// ------------------------------------------------------------------------
	
	/**
	 * The constant indicating a leaf page when found in the header.
	 */
	public static final int HEADER_TYPE_VALUE = 2;
	
	
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
	private static final int HEADER_NUM_ENTRIES_OFFSET = 12;
	
	/**
	 * The offset of the field holding the page number of the linked next page.
	 */
	private static final int HEADER_NEXT_PAGE_OFFSET = 16;
	
	/**
	 * The offset of the field holding the flags.
	 */
	private static final int HEADER_FLAGS_OFFSET = 20;
	
	/**
	 * Mask for the flag bit that indicates whether the last key continues on the next page. 
	 */
	private static final int FLAGS_MASK_KEY_CONTINUES = 0x1;

	
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
	 * The maximal number of entries in the node.
	 */
	private final int maxEntries;
	
	/**
	 * The width of a key in bytes.
	 */
	private final int keyWidth;
	
	/**
	 * The offset to the point where the sequence of RIDs begins.
	 */
	private final int RIDSequenceOffset;
	
	/**
	 * The current number of entries.
	 */
	private int numEntries;
	
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
	 * Creates a new leaf page wrapping the binary data in the given buffer.
	 * 
	 * @param schema The schema of the index that the leaf page data in the buffer belongs to.
	 * @param buffer The buffer with the binary page data.
	 */
	public BTreeLeafPage(IndexSchema schema, byte[] buffer)
	{
		this.buffer = buffer;
		this.keyType = schema.getIndexedColumnSchema().getDataType();
		this.maxEntries = schema.getMaximalLeafEntries();
		this.keyWidth = this.keyType.getNumberOfBytes();
		this.RIDSequenceOffset = HEADER_SIZE + (this.maxEntries * this.keyWidth);
		this.numEntries = IntField.getIntFromBinary(buffer, HEADER_NUM_ENTRIES_OFFSET);
		this.unique = schema.isUnique();
		this.modified = false;
		this.expired = false;
	}

	// ------------------------------------------------------------------------
	//                       generic cacheable data behavior
	// ------------------------------------------------------------------------


	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.io.cache.CacheableData#getPageNumber()
	 */
	@Override
	public int getPageNumber() throws PageExpiredException
	{
		if (this.expired) {
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.tuberlin.dima.minidb.io.cache.CacheableData#hasBeenModified()
	 */
	@Override
	public boolean hasBeenModified()
	{
		return this.modified;
	}

	/*
	 * (non-Javadoc)
	 * 
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
	 * @see de.tuberlin.dima.minidb.io.cache.CacheableData#markExpired()
	 */
	@Override
	public void markExpired()
	{
		this.expired = true;
	}

	
	// ------------------------------------------------------------------------
	//                   header access and modification
	// ------------------------------------------------------------------------

	/**
	 * Gets the page number of the next leaf page where the ascending sequence of (key / RID) pairs continues. Returns
	 * -1, if no next page is linked.
	 * 
	 * @return The page number of the linked next page, or -1, if no page is linked.
	 */
	public int getNextLeafPageNumber()
	{
		if (Constants.DEBUG_CHECK && this.expired) {
			throw new PageExpiredException();
		}
		
		return IntField.getIntFromBinary(this.buffer, HEADER_NEXT_PAGE_OFFSET);
	}

	/**
	 * Sets the page number of the next leaf page where the ascending sequence of (key / RID) pairs continues. A value
	 * of -1 indicates that no next page is linked.
	 * 
	 * @param nextPageNumber The page number of the next leaf page.
	 */
	public void setNextLeafPageNumber(int nextPageNumber)
	{
		if (Constants.DEBUG_CHECK && this.expired) {
			throw new PageExpiredException();
		}
		
		IntField.encodeIntAsBinary(nextPageNumber, this.buffer, HEADER_NEXT_PAGE_OFFSET);
	}

	/**
	 * Gets the number of (key / RID) pairs on this leaf page.
	 * 
	 * @return The number of (key / RID) pairs.
	 */
	public int getNumberOfEntries()
	{
		if (Constants.DEBUG_CHECK && this.expired) {
			throw new PageExpiredException();
		}
		
		return this.numEntries;
	}

	/**
	 * Checks, if the value of the last (highest) key in this page key continues on the next page.
	 * 
	 * @return true, if the last key's value continues on the next page, false if not.
	 */
	public boolean isLastKeyContinuingOnNextPage()
	{
		if (Constants.DEBUG_CHECK && this.expired) {
			throw new PageExpiredException();
		}
		
		int flags = IntField.getIntFromBinary(this.buffer, HEADER_FLAGS_OFFSET);
		return (flags & FLAGS_MASK_KEY_CONTINUES) != 0;
	}
	
	/**
	 * Sets the flag that indicates if the value of the last (highest) key in this page key continues on the next page.
	 * 
	 * @param keyContinues True, if the last key's value continues on the next page, false if not.
	 */
	public void setLastKeyContinuingOnNextPage(boolean keyContinues)
	{
		if (Constants.DEBUG_CHECK && this.expired) {
			throw new PageExpiredException();
		}
		
		int flags = IntField.getIntFromBinary(this.buffer, HEADER_FLAGS_OFFSET);
		if (keyContinues) {
			flags |= FLAGS_MASK_KEY_CONTINUES;
		} else {
			flags &= ~FLAGS_MASK_KEY_CONTINUES;
		}
		IntField.encodeIntAsBinary(flags, this.buffer, HEADER_FLAGS_OFFSET);
	}
	
	// ------------------------------------------------------------------------
	//                               lookups
	// ------------------------------------------------------------------------

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
		
		return this.numEntries == 0 ? null : this.keyType.getFromBinary(this.buffer, HEADER_SIZE, this.keyWidth);
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
		
		int offset = (this.numEntries - 1) * this.keyWidth + HEADER_SIZE;
		return this.numEntries == 0 ? null : this.keyType.getFromBinary(this.buffer, offset, this.keyWidth);
	}

	/**
	 * Gets the key at the given position.
	 * 
	 * @param position The position of the key.
	 * @return The key at the given position.
	 * @throws IndexOutOfBoundsException If the position is out of the range between zero and
	 *                                   the number of entries.
	 */
	public DataField getKey(int position)
	{
		if (Constants.DEBUG_CHECK && this.expired) {
			throw new PageExpiredException();
		}
		
		if (position < 0 || position >= this.numEntries) {
			throw new IndexOutOfBoundsException("position is not within valid range.");
		}

		position = position * this.keyWidth + HEADER_SIZE;
		return this.keyType.getFromBinary(this.buffer, position, this.keyWidth);
	}

	/**
	 * Gets the position of the key in the sequence of entries. If the key is contained
	 * multiple times, then the position of the first occurrence is returned, i.e. the
	 * lowest position where that key occurs.
	 * If the key itself is not contained, the position of the next larger key is returned.
	 * If the page is empty, then 0 is returned. The method returns hence values between
	 * <code>0</code> and <code>numberOfEntries</code>.
	 * 
	 * @param key The key to get the position for.
	 * @return The position of the key or the next larger key, if the key is not found.
	 */
	public int getPositionForKey(DataField key)
	{
		if (Constants.DEBUG_CHECK && this.expired) {
			throw new PageExpiredException();
		}
		
		int pos = binSearchForKey(key);

		// if the key is contained, move to the leftmost occurrence of the key
		if (pos >= 0) {
			int keyOffset = (pos - 1) * this.keyWidth + HEADER_SIZE;
			while (pos > 0 && this.keyType.getFromBinary(this.buffer, keyOffset, this.keyWidth).equals(key)) {
				pos--;
				keyOffset -= this.keyWidth;
			}

			return pos;
		} else {
			return -(pos + 1);
		}
	}

	/**
	 * Gets the RID at the given position.
	 * 
	 * @param position The position of the RID.
	 * @return The RID at the given position.
	 * @throws IndexOutOfBoundsException If the position is out of the range between zero and
	 *                                   the number of entries.
	 */
	public RID getRidAtPosition(int position)
	{
		if (Constants.DEBUG_CHECK && this.expired) {
			throw new PageExpiredException();
		}
		
		if (position < 0 || position >= this.numEntries) {
			throw new IndexOutOfBoundsException("position is not within valid range.");
		}

		position = position * RID.getRIDSize() + this.RIDSequenceOffset;
		return RID.getRidFromBinary(this.buffer, position);
	}

	/**
	 * Gets an RID for the given key. For unique indexes, this method returns 
	 * the only RID for that key, for none-unique indexes, this method returns any
	 * RID that the key maps to. Which RID is not predictable, since there is no
	 * order implied on the RIDs.
	 * 
	 * @param key The key to get an RID for.
	 * @return An RID for key, or null, if the key is not contained.
	 * 
	 * @throws PageFormatException Thrown, if the page was identified to be corrupted or
	 *                             inconsistent in any way that prevents consistent processing.
	 */
	public RID getRIDForKey(DataField key) throws PageFormatException
	{
		if (Constants.DEBUG_CHECK && this.expired) {
			throw new PageExpiredException();
		}
		
		int pos = binSearchForKey(key);
		if (pos >= 0) {
			pos = pos * RID.getRIDSize() + this.RIDSequenceOffset;
			return RID.getRidFromBinary(this.buffer, pos);
		} else {
			return null;
		}
	}

	/**
	 * Gets all RIDs for a key on the given page. The RIDs are added to the given list.
	 * If the key was the last (highest) key on the page, this method returns true
	 * to notify the caller that the same key might continue on the next page.
	 *  
	 * @param key The key to retrieve the RIDs for.
	 * @param target The list to which to add retrieved RIDs.
	 * @return True, if the key was the last (highest) key on the page, false otherwise.
	 * 
	 * @throws PageFormatException Thrown, if the page was identified to be corrupted or
	 *                             inconsistent in any way that prevents consistent processing.
	 */
	public boolean getAllsRIDsForKey(DataField key, List<RID> target) throws PageFormatException
	{
		if (Constants.DEBUG_CHECK && this.expired) {
			throw new PageExpiredException();
		}
		
		int pos = binSearchForKey(key);
		if (pos >= 0) {
			final int ridSize = RID.getRIDSize();
			int keyOffset = pos * this.keyWidth + HEADER_SIZE;
			int ridOffset = pos * ridSize + this.RIDSequenceOffset;

			// move backwards to where the key changes
			// we can add the RIDs in any order because the RID order for the same key is undefined.
			for (int k = keyOffset, r = ridOffset; k >= HEADER_SIZE; k -= this.keyWidth, r -= ridSize) {
				DataField currKey = this.keyType.getFromBinary(this.buffer, k, this.keyWidth);
				if (currKey.equals(key)) {
					// add the RID
					target.add(RID.getRidFromBinary(this.buffer, r));
				} else {
					break;
				}
			}

			// move forward until the key changes. the start position was already
			// covered by the previous loop
			pos++;
			keyOffset += this.keyWidth;
			ridOffset += ridSize;

			while (pos < this.numEntries) {
				DataField currKey = this.keyType.getFromBinary(this.buffer, keyOffset, this.keyWidth);
				if (currKey.equals(key)) {
					target.add(RID.getRidFromBinary(this.buffer, ridOffset));
				} else {
					pos--;
					break;
				}

				// increment
				pos++;
				keyOffset += this.keyWidth;
				ridOffset += ridSize;
			}

			// check if this was the last (highest) key on the page.
			return (pos == this.numEntries);
		} else {
			return false;
		}
	}

	/**
	 * Gets all keys on this index page.
	 * 
	 * @param target The list into which the keys are stored.
	 * @param startPosition The position of the first key.
	 */
	public void getAllKeys(List<DataField> target, int startPosition)
	{
		if (Constants.DEBUG_CHECK && this.expired) {
			throw new PageExpiredException();
		}
		if (startPosition < 0 || startPosition >= this.numEntries) {
			throw new IllegalArgumentException("The start position is out of bounds.");
		}
		
		for (int position = startPosition; position < this.numEntries; position++) {
			int offset = position * this.keyWidth + HEADER_SIZE;
			target.add(this.keyType.getFromBinary(this.buffer, offset, this.keyWidth));
		}
	}
	
	
	// ------------------------------------------------------------------------
	//                            modifications
	// ------------------------------------------------------------------------


	/**
	 * Inserts a pair (key / RID) into the sorted sequence of this index page.
	 * 
	 * @param key The key of the pair.
	 * @param value The RID of the pair.
	 * @return True, if the pair could be successfully inserted, false if there
	 *         was no space left.
	 *          
	 * @throws PageFormatException Thrown, if the page was identified to be corrupted or
	 *                             inconsistent in any way that prevents consistent processing.
	 * @throws DuplicateException Thrown, if the key is already contained, but the index is unique.
	 */
	public boolean insertKeyRIDPair(DataField key, RID rid) throws PageFormatException
	{
		if (Constants.DEBUG_CHECK && this.expired) {
			throw new PageExpiredException();
		}
		
		if (this.numEntries == this.maxEntries) {
			return false;
		}
		else {
			int pos = binSearchForKey(key);
			if (pos < 0) {
				// key not yet contained. make pos the insertion point.
				pos = -(pos + 1);
			}
			else if (this.unique) {
				throw new DuplicateException("Key " + key + " is already contained.");
			}

			// pos is now the position where the key goes
			final int ridWidth = RID.getRIDSize();
			int keyOffset = pos * this.keyWidth + HEADER_SIZE;
			int ridOffset = pos * ridWidth + this.RIDSequenceOffset;

			// shift all keys one to the right, on a byte level
			System.arraycopy(this.buffer, keyOffset, this.buffer, keyOffset + this.keyWidth, (this.numEntries - pos) * this.keyWidth);
			// shift all RIDs one to the right, on a byte level
			System.arraycopy(this.buffer, ridOffset, this.buffer, ridOffset + ridWidth, (this.numEntries - pos) * ridWidth);

			// store the key / rid pair
			key.encodeBinary(this.buffer, keyOffset);
			rid.encodeBinary(this.buffer, ridOffset);

			// mark modified and store the number of entries
			this.modified = true;
			this.numEntries++;
			IntField.encodeIntAsBinary(this.numEntries, this.buffer, HEADER_NUM_ENTRIES_OFFSET);
			return true;
		}
	}

	/**
	 * Deletes a pair of (key / RID) from this leaf page. If the pair is not contained,
	 * the method returns false, if it has been deleted, it returns true. 
	 * 
	 * @param key The key of the pair to be removed.
	 * @param value The rid of the pair to be removed.
	 * @return True, if the pair was successfully deleted, false if it was not
	 *         contained in the leaf page.
	 * 
	 * @throws PageFormatException Thrown, if the page was identified to be corrupted or
	 *                             inconsistent in any way that prevents consistent processing.
	 */
	public boolean deleteKeyRIDPair(DataField key, RID rid) throws PageFormatException
	{
		if (Constants.DEBUG_CHECK && this.expired) {
			throw new PageExpiredException();
		}
		
		int pos = binSearchForKey(key);
		if (pos < 0) {
			return false;
		}
		else {
			// key contained, find the rid
			final int ridSize = RID.getRIDSize();
			int keyOffset = pos * this.keyWidth + HEADER_SIZE;
			int ridOffset = pos * ridSize + this.RIDSequenceOffset;
			RID rdd = RID.getRidFromBinary(this.buffer, ridOffset);

			if (rdd.equals(rid)) {
				// immediate match
				deletePosition(pos);
				return true;
			}
			else if (this.unique) {
				return false;
			}
			// not unique, go left and right until the key/RID pair is found

			// move backwards to where the key changes
			for (int p = pos - 1, k = keyOffset - this.keyWidth, r = ridOffset - ridSize; p >= 0; p--, k -= this.keyWidth, r -= ridSize) {
				DataField currKey = this.keyType.getFromBinary(this.buffer, k, this.keyWidth);
				if (currKey.equals(key)) {
					// same key, check rid
					RID thisRid = RID.getRidFromBinary(this.buffer, r);
					if (thisRid.equals(rid)) {
						// found
						deletePosition(p);
						return true;
					}
					// go on left
				}
				else {
					// no longer the same key
					break;
				}
			}
			// move backwards to where the key changes
			for (; pos < this.numEntries; pos++, keyOffset += this.keyWidth, ridOffset += ridSize) {
				DataField currKey = this.keyType.getFromBinary(this.buffer, keyOffset, this.keyWidth);
				if (currKey.equals(key)) {
					// same key, check rid
					RID thisRid = RID.getRidFromBinary(this.buffer, ridOffset);
					if (thisRid.equals(rid)) {
						// found
						deletePosition(pos);
						return true;
					}
					// go on right
				}
				else {
					// no longer the same key
					break;
				}
			}

			// not found
			return false;
		}
	}


	/**
	 * Takes the first num keys from the given other page and moves them
	 * to the end of the sequence of keys on this page.
	 * This operation happens for example when an underflow in a leaf page
	 * happens or when a node is deleted.
	 * <p>
	 * WARNING: This method does not set the key-continuing-flag on the other page.
	 * <p>
	 * NOTE: This implies that the first element in the sequence of pairs to
	 * be moved is greater or equal to the last key on this page. If this
	 * page is empty, that condition is never violated.
	 * <p>
	 * If the keys could not be moved due to a lack of space, this method
	 * needs to leave both pages untouched and return false.
	 *  
	 * @param other The node from which to take the keys.
	 * @param num The number of keys to take.
	 * @return true, if the keys were successfully moved, false if there was
	 *         not enough space to move the keys.
	 *         
	 * @throws PageFormatException Thrown, if the page was identified to be corrupted or
	 *                             inconsistent in any way that prevents consistent processing.
	 * @throws IndexFormatCorruptException Thrown if the first key to move was not greater
	 *                             or equal to the last key on this page, meaning that the
	 *                             sorted order would be violated.
	 */
	public boolean appendEntriesFromOtherPage(BTreeLeafPage other, int num)
	throws PageFormatException, IndexFormatCorruptException
	{
		if (Constants.DEBUG_CHECK && this.expired) {
			throw new PageExpiredException();
		}
		
		// checks
		if (num <= 0) {
			return true;
		}
		else if (other.numEntries < num) {
			throw new IllegalArgumentException("Source does not contain enough entries.");
		}
		else if (this.numEntries + num > this.maxEntries) {
			return false;
		} 
		
		// sanity check
		DataField ourLast = getLastKey();
		DataField theirFirst = other.getFirstKey();
		if (ourLast != null && ourLast.compareTo(theirFirst) > 0) {
			// our last key is larger than their first, that should not be
			throw new IndexFormatCorruptException("Keys to append are smaller the last keys");
		}
		
		// copy from other to us
		final int ridWidth = RID.getRIDSize();
		int keyOffset = this.numEntries * this.keyWidth + HEADER_SIZE;
		int ridOffset = this.numEntries * ridWidth + this.RIDSequenceOffset;
		System.arraycopy(other.buffer, HEADER_SIZE, this.buffer, keyOffset, num * this.keyWidth);
		System.arraycopy(other.buffer, other.RIDSequenceOffset, this.buffer, ridOffset, num * ridWidth);

		// update our header
		this.numEntries += num;
		IntField.encodeIntAsBinary(this.numEntries, this.buffer, HEADER_NUM_ENTRIES_OFFSET);
		this.modified = true;
		
		if (other.numEntries > num) {
			// move the remaining entries in the other page to the front
			System.arraycopy(other.buffer, num * this.keyWidth + HEADER_SIZE,
			                 other.buffer, HEADER_SIZE, (other.numEntries - num) * this.keyWidth);
			System.arraycopy(other.buffer, num * ridWidth + other.RIDSequenceOffset,
			                 other.buffer, other.RIDSequenceOffset, (other.numEntries - num) * ridWidth);
			// at least one key will be kept on the other. the other's isKeyContinuing flag is still
			// valid. set ours, if necessary
		}
		// update the other page's header
		other.numEntries -= num;
		IntField.encodeIntAsBinary(other.numEntries, other.buffer, HEADER_NUM_ENTRIES_OFFSET);
		other.modified = true;
		
		return true;
	}

	/**
	 * Takes the last num keys from the given other page and inserts them
	 * at the beginning of the sequence of keys on this page. This operation is used to move some keys
	 * to a new page when a split happens, or when an underflow in a leaf page happens or a node is deleted.
	 * <p>
	 * WARNING: This method does not set the key-continuing-flag on the other page.
	 * <p>
	 * NOTE: This implies that the last element in the sequence of pairs to
	 * be moved is smaller or equal to the last key on this page. If this
	 * page is empty, that condition is never violated.
	 * <p>
	 * If the keys could not be moved due to a lack of space, this method
	 * needs to leave both pages untouched and return false.
	 *  
	 * @param other The node from which to take the keys.
	 * @param num The number of keys to take.
	 * @return true, if the keys were successfully moved, false if there was
	 *         not enough space to move the keys.
	 *         
	 * @throws PageFormatException Thrown, if the page was identified to be corrupted or
	 *                             inconsistent in any way that prevents consistent processing.
	 * @throws IndexFormatCorruptException Thrown if the last key to move was not smaller
	 *                             or equal to the first key on this page, meaning that the
	 *                             sorted order would be violated.
	 */
	public boolean prependEntriesFromOtherPage(BTreeLeafPage other, int num)
	throws PageFormatException, IndexFormatCorruptException
	{
		if (Constants.DEBUG_CHECK && this.expired) {
			throw new PageExpiredException();
		}
		
		// checks
		if (num <= 0) {
			return true;
		}
		else if (other.numEntries < num) {
			throw new IllegalArgumentException("Source does not contain enough entries.");
		}
		else if (this.numEntries + num > this.maxEntries) {
			return false;
		} 
		
		// sanity check
		DataField theirLast = other.getLastKey();
		DataField ourFirst = getFirstKey();
		if (ourFirst != null && theirLast.compareTo(ourFirst) > 0) {
			// their last key is larger than our first, that should not be
			throw new IndexFormatCorruptException("Keys to prepend are larger this node's first keys");
		}
		
		final int ridWidth = RID.getRIDSize();
		// make space on our side. move all entries to the side
		System.arraycopy(this.buffer, HEADER_SIZE, this.buffer, num * this.keyWidth + HEADER_SIZE, this.numEntries * this.keyWidth);
		System.arraycopy(this.buffer, this.RIDSequenceOffset, this.buffer, num * ridWidth + this.RIDSequenceOffset, this.numEntries * ridWidth);
		
		// copy from other to us
		int startPos = other.numEntries - num;
		System.arraycopy(other.buffer, startPos * this.keyWidth + HEADER_SIZE, this.buffer, HEADER_SIZE, num * this.keyWidth);
		System.arraycopy(other.buffer, startPos * ridWidth + this.RIDSequenceOffset, this.buffer, this.RIDSequenceOffset, num * ridWidth);

		// update our header
		this.numEntries += num;
		IntField.encodeIntAsBinary(this.numEntries, this.buffer, HEADER_NUM_ENTRIES_OFFSET);
		this.modified = true;
		
		// update the other page's header
		other.numEntries -= num;
		IntField.encodeIntAsBinary(other.numEntries, other.buffer, HEADER_NUM_ENTRIES_OFFSET);
		other.modified = true;
		
		return true;
	}
	
	// ------------------------------------------------------------------------
	//                         Utility Methods
	// ------------------------------------------------------------------------
	
	/**
	 * Finds the position of a key, or gives the insertion position.
	 * The algorithm is that of <code>Arrays.binarySearch()</code>.
	 * 
	 * @param key The key to search for.
	 * @return The position of the key or (-(insertion position) - 1), if the key
	 *         was not found.
	 */
	private int binSearchForKey(DataField key)
	{		
		int low = 0;
		int high = this.numEntries - 1;
		
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
	 * Deletes the entry (key/rid) at the given position.
	 * 
	 * @param pos The position of the entry to delete.
	 */
	private void deletePosition(int pos)
	{
		// move all keys from the right
		int keyOff = pos * this.keyWidth + HEADER_SIZE;
		int ridOff = pos * RID.getRIDSize() + this.RIDSequenceOffset;
		
		System.arraycopy(this.buffer, keyOff + this.keyWidth, this.buffer, keyOff, (this.numEntries - pos - 1) * this.keyWidth);
		System.arraycopy(this.buffer, ridOff + RID.getRIDSize(), this.buffer, ridOff, (this.numEntries - pos - 1) * RID.getRIDSize());
		
		// mark modified and adjust entry count
		this.modified = true;
		this.numEntries--;
		IntField.encodeIntAsBinary(this.numEntries, this.buffer, HEADER_NUM_ENTRIES_OFFSET);
	}
}
