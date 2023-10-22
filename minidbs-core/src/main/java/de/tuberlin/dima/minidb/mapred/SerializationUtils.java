package de.tuberlin.dima.minidb.mapred;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.codec.binary.Base64;

import de.tuberlin.dima.minidb.core.BasicType;
import de.tuberlin.dima.minidb.core.BigIntField;
import de.tuberlin.dima.minidb.core.CharField;
import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataType;
import de.tuberlin.dima.minidb.core.DateField;
import de.tuberlin.dima.minidb.core.DoubleField;
import de.tuberlin.dima.minidb.core.FloatField;
import de.tuberlin.dima.minidb.core.IntField;
import de.tuberlin.dima.minidb.core.RID;
import de.tuberlin.dima.minidb.core.SmallIntField;
import de.tuberlin.dima.minidb.core.TimeField;
import de.tuberlin.dima.minidb.core.TimestampField;
import de.tuberlin.dima.minidb.core.VarcharField;
import de.tuberlin.dima.minidb.parser.OutputColumn.AggregationType;
import de.tuberlin.dima.minidb.qexec.LowLevelPredicate;
import de.tuberlin.dima.minidb.qexec.predicate.JoinPredicate;
import de.tuberlin.dima.minidb.qexec.predicate.JoinPredicateAtom;
import de.tuberlin.dima.minidb.qexec.predicate.JoinPredicateConjunction;
import de.tuberlin.dima.minidb.qexec.predicate.JoinPredicateDisjunction;
import de.tuberlin.dima.minidb.qexec.predicate.LocalPredicate;
import de.tuberlin.dima.minidb.qexec.predicate.LocalPredicateConjunction;
import de.tuberlin.dima.minidb.qexec.predicate.LocalPredicateDisjunction;

/**
 * Utility class that offers functions to serialize/deserialize certain
 * important data structures for the Hadoop integration.
 * 
 * @author mheimel
 *
 */
public class SerializationUtils {
	
	/**
	 * Deserialize a local predicate from a data stream.
	 * 
	 * @param in
	 * @return
	 * @throws IOException
	 */
	public static LocalPredicate readLocalPredicateFromStream(DataInput in) throws IOException {
		LocalPredicate result = null;
		// Read the type.
		char type = in.readChar();
		switch (type) {
		case 'l':
			result = new LowLevelPredicate();
			break;
		case 'c':
			result = new LocalPredicateConjunction();
			break;
		case 'd':
			result = new LocalPredicateDisjunction();
			break;
		}
		result.readFields(in);
		return result;
	}
	
	/**
	 * Serialize a local predicate into a data stream.
	 * 
	 * @param in
	 * @return
	 * @throws IOException
	 */
	public static void writeLocalPredicateToStream(LocalPredicate predicate, 
			DataOutput out) throws IOException {
		if (predicate instanceof LowLevelPredicate) {
			out.writeChar('l');
		} else if (predicate instanceof LocalPredicateConjunction) {
			out.writeChar('c');
		} else if (predicate instanceof LocalPredicateDisjunction) {
			out.writeChar('d');
		}
		predicate.write(out);

	}
	
	/**
	 * Convenience function to de-serialize a local predicate from a string.
	 * 
	 * @param s
	 * @return
	 * @throws IOException
	 */
	public static LocalPredicate readLocalPredicateFromString(String s) throws IOException {
		if (s == null || s.isEmpty()) return null;
		DataInput in = new DataInputStream(
				new ByteArrayInputStream(Base64.decodeBase64(s)));
		return readLocalPredicateFromStream(in);
	}
	
	/**
	 * Conveneince function to serialize a local predicate into a string.
	 * @param predicate
	 * @return
	 * @throws IOException
	 */
	public static String writeLocalPredicateToString(LocalPredicate predicate) throws IOException {
		ByteArrayOutputStream byte_stream = new ByteArrayOutputStream(128);
		DataOutputStream out = new DataOutputStream(byte_stream);
		writeLocalPredicateToStream(predicate, out);
		return new String(Base64.encodeBase64(byte_stream.toByteArray()));
	}
	
	/**
	 * Deserialize a join predicate from a data stream.
	 * 
	 * @param in
	 * @return
	 * @throws IOException
	 */
	public static JoinPredicate readJoinPredicateFromStream(DataInput in) throws IOException {
		JoinPredicate result = null;
		// Read the type.
		char type = in.readChar();
		switch (type) {
		case 'a':
			result = new JoinPredicateAtom();
			break;
		case 'c':
			result = new JoinPredicateConjunction();
			break;
		case 'd':
			result = new JoinPredicateDisjunction();
			break;
		}
		result.readFields(in);
		return result;
	}
	
	/**
	 * Serialize a join predicate into a data stream.
	 * 
	 * @param out
	 * @return
	 * @throws IOException
	 */
	public static void writeJoinPredicateToStream(JoinPredicate predicate, 
			DataOutput out) throws IOException {
		if (predicate instanceof JoinPredicateAtom) {
			out.writeChar('a');
		} else if (predicate instanceof JoinPredicateConjunction) {
			out.writeChar('c');
		} else if (predicate instanceof JoinPredicateDisjunction) {
			out.writeChar('d');
		}
		predicate.write(out);
	}
	
	/**
	 * Convenience function to de-serialize a join predicate from a string.
	 * 
	 * @param s
	 * @return
	 * @throws IOException
	 */
	public static JoinPredicate readJoinPredicateFromString(String s) throws IOException {
		if (s == null || s.isEmpty()) return null;
		DataInput in = new DataInputStream(
				new ByteArrayInputStream(Base64.decodeBase64(s)));
		return readJoinPredicateFromStream(in);
	}
	
	/**
	 * Convenience function to serialize a join predicate into a string.
	 * @param predicate
	 * @return
	 * @throws IOException
	 */
	public static String writeJoinPredicateToString(JoinPredicate predicate) throws IOException {
		ByteArrayOutputStream byte_stream = new ByteArrayOutputStream(128);
		DataOutputStream out = new DataOutputStream(byte_stream);
		writeJoinPredicateToStream(predicate, out);
		return new String(Base64.encodeBase64(byte_stream.toByteArray()));
	}
	
	/**
	 * Serialize an integer array to a string.
	 * 
	 * @param array
	 * @return
	 * @throws IOException 
	 */
	public static String writeIntArrayToString(int[] array) throws IOException {
		ByteArrayOutputStream byte_stream = new ByteArrayOutputStream(4 + array.length * 4);
		DataOutputStream out = new DataOutputStream(byte_stream);
		out.writeInt(array.length);
		for (int i=0; i<array.length; ++i) {
			out.writeInt(array[i]);
		}
		return new String(Base64.encodeBase64(byte_stream.toByteArray()));
	}
	
	/**
	 * De-serialize an integer array from a string.
	 * 
	 * @param array
	 * @return
	 * @throws IOException
	 */
	public static int[] readIntArrayFromString(String s) throws IOException {
		if (s == null || s.isEmpty()) return null;
		DataInput in = new DataInputStream(
				new ByteArrayInputStream(Base64.decodeBase64(s)));
		int[] array = new int[in.readInt()];
		for (int i=0; i<array.length; ++i) {
			array[i] = in.readInt();
		}
		return array;
	}

	/**
	 * Serialize an AggregationType array to a string.
	 * @throws IOException 
	 * 
	 */
	public static String writeAggregationTypeArrayToString(
			AggregationType[] array) throws IOException {
		ByteArrayOutputStream byte_stream = new ByteArrayOutputStream(4 + array.length * 4);
		DataOutputStream out = new DataOutputStream(byte_stream);
		out.writeInt(array.length);
		for (int i=0; i<array.length; ++i) {
			out.writeInt(array[i].ordinal());
		}
		return new String(Base64.encodeBase64(byte_stream.toByteArray()));
	}
	
	/**
	 * De-serialize an AggregationType array from a string.
	 *
	 * @param s
	 * @return
	 * @throws IOException
	 */
	public static AggregationType[] readAggregationTypeArrayFromString(String s) 
			throws IOException {
		if (s == null || s.isEmpty()) return null;
		DataInput in = new DataInputStream(
				new ByteArrayInputStream(Base64.decodeBase64(s)));
		AggregationType[] array = new AggregationType[in.readInt()];
		for (int i=0; i<array.length; ++i) {
			array[i] = AggregationType.values()[in.readInt()];
		}
		return array;
	}
	
	/**
	 * Serialize a DataType array to a string.
	 * 
	 * @throws IOException 
	 * 
	 */
	public static String writeDataTypeArrayToString(
			DataType[] array) throws IOException {
		ByteArrayOutputStream byte_stream = new ByteArrayOutputStream(4 + array.length * 4);
		DataOutputStream out = new DataOutputStream(byte_stream);
		out.writeInt(array.length);
		for (int i=0; i<array.length; ++i) {
			out.writeInt(array[i].getBasicType().ordinal());
			out.writeInt(array[i].getLength());
		}
		return new String(Base64.encodeBase64(byte_stream.toByteArray()));
	}
	
	/**
	 * De-serialize an AggregationType array from a string.
	 *
	 * @param s
	 * @return
	 * @throws IOException
	 */
	public static DataType[] readDataTypeArrayFromString(String s) 
			throws IOException {
		if (s == null || s.isEmpty()) return null;
		DataInput in = new DataInputStream(
				new ByteArrayInputStream(Base64.decodeBase64(s)));
		DataType[] array = new DataType[in.readInt()];
		for (int i=0; i<array.length; ++i) {
			BasicType type = BasicType.values()[in.readInt()];
			int length = in.readInt();
			array[i] = DataType.get(type, length);
		};
		return array;
	}
	
	/**
	 * Serialize a Datafield into a stream.
	 * 
	 * @param in
	 * @return
	 * @throws IOException 
	 */
	public static DataField readDataFieldFromStream(DataInput in) throws IOException {
		DataField result = null;
		// A data field is encoded as a (type, size, content) triplet, where type encodes the type.
		// Size is the serialized size of the field in bytes and content is a byte buffer containing the field content.
		BasicType type = BasicType.values()[in.readInt()];
		int size = in.readInt();
		byte[] content = new byte[size];
		in.readFully(content);
		// Now call the deserialization function for the corresponding type to reconstruct the field.
		switch (type) {
			case SMALL_INT:
				result = SmallIntField.getFieldFromBinary(content);
				break;
			case INT:
				result = IntField.getFieldFromBinary(content);
				break;
			case BIG_INT:
				result = BigIntField.getFieldFromBinary(content);
				break;
			case FLOAT:
				result = FloatField.getFieldFromBinary(content);
				break;
			case DOUBLE:
				result = DoubleField.getFieldFromBinary(content);
				break;
			case CHAR:
				result = CharField.getFieldFromBinary(content);
				break;
			case VAR_CHAR:
				result = VarcharField.getFieldFromBinary(content);
				break;
			case DATE:
				result = DateField.getFieldFromBinary(content, 0);
				break;
			case TIME:
				result = TimeField.getFieldFromBinary(content, 0);
				break;
			case TIMESTAMP:
				result = TimestampField.getFieldFromBinary(content, 0);
				break;
			case RID:
				result = RID.getRidFromBinary(content, 0);
				break;	
		};
		return result;
	}
	
	/**
	 * Deserialize a DataField from a stream.
	 * 
	 * @param field
	 * @param out
	 * @throws IOException
	 */
	public static void writeDataFieldToStream(DataField field, DataOutput out) throws IOException {
		out.writeInt(field.getBasicType().ordinal());
		byte content[] = new byte[field.getNumberOfBytes()];
		field.encodeBinary(content, 0);
		out.writeInt(content.length);
		out.write(content);
	}
}
