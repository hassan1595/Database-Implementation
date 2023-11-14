package de.tuberlin.dima.minidb.io.tables;

import de.tuberlin.dima.minidb.catalogue.TableSchema;
import de.tuberlin.dima.minidb.core.*;
import de.tuberlin.dima.minidb.io.cache.CacheableData;
import de.tuberlin.dima.minidb.io.cache.PageExpiredException;
import de.tuberlin.dima.minidb.io.cache.PageFormatException;
import de.tuberlin.dima.minidb.qexec.LowLevelPredicate;
import de.tuberlin.dima.minidb.qexec.QueryExecutionException;
import de.tuberlin.dima.minidb.util.Pair;

import java.util.ArrayList;

public class TablePageClass implements TablePage, CacheableData {


    private final TableSchema schema;
    private byte[] binPage;
    private final int OFFSET_PAGE_NUMBER = 4;
    private final int OFFSET_NUM_RECORDS = 8;
    private final int OFFSET_RECORD_WIDTH = 12;
    private final int OFFSET_VARIABLE_OFFSET = 16;

    private boolean beenModified = false;
    private boolean expired = false;

    public TablePageClass(TableSchema schema, byte[] binPage, int pageNum) {

        this.schema = schema;
        this.binPage = binPage;
        this.beenModified = true;

        IntField magicField = new IntField(TABLE_DATA_PAGE_HEADER_MAGIC_NUMBER);
        int offsetMagicNum = 0;
        magicField.encodeBinary(binPage, offsetMagicNum);

        IntField pageNumField = new IntField(pageNum);
        pageNumField.encodeBinary(binPage, this.OFFSET_PAGE_NUMBER);

        IntField nRecords = new IntField(0);
        nRecords.encodeBinary(binPage, this.OFFSET_NUM_RECORDS);

        int rSize = 4;
        for (int i = 0; i < schema.getNumberOfColumns(); i++) {
            if (schema.getColumn(i).getDataType().getBasicType().isFixLength()) {
                rSize += schema.getColumn(i).getDataType().getNumberOfBytes();
            } else {
                rSize += 8;
            }

        }
        IntField rowWidth = new IntField(rSize);
        rowWidth.encodeBinary(binPage, this.OFFSET_RECORD_WIDTH);

        IntField varOffset = new IntField(binPage.length);
        varOffset.encodeBinary(binPage, this.OFFSET_VARIABLE_OFFSET);
    }

    public TablePageClass(TableSchema schema, byte[] binPage) {

        this.schema = schema;
        this.binPage = binPage;
    }

    private void throwExceptionIfExpired() {
        if (this.isExpired()) {
            throw new PageExpiredException("Page is expired");
        }
    }


    public boolean hasBeenModified() throws PageExpiredException {
        this.throwExceptionIfExpired();
        return beenModified;
    }

    @Override
    public void markExpired() {
        this.expired = true;
    }

    @Override
    public boolean isExpired() {
        return this.expired;
    }

    @Override
    public int getPageNumber() throws PageExpiredException {
        this.throwExceptionIfExpired();
        IntField pageNumField = IntField.getFieldFromBinary(this.binPage, this.OFFSET_PAGE_NUMBER);
        return pageNumField.getValue();
    }

    @Override
    public byte[] getBuffer() {
        return binPage;
    }

    @Override
    public int getNumRecordsOnPage() {

        this.throwExceptionIfExpired();
        IntField nRecords = IntField.getFieldFromBinary(this.binPage, this.OFFSET_NUM_RECORDS);
        return nRecords.getValue();
    }

    private boolean checkVariableOverflow(DataTuple tuple) {

        int bytesVariables = 0;
        for (int i = 0; i < tuple.getNumberOfFields(); i++) {

            if (!schema.getColumn(i).getDataType().getBasicType().isFixLength()) {

                bytesVariables += tuple.getField(i).getNumberOfBytes();
            }
        }
        int varOffset = IntField.getFieldFromBinary(this.binPage, this.OFFSET_VARIABLE_OFFSET).getValue();
        int nRecordsBefore = this.getNumRecordsOnPage();
        int rWidth = IntField.getFieldFromBinary(this.binPage, this.OFFSET_RECORD_WIDTH).getValue();
        int offset = this.TABLE_DATA_PAGE_HEADER_BYTES + nRecordsBefore * rWidth;
        return offset + rWidth > varOffset - bytesVariables;
    }

    @Override
    public boolean insertTuple(DataTuple tuple) throws PageExpiredException, PageFormatException {
        this.throwExceptionIfExpired();

        int rWidth = IntField.getFieldFromBinary(this.binPage, this.OFFSET_RECORD_WIDTH).getValue();
        int beforeVarOffset = IntField.getFieldFromBinary(this.binPage, this.OFFSET_VARIABLE_OFFSET).getValue();


        int nRecordsBefore = this.getNumRecordsOnPage();
        int offset = this.TABLE_DATA_PAGE_HEADER_BYTES + nRecordsBefore * rWidth;
        if (offset + rWidth > beforeVarOffset) {
            return false;
        }

        if (this.checkVariableOverflow(tuple)) {
            return false;
        }

        IntField metadata = new IntField(0);
        metadata.encodeBinary(this.binPage, offset);
        offset += metadata.getNumberOfBytes();

        for (int i = 0; i < tuple.getNumberOfFields(); i++) {
            if (schema.getColumn(i).getDataType().getBasicType().isFixLength()) {
                tuple.getField(i).encodeBinary(this.binPage, offset);
                offset += schema.getColumn(i).getDataType().getNumberOfBytes();
            } else {
                if (tuple.getField(i).isNULL()) {
                    BigIntField varPointer = new BigIntField(0);
                    varPointer.encodeBinary(this.binPage, offset);

                } else {
                    int lengthVariable = tuple.getField(i).getNumberOfBytes();
                    int newVarOffset = IntField.getFieldFromBinary(this.binPage, this.OFFSET_VARIABLE_OFFSET).getValue() - lengthVariable;
                    tuple.getField(i).encodeBinary(this.binPage, newVarOffset);
                    IntField newVarOffsetField = new IntField(newVarOffset);
                    newVarOffsetField.encodeBinary(binPage, this.OFFSET_VARIABLE_OFFSET);

                    BigIntField varPointer = new BigIntField(((long) lengthVariable << 32) + newVarOffset);
                    varPointer.encodeBinary(this.binPage, offset);

                }
                offset += 8;
            }
        }

        IntField nRecordsAfter = new IntField(nRecordsBefore + 1);
        nRecordsAfter.encodeBinary(this.binPage, this.OFFSET_NUM_RECORDS);
        return true;
    }

    @Override
    public DataTuple getDataTuple(int position, long columnBitmap, int numCols) throws PageTupleAccessException, PageExpiredException {
        this.throwExceptionIfExpired();


        int recordWidth = IntField.getFieldFromBinary(this.binPage, this.OFFSET_RECORD_WIDTH).getValue();
        int recordOffset = this.TABLE_DATA_PAGE_HEADER_BYTES + recordWidth * position;
        int metadata = IntField.getFieldFromBinary(this.binPage, recordOffset).getValue();

        if (metadata == 1) {
            return null;
        }
        if (position >= this.getNumRecordsOnPage()) {
            throw new PageTupleAccessException(position);
        }

        DataField[] fields = new DataField[numCols];
        int fields_idx = 0;
        int fieldOffset = recordOffset + 4;
        int totalNumCols = this.schema.getNumberOfColumns();

        for (int i = 0; i < 64; i++) {
            if (i >= totalNumCols) {
                break;
            }
            if (schema.getColumn(i).getDataType().getBasicType().isFixLength()) {
                if (((1L << i) & columnBitmap) != 0) {
                    fields[fields_idx] = schema.getColumn(i).getDataType().getFromBinary(this.binPage, fieldOffset);
                    fields_idx += 1;
                }
                fieldOffset += schema.getColumn(i).getDataType().getNumberOfBytes();

            } else {
                if (((1L << i) & columnBitmap) != 0) {
                    long varPointer = BigIntField.getFieldFromBinary(this.binPage, fieldOffset).getValue();
                    if (varPointer == 0) {
                        fields[fields_idx] = schema.getColumn(i).getDataType().getNullValue();
                    } else {

                        int lengthVariable = (int) (varPointer >> 32);
                        int varOffset = (int) (varPointer);
                        fields[fields_idx] = schema.getColumn(i).getDataType().getFromBinary(this.binPage, varOffset, lengthVariable);
                    }
                    fields_idx += 1;
                }
                fieldOffset += 8;
            }
        }
        return new DataTuple(fields);
    }


    @Override
    public DataTuple getDataTuple(LowLevelPredicate[] preds, int position, long columnBitmap, int numCols) throws PageTupleAccessException, PageExpiredException {
        this.throwExceptionIfExpired();
        long allColumnBitmap = (long) (Math.pow(2, schema.getNumberOfColumns()) - 1);
        int allCols = schema.getNumberOfColumns();
        DataTuple allTuples = getDataTuple(position, allColumnBitmap, allCols);
        if (allTuples == null) {
            return null;
        }

        boolean pass = true;
        for (LowLevelPredicate pred : preds) {
            try {
                pass = pass && pred.evaluate(allTuples);
            } catch (QueryExecutionException e) {
                System.out.println("Couldn't execute query");
            }
        }
        return pass ? getDataTuple(position, columnBitmap, numCols) : null;

    }



    @Override
    public TupleIterator getIterator(int numCols, long columnBitmap) throws PageTupleAccessException, PageExpiredException {
        this.throwExceptionIfExpired();
        ArrayList<DataTuple> dts = new ArrayList<>();
        for (int r_idx = 0; r_idx < this.getNumRecordsOnPage(); r_idx++) {
            DataTuple dt = getDataTuple(r_idx, columnBitmap, numCols);
            if (dt != null) {
                dts.add(dt);
            }
        }
        return new TupIterator(dts);
    }

    @Override
    public TupleIterator getIterator(LowLevelPredicate[] preds,
                                     int numCols, long columnBitmap) throws PageExpiredException, PageTupleAccessException {

        ArrayList<DataTuple> dts = new ArrayList<>();
        for (int r_idx = 0; r_idx < this.getNumRecordsOnPage(); r_idx++) {
            DataTuple dt = getDataTuple(preds, r_idx, columnBitmap, numCols);
            if (dt != null) {
                dts.add(dt);
            }
        }
        return new TupIterator(dts);
    }



    public TupleRIDIterator getIteratorWithRID() throws PageTupleAccessException {

        this.throwExceptionIfExpired();

        long allColumnBitmap = Long.MAX_VALUE;
        int allCols = schema.getNumberOfColumns();

        ArrayList<Pair<DataTuple, RID>> pairs = new ArrayList<>();
        for (int i = 0; i < this.getNumRecordsOnPage(); i++) {
            DataTuple dataTuple = this.getDataTuple(i, allColumnBitmap, allCols);
            if (dataTuple != null) {
                RID recordId = new RID(((long) this.getPageNumber() << 32) + i);
                Pair<DataTuple, RID> pair = new Pair<>(dataTuple, recordId);
                pairs.add(pair);
            }
        }
        return new RidIterator(pairs);

    }

    @Override
    public void deleteTuple(int i) throws PageExpiredException, PageTupleAccessException {
        this.throwExceptionIfExpired();
        int recordWidth = IntField.getFieldFromBinary(this.binPage, this.OFFSET_RECORD_WIDTH).getValue();
        int recordOffset = this.TABLE_DATA_PAGE_HEADER_BYTES + recordWidth * i;
        IntField metadata = new IntField(1);
        metadata.encodeBinary(this.binPage, recordOffset);
    }

    public void setBuffer(byte[] newBuf){

        this.binPage = newBuf;
    }

}