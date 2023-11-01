package de.tuberlin.dima.minidb.io.tables;

import de.tuberlin.dima.minidb.catalogue.TableSchema;
import de.tuberlin.dima.minidb.core.BigIntField;
import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.core.IntField;
import de.tuberlin.dima.minidb.core.RID;
import de.tuberlin.dima.minidb.io.cache.PageExpiredException;
import de.tuberlin.dima.minidb.qexec.LowLevelPredicate;
import de.tuberlin.dima.minidb.qexec.QueryExecutionException;
import de.tuberlin.dima.minidb.util.Pair;
import java.util.ArrayList;

public class TablePage {


    private TableSchema schema;
    private byte[] binPage;
    private  final int offsetMagicNum = 0;
    private  final int offsetPageNum = 4;
    private  final int offsetNRecords = 8;
    private  final int offsetRWidth = 12;
    private  final int offsetVarOffset= 16;
    private  final int TABLE_DATA_PAGE_HEADER_BYTES = 32;

    private final int TABLE_DATA_PAGE_HEADER_MAGIC_NUMBER = 0xDEADBEEF;

    private boolean beenModified = false;
    private boolean expired = false;
    public TablePage(TableSchema schema, byte[] binPage, int pageNum){

        this.schema = schema;
        this.binPage = binPage;
        this.beenModified = true;

        IntField magicField = new IntField(TABLE_DATA_PAGE_HEADER_MAGIC_NUMBER);
        magicField.encodeBinary(binPage, this.offsetMagicNum );

        IntField pageNumField = new IntField(pageNum);
        pageNumField.encodeBinary(binPage, this.offsetPageNum );

        IntField nRecords = new IntField(0);
        nRecords.encodeBinary(binPage, this.offsetNRecords );

        int rSize = 4;
        for (int i =0 ; i < schema.getNumberOfColumns(); i++)
        {
            if (schema.getColumn(i).getDataType().getBasicType().isFixLength())  {
                rSize += schema.getColumn(i).getDataType().getNumberOfBytes();
            }
            else{
                rSize += 8;
            }

        }
        IntField rowWidth = new IntField(rSize);
        rowWidth.encodeBinary(binPage, this.offsetRWidth );

        IntField varOffset = new IntField(binPage.length);
        varOffset.encodeBinary(binPage, this.offsetVarOffset );
    }

    public TablePage(TableSchema schema, byte[] binPage){

        this.schema = schema;
        this.binPage = binPage;
    }


    private void throwExceptionIfExpired()
    {
        if (this.expired) {
            throw new PageExpiredException("The page is expired");

        }
    }
    public boolean hasBeenModified(){

        return beenModified;
    }

    public void markExpired(){
        this.expired = true;
    }

    public boolean isExpired(){
        return this.expired;

    }

    public int getPageNumber(){

        this.throwExceptionIfExpired();
        IntField pageNumField = IntField.getFieldFromBinary(this.binPage, this.offsetPageNum);
        return pageNumField.getValue();
    }

    public byte[] getBuffer() {

        this.throwExceptionIfExpired();
        return binPage;
    }

    public int getNumRecordsOnPage(){

        this.throwExceptionIfExpired();
        IntField nRecords = IntField.getFieldFromBinary(this.binPage, this.offsetNRecords);
        return nRecords.getValue();
    }

    public boolean insertTuple(DataTuple tuple) {

        this.throwExceptionIfExpired();
        int rWidth = IntField.getFieldFromBinary(this.binPage, this.offsetRWidth).getValue();
        int beforeVarOffset = IntField.getFieldFromBinary(this.binPage, this.offsetVarOffset).getValue();
        int nRecordsBefore = this.getNumRecordsOnPage();
        int offset = this.TABLE_DATA_PAGE_HEADER_BYTES + nRecordsBefore*rWidth;
        if (offset + rWidth >  beforeVarOffset)
        {
            return false;
        }

        IntField metadata = new IntField(1);
        metadata.encodeBinary(this.binPage, offset);
        offset += metadata.getNumberOfBytes();


        for(int i = 0 ; i < tuple.getNumberOfFields(); i++)
        {
            if (schema.getColumn(i).getDataType().getBasicType().isFixLength())  {
                tuple.getField(i).encodeBinary(this.binPage, offset);
                offset += tuple.getField(i).getNumberOfBytes();
            }
            else{

                int lengthVariable = tuple.getField(i).getNumberOfBytes();
                int varOffset = IntField.getFieldFromBinary(this.binPage, this.offsetVarOffset).getValue() - lengthVariable;
                tuple.getField(i).encodeBinary(this.binPage, varOffset);
                IntField newVarOffset = new IntField(binPage.length);
                newVarOffset.encodeBinary(binPage, this.offsetVarOffset );
                BigIntField varPointer = new BigIntField((long) lengthVariable << 32 + varOffset);
                varPointer.encodeBinary(this.binPage, offset);
                offset += varPointer.getNumberOfBytes();

            }
        }

        IntField nRecordsAfter = new IntField(nRecordsBefore +1);
        nRecordsAfter.encodeBinary(this.binPage, this.offsetNRecords);
        return true;
    }


    public DataTuple getDataTuple(int position, long columnBitmap, int numCols){

        this.throwExceptionIfExpired();
        int rWidth = IntField.getFieldFromBinary(this.binPage, this.offsetRWidth).getValue();
        int recordOffset = this.TABLE_DATA_PAGE_HEADER_BYTES +  rWidth * position;
        DataField[] fields = new DataField[numCols];
        int fields_idx = 0;
        int fieldOffset = recordOffset + 4;

        for (int i = 0; i < 64; i++) {

            if ( ((1L << i) & columnBitmap) != 0) {

                if (schema.getColumn(i).getDataType().getBasicType().isFixLength()) {

                    fields[fields_idx] = schema.getColumn(i).getDataType().getFromBinary(this.binPage, fieldOffset);
                    fieldOffset += schema.getColumn(i).getDataType().getNumberOfBytes();
                    fields_idx += 1;


                } else {

                    long varPointer = BigIntField.getFieldFromBinary(this.binPage, fieldOffset).getValue();
                    int lengthVariable = (int) (varPointer >> 32);
                    int varOffset = (int) (varPointer);
                    fields[fields_idx] = schema.getColumn(i).getDataType().getFromBinary(this.binPage, varOffset, lengthVariable);
                    fields_idx += 1;


                }

            }


            }
        return new DataTuple(fields);
        }



    public DataTuple getDataTuple(LowLevelPredicate[] preds, int position,
                             long columnBitmap, int numCols) throws QueryExecutionException {

        this.throwExceptionIfExpired();
        long allColumnBitmap = (long)(Math.pow(2, schema.getNumberOfColumns()) - 1);
        int allCols = schema.getNumberOfColumns();;
        DataTuple allTupel = getDataTuple(position, allColumnBitmap, allCols);
        boolean pass = true;
        for(LowLevelPredicate pred: preds){

            pass = pass && pred.evaluate(allTupel);
        }

        if(pass){

            return getDataTuple(position, columnBitmap, numCols);

        }

        return null;
    }

    private  class TIteratorimpl implements  TupleIterator{

        private final DataTuple[] dts;
        private int i = 0;
        public TIteratorimpl(DataTuple[] dts)
        {
            this.dts = dts;
        }
        @Override
        public boolean hasNext() throws PageTupleAccessException {
            return (i < this.dts.length);
        }

        @Override
        public DataTuple next() throws PageTupleAccessException {
            DataTuple res = this.dts[i];
            this.i += 1;
            return res;
        }
    }
    public TupleIterator getIterator(int numCols, long columnBitmap){


        this.throwExceptionIfExpired();
        DataTuple[] dts = new DataTuple[this.getNumRecordsOnPage()] ;
        for (int r_idx = 0 ; r_idx < this.getNumRecordsOnPage(); r_idx++)
        {
            dts[r_idx] = getDataTuple(r_idx, columnBitmap,numCols );
        }

        return new TIteratorimpl(dts);
    }

    public TupleIterator getIterator(LowLevelPredicate[] preds,
                              int numCols, long columnBitmap) throws QueryExecutionException {

        DataTuple[] dts = new DataTuple[this.getNumRecordsOnPage()] ;
        for (int r_idx = 0 ; r_idx < this.getNumRecordsOnPage(); r_idx++)
        {
            dts[r_idx] = getDataTuple(preds, r_idx, columnBitmap,numCols );
        }

        return new TIteratorimpl(dts);

    }


    private  class RIteratorimpl implements  TupleRIDIterator{

        private final ArrayList<Pair<DataTuple, RID>> pairs;
        private int i = 0;
        public RIteratorimpl(ArrayList<Pair<DataTuple, RID>> pairs)
        {
            this.pairs = pairs;
        }
        @Override
        public boolean hasNext() throws PageTupleAccessException {
            return (i < this.pairs.size());
        }

        @Override
        public Pair<DataTuple, RID> next() throws PageTupleAccessException {
            Pair<DataTuple, RID> tupelRid = this.pairs.get(i);
            this.i += 1;
            return tupelRid;
        }
    }
    public TupleRIDIterator getIteratorWithRID() throws PageTupleAccessException {

        this.throwExceptionIfExpired();
        long allColumnBitmap = (long)(Math.pow(2, schema.getNumberOfColumns()) - 1);
        int allCols = schema.getNumberOfColumns();;
        TupleIterator dts = this.getIterator(allCols, allColumnBitmap);

        ArrayList<Pair<DataTuple, RID>> pairs = new ArrayList<>();

        int i = 0;
        while(dts.hasNext()){

            RID recordId = new RID((long) this.getPageNumber() << 32 + i);
            Pair<DataTuple, RID> pair = new Pair<>(dts.next(), recordId);
            pairs.add(pair);
            i++;
        }

        return new RIteratorimpl(pairs);


    }


    public void deleteTuple(int i) {

        this.throwExceptionIfExpired();
        int rWidth = IntField.getFieldFromBinary(this.binPage, this.offsetRWidth).getValue();
        int recordOffset = this.TABLE_DATA_PAGE_HEADER_BYTES +  rWidth * i;

        IntField metadata = new IntField(1);
        metadata.encodeBinary(this.binPage, recordOffset);
    }
}
