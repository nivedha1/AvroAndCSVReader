package util;

import com.opencsv.CSVReader;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;


public class Splitter {
    static final Logger logger = Logger.getLogger(Splitter.class);
    int recordCount = 0;
    int fileIndex = 0;
    public int inputFileIndex = 0;
    int recordcountCSV =0;
    public static Schema schema;
    GenericRecord user = null;
    DataFileStream<GenericRecord> reader = null;
    AtomicReference<GenericRecord> recordHolder = null;
    AtomicReference<Boolean> hasNextHolder = null;


    /**
     * Split and merge all files by split size (all the record in file will be parsed)
     *
     * @throws IOException
     */
    public void splitWithMergeAllRecordsBySplitSize(int numberofRecordetoMerge, File inputFile) {
        boolean isReadFileSuccess = true;
        recordCount = 0;
        fileIndex = 0;
        recordHolder = new AtomicReference<>(null);
        try (InputStream in = new FileInputStream(inputFile)) {
            reader = new DataFileStream<>(in, new GenericDatumReader<GenericRecord>());
            user = new GenericData.Record(schema);
            hasNextHolder = new AtomicReference<>(reader.hasNext());
            while (Boolean.TRUE.equals(hasNextHolder.get())) {
                writeRecordForSplitWithMergeAllRecordsBySplitSize(numberofRecordetoMerge, inputFile);
            }
        } catch (IllegalArgumentException e) {
            isReadFileSuccess = false;
            logger.error(ErrorCodes.getErrorMessage(ErrorCodes.CORRUPT_RECORD));
        } catch (IOException | AvroRuntimeException e) {
            isReadFileSuccess = false;
            if (e.getCause() != null && e.getCause().getMessage().equals(Constants.IOEXCEPTION_CLAUSE)) {
                logger.error(ErrorCodes.getErrorMessage(ErrorCodes.CORRUPT_RECORD));
            } else {
                logger.error(ErrorCodes.getErrorMessage(ErrorCodes.READ_RECORD_EXCEPTION));
            }
        } catch (Exception e) {
            isReadFileSuccess = false;
            logger.error(ErrorCodes.getErrorMessage(ErrorCodes.GENERAL_ERROR));
        }finally {
            if(isReadFileSuccess)
                logger.info(Constants.SUCCESS_FILE_READ_MESSAGE+inputFile.getName());
            else
                logger.error(Constants.FAILURE_FILE_READ_MESSAGE+inputFile.getName());
        }
    }

    /**
     * Utility method to Split and merge all files by split size
     *
     * @param numberofRecordetoMerge
     * @param inputFile
     */
    private void writeRecordForSplitWithMergeAllRecordsBySplitSize(int numberofRecordetoMerge,  File inputFile) {
        try (DataFileWriter writer = new DataFileWriter<>(new GenericDatumWriter<GenericRecord>())) {
            writer.setMeta(Constants.META_KEY, inputFile + String.valueOf(UUID.randomUUID()));
            writer.create(user.getSchema(), new File(Constants.OUTPUT_INDIVIDUAL_OPTION_3 + fileIndex + "_"+inputFileIndex + Constants.AVRO_FILE_EXTENSION));
            fileIndex++;
            int j=0;
            for (j = 0; j < numberofRecordetoMerge && Boolean.TRUE.equals(hasNextHolder.get()); j++) {
                recordHolder.set(reader.next(recordHolder.get()));
                writeRecord(writer, inputFile);
                recordCount++;
                hasNextHolder.set(reader.hasNext());
            }
            logger.info(Constants.SUCCESS_FILE_AVRO_RECORD_MESSAGE + j);
        } catch (IOException e) {
            logger.error(ErrorCodes.getErrorMessage((ErrorCodes.WRITE_EXCEPTION)));
        }
    }

    /**
     * Method to write a record into output avro file
     *
     * @param writer
     * @param finalName
     */
    private void writeRecord(DataFileWriter writer, File finalName) {
        try {
            writer.append(recordHolder.get());
            //logger.info(Constants.SUCCESS_CONSTANT_PREFIX + recordCount + Constants.SUCCESS_CONSTANT_SUFFIX + finalName);
        } catch (IOException e) {
            logger.error(Constants.FAILURE_CONSTANT_PREFIX + recordCount + Constants.FAILURE_CONSTANT_SUFFIX + finalName);
            logger.error(ErrorCodes.getErrorMessage((ErrorCodes.WRITE_EXCEPTION)));
        }
    }

    /**
     * Read Schema from Avro file
     *
     * @return
     */
    public static boolean getSchemaFromAvroFile(File inputFile) {
        try {
            DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
            if(checkFile(inputFile)) {
                try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(inputFile, datumReader)) {
                    schema = dataFileReader.getSchema();
                    logger.info("Schema file read successfully");
                    return true;
                }
            }
        } catch (IllegalArgumentException e) {
            logger.error(ErrorCodes.getErrorMessage(ErrorCodes.CORRUPT_SCHEMA));
        } catch (IOException e) {
            if (e.getCause() != null && e.getCause().getMessage().equals(Constants.IOEXCEPTION_CLAUSE)) {
                logger.error(ErrorCodes.getErrorMessage(ErrorCodes.CORRUPT_SCHEMA));
            }
        } catch (Exception e) {
            logger.error(ErrorCodes.getErrorMessage(ErrorCodes.GENERAL_ERROR));
        }
        return false;
    }

    /**
     * Check file size and existence
     *
     * @param f
     */
    public static boolean checkFile(File f) {

        if (!f.exists()) {
            logger.error(ErrorCodes.getErrorMessage(ErrorCodes.EMPTY_FILE));
            return false;
        }
        if (f.length() == 0) {
            logger.error(ErrorCodes.getErrorMessage(ErrorCodes.FILE_NOT_FOUND));
            return false;
        }
        return true;
    }

    public void splitCSVWithMergeAllRecordsBySplitSize(int numberofrectoMerge, File inputFile)  {
        CSVReader reader = null;
        try {
            reader = new CSVReader(new InputStreamReader(new FileInputStream(inputFile)));
        } catch (IOException e) {
            logger.error(ErrorCodes.getErrorMessage(ErrorCodes.READ_CSV_EXCEPTION));
        }
        Iterator<String[]> readerItr = reader.iterator();
        fileIndex=0;
        boolean hasrecord =  false;
        try{
        while(readerItr.hasNext()){
            hasrecord=true;
            recordcountCSV=0;
            try(FileWriter writer = new FileWriter(new File("outputcsv"+fileIndex+".csv"))) {
                while (hasrecord && recordcountCSV <= numberofrectoMerge) {
                    recordcountCSV++;
                    writer.write(Arrays.toString(readerItr.next()));
                    hasrecord=readerItr.hasNext();
                }
            }
            if(!hasrecord)
                break;
            fileIndex++;
            logger.info(Constants.SUCCESS_FILE_CSV_RECORD_MESSAGE + recordcountCSV);
        }
        }
        catch (IOException e){
            logger.error(ErrorCodes.getErrorMessage(ErrorCodes.CSV_FILE_WRITE_EXCEPTION));
        }
        logger.info(Constants.SUCCESS_FILE_READ_MESSAGE+inputFile);
    }
}


