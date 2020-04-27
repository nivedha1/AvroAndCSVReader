package util;

public enum ErrorCodes {


    EMPTY_FILE(1000, "Avro file is Empty"),
    FILE_NOT_FOUND(1001, "Avro file not found"),
    CORRUPT_SCHEMA(1002, "Corrupt schema in avro file"),
    CORRUPT_RECORD(1003, "Corrupt record in avro file"),
    READ_SCHEMA_EXCEPTION(1004, "Error in reading  avro file schema"),
    READ_RECORD_EXCEPTION(1005, "Error in reading  avro file record"),
    WRITE_EXCEPTION(1006, "Error is writing to avro file"),
    AVRO_RUNTIME_EXCEPTION(1007, "Avro runtime write exception"),
    GENERAL_ERROR(1008,"Other general error"),
    EMPTY_DIRECTORY(1009,"The input directory is empty"),
    NOT_A_DIRECTORY(1010,"The input is not a directory"),
    CSV_FILE_WRITE_EXCEPTION(1011, "Error in writing to csv file"),
    READ_CSV_EXCEPTION(1012," Error in reading csv file");

    private final int id;
    private final String msg;

    ErrorCodes(int id, String msg) {
        this.id = id;
        this.msg = msg;
    }

    public int getId() {
        return this.id;
    }

    public static String getErrorMessage(ErrorCodes errorCodes){
        return errorCodes.id + " , " + errorCodes.msg;
    }
}
