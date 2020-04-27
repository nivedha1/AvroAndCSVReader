import org.apache.log4j.Logger;
import util.Constants;
import util.ErrorCodes;
import util.Splitter;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;


/**
 * Class to split a avro file into indivisual chunks
 */
public class FileSplit {
    static final Logger logger = Logger.getLogger(FileSplit.class);
    static int caseFlag = 1;

    /**
     * Main method to init the generation and slitting of java file
     *
     * @param args
     */
    public static void main(String[] args) {
        Splitter obj = new Splitter();
        File inputDir = new File(args[0]);
        if (!inputDir.exists()) {
            logger.error(ErrorCodes.getErrorMessage(ErrorCodes.EMPTY_DIRECTORY));
            return;
        }
        if(!inputDir.isDirectory()){
            logger.error(ErrorCodes.getErrorMessage(ErrorCodes.NOT_A_DIRECTORY));
            return;
        }
        logger.info(Constants.START_TIME + new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").format(Calendar.getInstance().getTime()));
        caseFlag = Integer.parseInt(args[1]);
        if (caseFlag == 1) {
            for (File inputFile : inputDir.listFiles()) {
                boolean isSuccessSchemaRead = obj.getSchemaFromAvroFile(inputFile);
                if (isSuccessSchemaRead) {
                    caseFlag = Integer.parseInt(args[1]);
                    int numberofRecordetoMerge = Integer.parseInt(args[2]);
                    obj.splitWithMergeAllRecordsBySplitSize(numberofRecordetoMerge, inputFile);
                    obj.inputFileIndex++;
                }
            }
        }else if(caseFlag==2){
            for (File inputFile : inputDir.listFiles()) {
                int numberofRecordetoMerge = Integer.parseInt(args[2]);
                obj.splitCSVWithMergeAllRecordsBySplitSize(numberofRecordetoMerge, inputFile);
            }
            obj.inputFileIndex++;
        }
        logger.info(Constants.END_TIME +new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").format(Calendar.getInstance().getTime()));
    }
}
