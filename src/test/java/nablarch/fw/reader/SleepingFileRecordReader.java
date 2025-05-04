package nablarch.fw.reader;

import nablarch.core.dataformat.DataRecord;
import nablarch.core.dataformat.FileRecordReader;

import java.io.File;

/**
 * {@link FileRecordReader#read()}で一定時間待機するために使用する{@link FileRecordReader}。
 */
public class SleepingFileRecordReader extends FileRecordReader {

    /**
     * read()メソッド内での待機時間
     */
    private final long sleepTimeMillis;

    public SleepingFileRecordReader(File dataFile, File layoutFile, int bufferSize, long sleepTimeMillis) {
        super(dataFile, layoutFile, bufferSize);
        this.sleepTimeMillis = sleepTimeMillis;
    }

    public DataRecord read() {
        try {
            Thread.sleep(sleepTimeMillis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return super.read();
    }
}