package nablarch.fw.reader;

import nablarch.common.databind.ObjectMapper;
import nablarch.fw.ExecutionContext;
import nablarch.fw.reader.iterator.ObjectMapperIterator;

/**
 * {@link DataBindRecordReader#read(ExecutionContext)}で一定時間待機するために使用する{@link ObjectMapperIterator}。
 * @param <T>
 */
public class SleepingObjectMapperIterator<T> extends ObjectMapperIterator<T> {

    /**
     * read()メソッド内での待機時間
     */
    private final long sleepTimeMillis;

    public SleepingObjectMapperIterator(ObjectMapper<T> mapper, long sleepTimeMillis) {
        super(mapper);
        this.sleepTimeMillis = sleepTimeMillis;
    }

    public T next() {
        try {
            Thread.sleep(sleepTimeMillis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return super.next();
    }
}
