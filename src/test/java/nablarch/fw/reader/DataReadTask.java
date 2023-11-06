package nablarch.fw.reader;

import nablarch.fw.DataReader;
import nablarch.fw.ExecutionContext;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * リーダからデータをリードし、リードした値を返却するタスク。
 * {@link CountDownLatch}を使用して、{@link DataReader#read(ExecutionContext)}を同期的に呼び出す。
 */
public class DataReadTask<T> implements Callable<T> {

    private final DataReader<T> reader;

    private final CountDownLatch latch;

    private final ExecutionContext ctx;

    public DataReadTask(DataReader<T> reader, CountDownLatch latch, ExecutionContext ctx) {
        this.reader = reader;
        this.latch = latch;
        this.ctx = ctx;
    }

    @Override
    public T call() throws Exception {
        latch.countDown();
        latch.await();
        return reader.read(ctx);
    }
}
