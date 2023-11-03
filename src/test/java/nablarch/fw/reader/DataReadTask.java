package nablarch.fw.reader;

import nablarch.fw.DataReader;
import nablarch.fw.ExecutionContext;

import java.util.concurrent.Callable;

/**
 * リーダからデータをリードし、リードした値を返却するタスク。
 */
public class DataReadTask<T> implements Callable<T> {

    private final DataReader<T> reader;

    private final ExecutionContext ctx;

    public DataReadTask(DataReader<T> reader) {
        this.reader = reader;
        this.ctx = new ExecutionContext();
    }

    @Override
    public T call() throws Exception {
        return reader.read(ctx);
    }
}
