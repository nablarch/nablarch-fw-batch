package nablarch.fw.batch.integration;

import nablarch.common.io.FileRecordWriterHolder;
import nablarch.core.db.statement.SqlPStatement;
import nablarch.core.db.statement.SqlRow;
import nablarch.core.db.support.DbAccessSupport;
import nablarch.fw.DataReader;
import nablarch.fw.ExecutionContext;
import nablarch.fw.Result;
import nablarch.fw.action.BatchAction;
import nablarch.fw.reader.DatabaseRecordReader;
import nablarch.fw.reader.DatabaseTableQueueReader;

/**
 * ファイルを出力するバッチアクションクラス。
 *
 * @author hisaaki sioiri
 */
public class FileWriterBatchAction extends BatchAction<SqlRow> {

    /** BatchActionHandlerとSQLファイルを共有するための宣言 */
    private static DbAccessSupport dbAccessSupport = new DbAccessSupport(BatchActionHandler.class);

    public Result handle(SqlRow inputData, ExecutionContext ctx) {

        String id = inputData.getString("ID");

        String file = "file_" + id;
        FileRecordWriterHolder.open(file, "test");
        FileRecordWriterHolder.write(inputData, file);

        return new Result.Success();
    }

    @Override
    public DataReader<SqlRow> createReader(ExecutionContext ctx) {
        SqlPStatement statement = dbAccessSupport.getSqlPStatement("SELECT_BATCH_INPUT");
        DatabaseRecordReader reader = new DatabaseRecordReader()
                .setStatement(statement);

        return new DatabaseTableQueueReader(reader, 1000, "ID");
    }

    @Override
    protected void transactionSuccess(SqlRow inputData, ExecutionContext context) {
        SqlPStatement statement = dbAccessSupport.getSqlPStatement("UPDATE_STATUS");
        statement.setString(1, "1");
        statement.setInt(2, inputData.getInteger("id"));
        statement.executeUpdate();
    }

    @Override
    protected void transactionFailure(SqlRow inputData, ExecutionContext context) {
        SqlPStatement statement = dbAccessSupport.getSqlPStatement("UPDATE_STATUS");
        statement.setString(1, "9");
        statement.setString(2, inputData.getString("id"));
        statement.executeUpdate();
    }
}

