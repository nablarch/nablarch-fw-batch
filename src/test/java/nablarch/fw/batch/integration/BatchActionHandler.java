package nablarch.fw.batch.integration;

import nablarch.core.db.statement.SqlPStatement;
import nablarch.core.db.statement.SqlRow;
import nablarch.fw.DataReader;
import nablarch.fw.ExecutionContext;
import nablarch.fw.Result;
import nablarch.fw.action.BatchAction;
import nablarch.fw.reader.DatabaseRecordReader;
import nablarch.fw.reader.DatabaseTableQueueReader;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 機能テスト用のバッチアクションハンドラ。
 *
 * 入力データを単純に出力テーブルに書き込むだけの業務アクションハンドラ。
 *
 * @author hisaaki sioiri
 */
public class BatchActionHandler extends BatchAction<SqlRow> {

    private static final Pattern ID_EXTRACTION_PATTERN = Pattern.compile("data_([0-9]+)");

    @Override
    public Result handle(SqlRow inputData, ExecutionContext ctx) {

        String data = inputData.getString("data");
        Matcher matcher = ID_EXTRACTION_PATTERN.matcher(data);
        String id;
        if (matcher.find()) {
            id = matcher.group(1);
        } else {
            throw new IllegalArgumentException("invalid data. data=" + data);
        }

        SqlPStatement statement = getSqlPStatement("INSERT_OUTPUT");
        statement.setInt(1, Integer.parseInt(id));
        statement.setString(2, data);
        statement.executeUpdate();
        return new Result.Success();
    }

    @Override
    public DataReader<SqlRow> createReader(ExecutionContext ctx) {
        SqlPStatement statement = getSqlPStatement("SELECT_BATCH_INPUT");
        DatabaseRecordReader reader = new DatabaseRecordReader()
                .setStatement(statement);

        return new DatabaseTableQueueReader(reader, 1000, "ID");
    }

    @Override
    protected void transactionSuccess(SqlRow inputData, ExecutionContext context) {
        SqlPStatement statement = getSqlPStatement("UPDATE_STATUS");
        statement.setString(1, "1");
        statement.setInt(2, inputData.getInteger("id"));
        statement.executeUpdate();
    }

    @Override
    protected void transactionFailure(SqlRow inputData, ExecutionContext context) {
        SqlPStatement statement = getSqlPStatement("UPDATE_STATUS");
        statement.setString(1, "9");
        statement.setInt(2, inputData.getInteger("id"));
        statement.executeUpdate();
    }
}

