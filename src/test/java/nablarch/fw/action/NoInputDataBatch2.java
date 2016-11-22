package nablarch.fw.action;

import nablarch.core.db.connection.AppDbConnection;
import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.statement.SqlPStatement;
import nablarch.fw.ExecutionContext;
import nablarch.fw.Result;

/**
 * テスト用のバッチアクションその２
 *
 * @author hisaaki sioiri
 */
public class NoInputDataBatch2 extends NoInputDataBatchAction {


    @Override
    public Result handle(ExecutionContext ctx) {
        AppDbConnection connection = DbConnectionContext.getConnection();
        SqlPStatement statement = connection.prepareStatement(
                "update hoge set fuga = '2'");
        statement.executeUpdate();

        return new Result.Success();
    }
}

