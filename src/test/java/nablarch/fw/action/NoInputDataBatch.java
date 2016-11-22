package nablarch.fw.action;

import java.util.ArrayList;
import java.util.List;

import nablarch.core.db.connection.AppDbConnection;
import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.statement.SqlPStatement;
import nablarch.fw.ExecutionContext;
import nablarch.fw.Result;
import nablarch.fw.launcher.CommandLine;

/**
 * テスト用のバッチアクション
 *
 * @author hisaaki sioiri
 */
public class NoInputDataBatch extends NoInputDataBatchAction {

    /** 呼び出されたメソッドの情報を貯めこむリスト */
    static List<String> CALL_LIST = new ArrayList<String>();

    /**
     * 初期処理。
     *
     * @param command 起動コマンドライン
     * @param context 実行コンテキスト
     */
    @Override
    protected void initialize(CommandLine command, ExecutionContext context) {
        CALL_LIST.add("initialize");
    }

    @Override
    public Result handle(ExecutionContext ctx) {
        CALL_LIST.add("handle");
        AppDbConnection connection = DbConnectionContext.getConnection();
        SqlPStatement statement = connection.prepareStatement(
                "update hoge set fuga = '1'");
        statement.executeUpdate();

        return new Result.Success();
    }

    @Override
    protected void terminate(Result result, ExecutionContext context) {
        CALL_LIST.add("terminate");
    }

    @Override
    protected void error(Throwable error, ExecutionContext context) {
        CALL_LIST.add("error");
    }
}
