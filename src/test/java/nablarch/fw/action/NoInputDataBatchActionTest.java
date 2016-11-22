package nablarch.fw.action;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

import java.sql.SQLException;
import java.util.List;

import nablarch.fw.launcher.CommandLine;
import nablarch.fw.launcher.Main;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.VariousDbTestHelper;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * {@link NoInputDataBatchAction}のテストクラス。
 *
 * @author hisaaki sioiri
 */
@RunWith(DatabaseTestRunner.class)
public class NoInputDataBatchActionTest {

    @BeforeClass
    public static void setUpClass() {
        // 受信メッセージを格納するためのテーブルをセットアップする。
        VariousDbTestHelper.createTable(TestBatchRequest2.class);
        VariousDbTestHelper.createTable(Hoge.class);
    }
    
    @Before
    public void setUp() {
        // バッチリクエスト
        VariousDbTestHelper.setUpTable(
                new TestBatchRequest2("R000000001", "0", "0"),
                new TestBatchRequest2("R000000002", "0", "0"),
                new TestBatchRequest2("R000000003", "0", "0"),
                new TestBatchRequest2("R000000004", "0", "0"));

        // テストで使用するテーブル
        VariousDbTestHelper.setUpTable(
                new Hoge("1", "0"), new Hoge("2", "0"),
                new Hoge("3", "0"), new Hoge("4", "0"));
    }

    /** 正常終了するケース1 */
    @Test
    public void testNormalEnd1() {

        NoInputDataBatch.CALL_LIST.clear();
        int ret = executeBatchAction("batch-user", "R000000004",
                "NoInputDataBatch");

        // 正常に終わっていることを確認
        assertThat(ret, is(0));

        // 各メソッドが呼び出されたことを確認する。
        // これにより、handleメソッドが1度だけ呼び出されていることが確認できる。
        List<String> callList = NoInputDataBatch.CALL_LIST;
        assertThat(callList.remove(0), is("initialize"));
        assertThat(callList.remove(0), is("handle"));
        assertThat(callList.remove(0), is("terminate"));
        assertThat(callList.isEmpty(), is(true));

        // 本処理で更新したDBのアサート
        // 想定するハンドラ構成でトランザクション制御が正しく行われていることを確認する。
        List<Hoge> query = VariousDbTestHelper.findAll(Hoge.class, "fuga");

                for (Hoge map : query) {
                    String val = (String) map.fuga;
                    assertThat(val, is("1"));
                }
    }

    /** 正常終了するケース2 */
    @Test
    public void testNormalEnd2() {

        // testNormalEnd1とは異なるアクションで起動
        int ret = executeBatchAction("batch-user", "R000000003",
                "NoInputDataBatch2");

        // 正常に終わっていることを確認
        assertThat(ret, is(0));

        // 本処理で更新したDBのアサート
        // 想定するハンドラ構成でトランザクション制御が正しく行われていることを確認する。
        List<Hoge> query = VariousDbTestHelper.findAll(Hoge.class, "fuga");

                for (Hoge map : query) {
                    String val = (String) map.fuga;
                    assertThat(val, is("2"));
                }
    }

    /** 異常終了するケース */
    @Test
    public void testAbNormalEnd1() throws SQLException {

        NoInputDataBatch.CALL_LIST.clear();
        // 本処理で使うテーブルを事前に削除
        VariousDbTestHelper.dropTable(Hoge.class);

        // testNormalEnd1とは異なるアクションで起動
        int ret = executeBatchAction("batch-user", "R000000002",
                "NoInputDataBatch");

        // 異常終了していることを確認する。
        assertThat(ret, not(0));

        // 各メソッドが呼び出されたことを確認する。
        // これにより、handleメソッドが1度だけ呼び出されていることが確認できる。
        List<String> callList = NoInputDataBatch.CALL_LIST;
        assertThat(callList.remove(0), is("initialize"));
        assertThat(callList.remove(0), is("handle"));
        assertThat(callList.remove(0), is("error"));
        assertThat(callList.remove(0), is("terminate"));
        assertThat(callList.isEmpty(), is(true));
    }

    /**
     * テスト対象のバッチアクションを実行する。
     *
     * @param userId ユーザID
     * @param requestId リクエストID
     * @param actionClassName アクションクラス名
     * @return 処理結果
     */
    private static int executeBatchAction(String userId, String requestId,
            String actionClassName) {
        CommandLine commandLine = new CommandLine(
                "-diConfig",
                "nablarch/fw/action/NoInputDataBatch.xml",
                "-userId", userId,
                "-requestPath",
                actionClassName + '/' + requestId);
        return Main.execute(commandLine);
    }
}
