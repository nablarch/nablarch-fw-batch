package nablarch.fw.action;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import nablarch.core.ThreadContext;
import nablarch.core.dataformat.DataRecord;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.log.app.OnMemoryLogWriter;
import nablarch.test.support.message.MockStringResourceHolder;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * {@link BatchActionBaseTest}のテストケース。
 *
 * @author Masato Inoue
 */
public class BatchActionBaseTest {

    @ClassRule
    public static SystemRepositoryResource repositoryResource = new SystemRepositoryResource(
            "nablarch/fw/action/BatchActionBaseTest.xml");

    private static final String[][] MESSAGES = {
        { "MSG0001", "en", " [error] {0} is {1}."},
        { "MSG0002", "en", " [fatal] {0} was {1}."},
        };

    /** 初期化処理（クラス） */
    @Before
    public void setUpClass() throws Exception {
        repositoryResource.getComponentByType(MockStringResourceHolder.class).setMessages(MESSAGES);
        Map<String, String[]> params = new HashMap<String, String[]>();
        params.put("param", new String[]{"10"});
    }


    /** 初期化処理（メソッド） */
    @Before
    public void setUp() throws Exception {
        OnMemoryLogWriter.clear();
        ThreadContext.setLanguage(Locale.ENGLISH);
    }

    @After
    public void tearDown() throws Exception {
        ThreadContext.clear();
    }

    /**
     * Errorレベルの障害ログのテスト。
     */
    @Test
    public void testLogError(){
        // 準備
        BatchActionBaseExtends batchAction = new BatchActionBaseExtends();
        DataRecord dataRecord = new DataRecord();
        dataRecord.put("dataKey01", "dataValue01");
        dataRecord.put("dataKey02", "dataValue02");

        // 実行
        batchAction.writeErrorLog(dataRecord, "MSG0001", "milk", "cat");

        // ログの確認
        List<String> appMessages = OnMemoryLogWriter.getMessages("writer.memory");
        assertTrue(appMessages.get(0).contains("[error] milk is cat."));
    }

    /**
     * Fatalレベルの障害ログのテスト。
     */
    @Test
    public void testLogFatal(){
        // 準備
        BatchActionBaseExtends batchAction = new BatchActionBaseExtends();
        DataRecord dataRecord = new DataRecord();
        dataRecord.put("dataKey01", "dataValue01");
        dataRecord.put("dataKey02", "dataValue02");

        // 実行
        batchAction.writeFatalLog(dataRecord, "MSG0002", "milk", "cat");

        // ログの確認
        List<String> appMessages = OnMemoryLogWriter.getMessages("writer.memory");
        assertTrue(appMessages.get(0).contains("[fatal] milk was cat."));
    }

    /**
     * BatchActionBaseを継承しただけのクラス。
     * @author Masato Inoue
     */
    private class BatchActionBaseExtends extends BatchActionBase<Object> {
        // nop
    }
}
