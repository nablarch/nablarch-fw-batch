package nablarch.fw.reader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import nablarch.core.ThreadContext;
import nablarch.core.dataformat.DataRecord;
import nablarch.core.dataformat.FormatterFactory;
import nablarch.core.dataformat.InvalidDataFormatException;
import nablarch.core.repository.SystemRepository;
import nablarch.core.util.FilePathSetting;
import nablarch.fw.DataReader;
import nablarch.fw.ExecutionContext;
import nablarch.fw.Handler;
import nablarch.fw.Result;
import nablarch.fw.TestSupport;
import nablarch.fw.handler.RecordTypeBinding;
import nablarch.test.support.SystemRepositoryResource;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;


public class ValidatableFileDataReaderTest {

    @Rule
    public final SystemRepositoryResource systemRepositoryResource = new SystemRepositoryResource(
            "nablarch/fw/reader/FileDataReaderTest.xml");

    private final File tempDir = new File(System.getProperty("java.io.tmpdir"));

    @Before
    public void setUp() throws Exception {
        ThreadContext.clear();
        // データフォーマット定義ファイル
        File formatFile = new File(tempDir, "format.fmt");
        TestSupport.createFile(formatFile, Charset.forName("UTF-8"),
                "file-type:    \"Fixed\"",
                "text-encoding: \"sjis\"",
                "record-length: 10",
                "",
                "[classifier]",
                "1 type X(1)",
                "",
                "[header]",
                "type = \"H\"",
                "1  type   X(1) \"H\"",
                "2 ?filler X(9)",
                "",
                "[data]",
                "type = \"D\"",
                "1    type   X(1)  \"D\"",
                "2    amount Z(9)",
                "",
                "[trailer]",
                "type = \"T\"",
                "1    type        X(1)  \"T\"",
                "2    records     Z(3)",
                "5    totalAmount Z(6)"
        );
    }

    @After
    public void tearDown() {
        ThreadContext.clear();
    }

    @Test
    public void testValidateNoCache() throws Exception {
        ThreadContext.setRequestId("test");
        _testValidate(false);
    }

    @Test
    public void testValidateWithCache() throws Exception {
        ThreadContext.setRequestId("test");
        _testValidate(true);
    }

    public void _testValidate(boolean useCache) throws Exception {
        final List<String> activities = new ArrayList<String>();
        class TestValidator implements ValidatableFileDataReader.FileValidatorAction {

            private int totalAmount = 0;

            private int dataRecords = 0;

            public Result doHeader(DataRecord record, ExecutionContext ctx) {
                activities.add("doHeader");
                return new Result.Success();
            }

            public Result doData(DataRecord record, ExecutionContext ctx) {
                dataRecords++;
                totalAmount += record.getBigDecimal("amount")
                                     .intValue();
                activities.add("doData: amount=" + record.get("amount"));
                return new Result.Success();
            }

            public Result doTrailer(DataRecord record, ExecutionContext ctx) {
                activities.add("doTrailer: totalAmount=" + record.get("totalAmount")
                        + " records=" + record.get("records"));

                boolean checkResult
                        = (totalAmount == record.getBigDecimal("totalAmount")
                                                .intValue())
                        && (dataRecords == record.getBigDecimal("records")
                                                 .intValue());
                activities.add("checkResult: " + checkResult);
                return new Result.Success();
            }

            public void onFileEnd(ExecutionContext ctx) {
                activities.add("onFileEnd");
            }
        }

        ExecutionContext ctx = new ExecutionContext()
                .setMethodBinder(new RecordTypeBinding.Binder());

        // 初期検証機能付きリーダー
        DataReader<DataRecord> reader = new ValidatableFileDataReader()
                .setValidatorAction(new TestValidator())
                .setUseCache(useCache)
                .setLayoutFile("format")
                .setDataFile("data");

        String data;
        data = "H         ";
        data += "D000000001";
        data += "D000000010";
        data += "D000000100";
        data += "D000001000";
        data += "D000010000";
        data += "T005011111";

        final FileOutputStream outputStream = new FileOutputStream(new File(tempDir, "data.dat"));
        outputStream.write(data.getBytes("sjis"));
        outputStream.close();

        assertTrue(activities.isEmpty());

        assertTrue(reader.hasNext(ctx));

        assertTrue(activities.isEmpty()); // hasNext()を実行しても精査処理は行われない。

        DataRecord record = reader.read(ctx);
        assertEquals(9, activities.size());
        assertEquals(activities.get(0), "doHeader");
        assertEquals(activities.get(1), "doData: amount=1");
        assertEquals(activities.get(2), "doData: amount=10");
        assertEquals(activities.get(3), "doData: amount=100");
        assertEquals(activities.get(4), "doData: amount=1000");
        assertEquals(activities.get(5), "doData: amount=10000");
        assertEquals(activities.get(6), "doTrailer: totalAmount=11111 records=5");
        assertEquals(activities.get(7), "checkResult: true");
        assertEquals(activities.get(8), "onFileEnd");

        assertEquals("header", record.getRecordType());
        assertEquals("H", record.getString("type"));

        assertTrue(reader.hasNext(ctx));

        record = reader.read(ctx);
        assertEquals("data", record.getRecordType());
        assertEquals("D", record.getString("type"));
        assertEquals(1, record.getBigDecimal("amount")
                              .intValue());

        record = reader.read(ctx);
        assertEquals(10, record.getBigDecimal("amount")
                               .intValue());


        record = reader.read(ctx);
        assertEquals(100, record.getBigDecimal("amount")
                                .intValue());

        record = reader.read(ctx);
        assertEquals(1000, record.getBigDecimal("amount")
                                 .intValue());

        record = reader.read(ctx);
        assertEquals(10000, record.getBigDecimal("amount")
                                  .intValue());

        record = reader.read(ctx);
        assertEquals("trailer", record.getRecordType());
        assertEquals("T", record.getString("type"));
        assertEquals(5, record.getBigDecimal("records")
                              .intValue());
        assertEquals(11111, record.getBigDecimal("totalAmount")
                                  .intValue());

        assertFalse(reader.hasNext(ctx));
        assertNull(reader.read(ctx));


        //------------ ファイルの形式エラーが発生した場合。--------//
        activities.clear();


        data = "H         ";
        data += "D000000001";
        data += "D000000010";
        data += "D000000100";
        data += "000001000"; // フォーマット不正!!
        data += "D000010000";
        data += "T005011111";
        final FileOutputStream stream = new FileOutputStream(new File(tempDir, "data.dat"));
        stream.write(data.getBytes("sjis"));
        stream.close();

        FormatterFactory.getInstance()
                        .setCacheLayoutFileDefinition(false);


        // 初期検証機能付きリーダー
        reader = new ValidatableFileDataReader()
                .setValidatorAction(new TestValidator())
                .setUseCache(useCache)
                .setLayoutFile("format")
                .setDataFile("data");
        try {
            reader.read(ctx);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof InvalidDataFormatException);
        }

        assertEquals(4, activities.size());
        assertEquals(activities.get(0), "doHeader");
        assertEquals(activities.get(1), "doData: amount=1");
        assertEquals(activities.get(2), "doData: amount=10");
        assertEquals(activities.get(3), "doData: amount=100");


        // result.datファイルが正常に書きだされていることの確認
        BufferedReader resultFileReader = new BufferedReader(new InputStreamReader(
                new FileInputStream(new File(tempDir, "data.dat")), "sjis"));
        assertEquals("H         D000000001D000000010D000000100000001000D000010000T005011111",
                resultFileReader.readLine());
        assertNull(resultFileReader.readLine());
        resultFileReader.close();

        assertNull(reader.read(ctx));
    }

    /** ValidaterがHandlerの場合のテスト。 */
    @Test
    public void testValidateHandler() throws Exception {
        final List<String> activities = new ArrayList<String>();
        class TestValidatorHandler implements ValidatableFileDataReader.FileValidatorAction, Handler<Object, Object> {

            public int handleCount = 0;

            private int totalAmount = 0;

            private int dataRecords = 0;

            public Result doHeader(DataRecord record, ExecutionContext ctx) {
                activities.add("doHeader");
                return new Result.Success();
            }

            public Result doData(DataRecord record, ExecutionContext ctx) {
                dataRecords++;
                totalAmount += record.getBigDecimal("amount")
                                     .intValue();
                activities.add("doData: amount=" + record.get("amount"));
                return new Result.Success();
            }

            public Result doTrailer(DataRecord record, ExecutionContext ctx) {
                activities.add("doTrailer: totalAmount=" + record.get("totalAmount")
                        + " records=" + record.get("records"));

                boolean checkResult
                        = (totalAmount == record.getBigDecimal("totalAmount")
                                                .intValue())
                        && (dataRecords == record.getBigDecimal("records")
                                                 .intValue());
                activities.add("checkResult: " + checkResult);
                return new Result.Success();
            }

            public void onFileEnd(ExecutionContext ctx) {
                activities.add("onFileEnd");
            }

            /**
             * handleメソッドでデータレコードを読む
             */
            public Object handle(Object data, ExecutionContext context) {
                DataRecord record = (DataRecord) data;
                handleCount++;
                activities.add("doHandle: " + record.getRecordType());
                return "hoge";
            }
        }

        ExecutionContext ctx = new ExecutionContext()
                .setMethodBinder(new RecordTypeBinding.Binder());

        TestValidatorHandler testValidatorHandler = new TestValidatorHandler();

        // 初期検証機能付きリーダー
        DataReader<DataRecord> reader = new ValidatableFileDataReader()
                .setValidatorAction(testValidatorHandler)
                .setUseCache(false)
                .setLayoutFile("format")
                .setDataFile("data");

        TestSupport.createFile(new File(tempDir, "data.dat"), "", Charset.forName("windows-31j"),
                "H         ",
                "D000000001",
                "D000000010",
                "D000000100",
                "D000001000",
                "D000010000",
                "T005011111"
        );

        assertTrue(activities.isEmpty());

        assertTrue(reader.hasNext(ctx));

        assertTrue(activities.isEmpty()); // hasNext()を実行しても精査処理は行われない。

        reader.read(ctx);
        assertEquals(8, activities.size());
        assertEquals(activities.get(0), "doHandle: header");
        assertEquals(activities.get(1), "doHandle: data");
        assertEquals(activities.get(2), "doHandle: data");
        assertEquals(activities.get(3), "doHandle: data");
        assertEquals(activities.get(4), "doHandle: data");
        assertEquals(activities.get(5), "doHandle: data");
        assertEquals(activities.get(6), "doHandle: trailer");
        assertEquals(activities.get(7), "onFileEnd");

    }

    /** メソッドバインディングを設定しない場合のテスト。 */
    @Test
    public void testInvalidBinding() throws Exception {
        final List<String> activities = new ArrayList<String>();
        class TestValidatorHandler implements ValidatableFileDataReader.FileValidatorAction {

            public boolean isHandle = false;

            public void onFileEnd(ExecutionContext ctx) {
                activities.add("onFileEnd");
            }

        }

        ExecutionContext ctx = new ExecutionContext();

        TestValidatorHandler testValidatorHandler = new TestValidatorHandler();

        // 初期検証機能付きリーダー
        DataReader<DataRecord> reader = new ValidatableFileDataReader()
                .setValidatorAction(testValidatorHandler)
                .setUseCache(false)
                .setLayoutFile("format")
                .setDataFile("data");

        TestSupport.createFile(new File(tempDir, "data.dat"), "", Charset.forName("windows-31j"),
                "H         ",
                "D000000001",
                "D000000010",
                "D000000100",
                "D000001000",
                "D000010000",
                "T005011111"
        );

        assertTrue(activities.isEmpty());

        assertTrue(reader.hasNext(ctx));

        assertTrue(activities.isEmpty()); // hasNext()を実行しても精査処理は行われない。

        try {
            reader.read(ctx);
            fail();
        } catch (RuntimeException e) {
            assertEquals(
                    "MethodBinder was not found. you must set a MethodBinder to the ExecutionHandler or make validator object implement Handler.",
                    e.getMessage());
        }
    }

    /** closeを実行し、キャッシュをクリアした後にhasNextを呼ぶテスト。 */
    @Test
    public void testClose() throws Exception {

        final List<String> activities = new ArrayList<String>();
        class TestValidator implements ValidatableFileDataReader.FileValidatorAction {

            private int totalAmount = 0;

            private int dataRecords = 0;

            public Result doHeader(DataRecord record, ExecutionContext ctx) {
                activities.add("doHeader");
                return new Result.Success();
            }

            public Result doData(DataRecord record, ExecutionContext ctx) {
                dataRecords++;
                totalAmount += record.getBigDecimal("amount")
                                     .intValue();
                activities.add("doData: amount=" + record.get("amount"));
                return new Result.Success();
            }

            public Result doTrailer(DataRecord record, ExecutionContext ctx) {
                activities.add("doTrailer: totalAmount=" + record.get("totalAmount")
                        + " records=" + record.get("records"));

                boolean checkResult
                        = (totalAmount == record.getBigDecimal("totalAmount")
                                                .intValue())
                        && (dataRecords == record.getBigDecimal("records")
                                                 .intValue());
                activities.add("checkResult: " + checkResult);
                return new Result.Success();
            }

            public void onFileEnd(ExecutionContext ctx) {
                activities.add("onFileEnd");
            }
        }

        ExecutionContext ctx = new ExecutionContext()
                .setMethodBinder(new RecordTypeBinding.Binder());

        // 初期検証機能付きリーダー
        DataReader<DataRecord> reader = new ValidatableFileDataReader()
                .setValidatorAction(new TestValidator())
                .setUseCache(true)
                .setLayoutFile("format")
                .setDataFile("data");

        TestSupport.createFile(new File(tempDir, "data.dat"), "", Charset.forName("windows-31j"),
                "H         ",
                "D000000001",
                "D000000010",
                "D000000100",
                "D000001000",
                "D000010000",
                "T005011111"
                );
        

        assertTrue(activities.isEmpty());

        assertTrue(reader.hasNext(ctx));

        assertTrue(activities.isEmpty()); // hasNext()を実行しても精査処理は行われない。

        DataRecord record = reader.read(ctx);
        assertEquals(9, activities.size());
        assertEquals(activities.get(0), "doHeader");
        assertEquals(activities.get(1), "doData: amount=1");
        assertEquals(activities.get(2), "doData: amount=10");
        assertEquals(activities.get(3), "doData: amount=100");
        assertEquals(activities.get(4), "doData: amount=1000");
        assertEquals(activities.get(5), "doData: amount=10000");
        assertEquals(activities.get(6), "doTrailer: totalAmount=11111 records=5");
        assertEquals(activities.get(7), "checkResult: true");
        assertEquals(activities.get(8), "onFileEnd");

        assertEquals("header", record.getRecordType());
        assertEquals("H", record.getString("type"));

        assertTrue(reader.hasNext(ctx));

        record = reader.read(ctx);
        assertEquals("data", record.getRecordType());
        assertEquals("D", record.getString("type"));
        assertEquals(1, record.getBigDecimal("amount")
                              .intValue());

        record = reader.read(ctx);
        assertEquals(10, record.getBigDecimal("amount")
                               .intValue());


        record = reader.read(ctx);
        assertEquals(100, record.getBigDecimal("amount")
                                .intValue());

        record = reader.read(ctx);
        assertEquals(1000, record.getBigDecimal("amount")
                                 .intValue());

        record = reader.read(ctx);
        assertEquals(10000, record.getBigDecimal("amount")
                                  .intValue());

        record = reader.read(ctx);
        assertEquals("trailer", record.getRecordType());
        assertEquals("T", record.getString("type"));
        assertEquals(5, record.getBigDecimal("records")
                              .intValue());
        assertEquals(11111, record.getBigDecimal("totalAmount")
                                  .intValue());
        assertFalse(reader.hasNext(ctx));
        assertNull(reader.read(ctx));

        reader.close(ctx);

        assertFalse(reader.hasNext(ctx));

    }


    /** nullの場合のテスト。 */
    @Test
    public void testReaderNull() throws Exception {
        ValidatableFileDataReader reader = new ValidatableFileDataReader();
        try {
            reader.setValidatorAction(null);
            fail();
        } catch (IllegalArgumentException e) {
            assertTrue(true);
        }

        ExecutionContext ctx = new ExecutionContext();
        try {
            reader.read(ctx);
            fail();
        } catch (IllegalStateException e) {
            assertTrue(true);
        }

    }
}
