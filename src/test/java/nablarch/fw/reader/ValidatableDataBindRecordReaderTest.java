package nablarch.fw.reader;

import nablarch.common.databind.ObjectMapper;
import nablarch.common.databind.ObjectMapperFactory;
import nablarch.core.message.ApplicationException;
import nablarch.core.validation.ee.ValidatorUtil;
import nablarch.fw.ExecutionContext;
import nablarch.fw.Result;
import nablarch.fw.SynchronizedDataReaderWrapper;
import nablarch.fw.TestSupport;
import nablarch.fw.reader.iterator.ObjectMapperIterator;
import nablarch.test.support.SystemRepositoryResource;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("NonAsciiCharacters")
public class ValidatableDataBindRecordReaderTest {

    @Rule
    public final SystemRepositoryResource resource = new SystemRepositoryResource(
            "nablarch/fw/reader/DataBindReaderTest.xml");

    private final File dataFile = new File(System.getProperty("java.io.tmpdir"), "record.csv");

    private ValidatableDataBindRecordReader<DataBindTestForm> sut = null;

    private final ValidatableDataBindRecordReader.DataBindFileValidatorAction<DataBindTestForm> validatorAction =
            new ValidatableDataBindRecordReader.DataBindFileValidatorAction<>() {
                @Override
                public Object handle(DataBindTestForm dataBindTestForm, ExecutionContext executionContext) {
                    ValidatorUtil.validate(dataBindTestForm);
                    return new Result.Success();
                }

                @Override
                public void onFileEnd(ExecutionContext ctx) {
                    // nop
                }
            };

    private final ValidatableDataBindRecordReader.DataBindFileValidatorAction<DataBindTestForm> errorAction =
            new ValidatableDataBindRecordReader.DataBindFileValidatorAction<>() {
                @Override
                public void onFileEnd(ExecutionContext ctx) {
                    // nop
                }

                @Override
                public Object handle(DataBindTestForm dataBindTestForm, ExecutionContext context) {
                    throw new OutOfMemoryError();
                }
            };

    private static class MockObjectMapperIterator<T> extends ObjectMapperIterator<T> {

        private boolean closed = false;

        public MockObjectMapperIterator(ObjectMapper<T> mapper) {
            super(mapper);
        }

        public void close() {
            if(closed) {
                return;
            }
            closed = true;
            throw new RuntimeException();
        }

    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @After
    public void tearDown() throws Exception {
        if (sut != null) {
            sut.close(new ExecutionContext());
        }

        if (dataFile.exists()) {
            dataFile.delete();
        }
    }

    @Test
    public void ファイルからデータを取得できること() throws Exception {
        sut = new ValidatableDataBindRecordReader<>(DataBindTestForm.class)
                .setDataFile("record")
                .setValidatorAction(validatorAction);

        TestSupport.createFile(dataFile, "\r\n", StandardCharsets.UTF_8,
                "年齢,氏名",
                "20,山田太郎",
                "30,鈴木次郎",
                "40,佐藤花子"
        );

        ExecutionContext ctx = new ExecutionContext();

        assertTrue(sut.hasNext(ctx));
        DataBindTestForm form = sut.read(ctx);
        assertEquals(20, form.getAge().intValue());
        assertEquals("山田太郎", form.getName());
        assertEquals(2, form.getLineNumber());

        assertTrue(sut.hasNext(ctx));
        assertEquals(3, sut.read(ctx).getLineNumber());
        assertTrue(sut.hasNext(ctx));
        assertEquals(4, sut.read(ctx).getLineNumber());
        assertFalse(sut.hasNext(ctx));
        assertNull(sut.read(ctx));

    }

    @Test
    public void キャッシュからデータを取得できること() throws Exception {
        sut = new ValidatableDataBindRecordReader<>(DataBindTestForm.class)
                .setDataFile("record")
                .setValidatorAction(validatorAction)
                .setUseCache(true);

        TestSupport.createFile(dataFile, "\r\n", StandardCharsets.UTF_8,
                "年齢,氏名",
                "20,山田太郎",
                "30,鈴木次郎",
                "40,佐藤花子"
        );

        ExecutionContext ctx = new ExecutionContext();

        assertTrue(sut.hasNext(ctx));
        DataBindTestForm form = sut.read(ctx);
        assertEquals(20, form.getAge().intValue());
        assertEquals("山田太郎", form.getName());
        assertEquals(2, form.getLineNumber());

        assertTrue(sut.hasNext(ctx));
        assertEquals(3, sut.read(ctx).getLineNumber());
        assertTrue(sut.hasNext(ctx));
        assertEquals(4, sut.read(ctx).getLineNumber());
        assertFalse(sut.hasNext(ctx));
        assertNull(sut.read(ctx));
    }

    @Test
    public void バリデーション失敗時はデータを取得できないこと() throws Exception {
        sut = new ValidatableDataBindRecordReader<>(DataBindTestForm.class)
                .setDataFile("record")
                .setValidatorAction(validatorAction);

        TestSupport.createFile(dataFile, "\r\n", StandardCharsets.UTF_8,
                "年齢,氏名",
                "200,山田太郎",
                "30,鈴木次郎",
                "40,佐藤花子"
        );

        ExecutionContext ctx = new ExecutionContext();

        assertThrows(ApplicationException.class, () -> sut.read(ctx));
        assertFalse(sut.hasNext(ctx));
        assertNull(sut.read(ctx));
    }

    @Test
    public void バリデーションでError発生時はデータを取得できないこと() throws Exception {
        sut = new ValidatableDataBindRecordReader<>(DataBindTestForm.class)
                .setDataFile("record")
                .setValidatorAction(errorAction);

        TestSupport.createFile(dataFile, "\r\n", StandardCharsets.UTF_8,
                "年齢,氏名",
                "20,山田太郎",
                "30,鈴木次郎",
                "40,佐藤花子"
        );

        ExecutionContext ctx = new ExecutionContext();

        assertThrows(OutOfMemoryError.class, () -> sut.read(ctx));
        assertFalse(sut.hasNext(ctx));
        assertNull(sut.read(ctx));
    }

    @Test
    public void バリデーションでError発生時_更に例外が発生した場合は最初に発生したErrorが創出されること() throws Exception {
        sut = new ValidatableDataBindRecordReader<>(DataBindTestForm.class)
                .setDataFile("record")
                .setValidatorAction(errorAction);

        TestSupport.createFile(dataFile, "\r\n", StandardCharsets.UTF_8,
                "年齢,氏名",
                "20,山田太郎",
                "30,鈴木次郎",
                "40,佐藤花子"
        );

        sut.setObjectMapperIterator(new MockObjectMapperIterator<>(ObjectMapperFactory.create(DataBindTestForm.class, new FileInputStream(dataFile))));

        assertThrows(OutOfMemoryError.class, () -> sut.read(new ExecutionContext()));
    }

    @Test
    public void キャッシュ有効時_close済みのデータリーダにアクセスしても例外が発生しないこと() throws Exception {
        sut = new ValidatableDataBindRecordReader<>(DataBindTestForm.class)
                .setDataFile("record")
                .setValidatorAction(validatorAction)
                .setUseCache(true);

        TestSupport.createFile(dataFile, "\r\n", StandardCharsets.UTF_8,
                "年齢,氏名",
                "20,山田太郎",
                "30,鈴木次郎",
                "40,佐藤花子"
        );

        ExecutionContext ctx = new ExecutionContext();

        sut.read(ctx);
        sut.close(ctx);

        assertFalse(sut.hasNext(ctx));
        assertNull(sut.read(ctx));
    }

    @Test
    public void キャッシュ無効時_close済みのデータリーダにアクセスしても例外が発生しないこと() throws Exception {
        sut = new ValidatableDataBindRecordReader<>(DataBindTestForm.class)
                .setDataFile("record")
                .setValidatorAction(validatorAction);

        TestSupport.createFile(dataFile, "\r\n", StandardCharsets.UTF_8,
                "年齢,氏名",
                "20,山田太郎",
                "30,鈴木次郎",
                "40,佐藤花子"
        );

        ExecutionContext ctx = new ExecutionContext();

        sut.read(ctx);
        sut.close(ctx);

        assertFalse(sut.hasNext(ctx));
        assertNull(sut.read(ctx));
    }

    @Test
    public void ValidatorActionを設定しない場合_例外が発生すること() throws Exception {
        sut = new ValidatableDataBindRecordReader<>(DataBindTestForm.class)
                .setDataFile("record");

        TestSupport.createFile(dataFile, "\r\n", StandardCharsets.UTF_8,
                "年齢,氏名",
                "20,山田太郎",
                "30,鈴木次郎",
                "40,佐藤花子"
        );

        IllegalStateException result = assertThrows(IllegalStateException.class, () -> sut.read(new ExecutionContext()));
        assertEquals("FileValidatorAction was not set. an Object that implements the validation logic must be set.", result.getMessage());
    }

    @Test
    public void ValidatorActionにnullを設定する場合_例外が発生すること() throws Exception {
        IllegalArgumentException result = assertThrows(IllegalArgumentException.class,
                () -> new ValidatableDataBindRecordReader<>(DataBindTestForm.class)
                        .setDataFile("record")
                        .setValidatorAction(null));
        assertEquals("validator action was null. validator action must not be null.", result.getMessage());
    }

    @Test
    public void バッファーを使用しない場合でもデータを読み込めること() throws Exception {
        sut = new ValidatableDataBindRecordReader<>(DataBindTestForm.class)
                .setDataFile("record")
                .setUseBuffer(false)
                .setValidatorAction(validatorAction);

        TestSupport.createFile(dataFile, "\r\n", StandardCharsets.UTF_8,
                "年齢,氏名",
                "20,山田太郎",
                "30,鈴木次郎",
                "40,佐藤花子"
        );

        ExecutionContext ctx = new ExecutionContext();

        assertTrue(sut.hasNext(ctx));
        assertNotNull(sut.read(ctx));
    }

    @Test
    public void バッファーサイズを変更してもデータを読み込めること() throws Exception {
        sut = new ValidatableDataBindRecordReader<>(DataBindTestForm.class)
                .setDataFile("record")
                .setBufferSize(10)
                .setValidatorAction(validatorAction);

        TestSupport.createFile(dataFile, "\r\n", StandardCharsets.UTF_8,
                "年齢,氏名",
                "20,山田太郎",
                "30,鈴木次郎",
                "40,佐藤花子"
        );

        ExecutionContext ctx = new ExecutionContext();

        assertTrue(sut.hasNext(ctx));
        assertNotNull(sut.read(ctx));
    }

    @Test
    public void synchronizedでラップした場合_スレッドセーフな挙動となっていること() throws Exception {

        TestSupport.createFile(dataFile, "\r\n", StandardCharsets.UTF_8,
                "年齢,氏名",
                "20,山田太郎",
                "30,鈴木次郎",
                "40,佐藤花子",
                "50,田中一郎"
        );

        // synchronizedでラップしたデータリーダの作成
        sut = new ValidatableDataBindRecordReader<>(DataBindTestForm.class)
                .setValidatorAction(validatorAction)
                .setDataFile("record");
        sut.setObjectMapperIterator(new SleepingObjectMapperIterator<>(
                ObjectMapperFactory.create(DataBindTestForm.class, new FileInputStream(dataFile)),
                500
        ));
        SynchronizedDataReaderWrapper<DataBindTestForm> testReader = new SynchronizedDataReaderWrapper<>(sut);

        // 並列実行するタスクを作成
        List<DataReadTask<DataBindTestForm>> tasks = new ArrayList<>(4);
        DataReadTask<DataBindTestForm> task = new DataReadTask<>(testReader, new CountDownLatch(4), new ExecutionContext());
        for (int i = 0; i < 4; i++) {
            tasks.add(task);
        }

        // 並列実行し、結果を取得
        // DataReadTaskのラッチ機構で各スレッドのread()呼出タイミングを同期し、更にread()内で一定時間待つことで実行タイミングを重複させる。
        ExecutorService executor = Executors.newFixedThreadPool(4);
        List<Future<DataBindTestForm>> result = executor.invokeAll(tasks);

        List<String> actualList = new ArrayList<>();
        for (Future<DataBindTestForm> future : result) {
            actualList.add(future.get().getName());
        }

        assertThat(actualList, hasItems("山田太郎","鈴木次郎","佐藤花子","田中一郎"));
    }
}