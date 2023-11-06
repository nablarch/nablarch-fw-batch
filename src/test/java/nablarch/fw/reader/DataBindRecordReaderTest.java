package nablarch.fw.reader;

import nablarch.common.databind.ObjectMapperFactory;
import nablarch.fw.ExecutionContext;
import nablarch.fw.SynchronizedDataReaderWrapper;
import nablarch.fw.TestSupport;
import nablarch.test.support.SystemRepositoryResource;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasProperty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("NonAsciiCharacters")
public class DataBindRecordReaderTest {

    @Rule
    public final SystemRepositoryResource resource = new SystemRepositoryResource(
            "nablarch/fw/reader/DataBindReaderTest.xml");

    @SuppressWarnings("deprecation")
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    private final File dataFile = new File(System.getProperty("java.io.tmpdir"), "record.csv");

    private DataBindRecordReader<DataBindTestForm> sut = null;

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
    public void ファイルを読み込めること() throws Exception {
        sut = new DataBindRecordReader<>(DataBindTestForm.class)
                .setDataFile("record");

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
    public void hasNextなしでreadを呼び出した場合もファイルを読み込めること() throws Exception {
        sut = new DataBindRecordReader<>(DataBindTestForm.class)
                .setDataFile("record");

        TestSupport.createFile(dataFile, "\r\n", StandardCharsets.UTF_8,
                "年齢,氏名",
                "20,山田太郎",
                "30,鈴木次郎",
                "40,佐藤花子"
        );

        ExecutionContext ctx = new ExecutionContext();

        DataBindTestForm form = sut.read(ctx);
        assertEquals(20, form.getAge().intValue());
        assertEquals("山田太郎", form.getName());
        assertEquals(2, form.getLineNumber());

        assertEquals(3, sut.read(ctx).getLineNumber());
        assertEquals(4, sut.read(ctx).getLineNumber());
        assertNull(sut.read(ctx));
    }

    @Test
    public void 初期化前にcloseを呼び出した場合は例外が送出されないこと() throws Exception {
        sut = new DataBindRecordReader<>(DataBindTestForm.class)
                .setDataFile("record");
        sut.close(new ExecutionContext());
    }

    @Test
    public void close後にデータリーダにアクセスした場合は例外が発生しないこと() throws Exception {
        sut = new DataBindRecordReader<>(DataBindTestForm.class)
                .setDataFile("record");

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
    public void データファイルが存在しない場合は例外が送出されること() throws Exception {
        sut = new DataBindRecordReader<>(DataBindTestForm.class)
                .setDataFile("record");

        expectedException.expect(IllegalStateException.class);
        expectedException.expectCause(allOf(
                instanceOf(FileNotFoundException.class),
                hasProperty("message", containsString("/tmp/record.csv"))
        ));

        assertFalse(dataFile.exists());
        sut.hasNext(new ExecutionContext());
    }

    @Test
    public void データファイルを指定しない場合は例外が送出されること() throws Exception {
        sut = new DataBindRecordReader<>(DataBindTestForm.class);

        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("data file name was blank. data file name must not be blank.");

        sut.hasNext(new ExecutionContext());
    }

    @Test
    public void データファイルにnullを指定した場合は例外が送出されること() throws Exception {
        sut = new DataBindRecordReader<>(DataBindTestForm.class)
                .setDataFile(null);

        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("data file name was blank. data file name must not be blank.");

        sut.hasNext(new ExecutionContext());
    }

    @Test
    public void データファイルのベースパスにnullを指定した場合は例外が送出されること() throws Exception {
        sut = new DataBindRecordReader<>(DataBindTestForm.class)
                .setDataFile(null, "record");

        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("data file base path name was blank. data file base path name must not be blank.");

        sut.hasNext(new ExecutionContext());
    }

    @Test
    public void データファイルが空の場合は例外が送出されないこと() throws Exception {
        sut = new DataBindRecordReader<>(DataBindTestForm.class)
                .setDataFile("record");

        TestSupport.createFile(dataFile, "\r\n", StandardCharsets.UTF_8);

        assertTrue(dataFile.exists());
        assertFalse(sut.hasNext(new ExecutionContext()));
    }

    @Test
    public void バッファーを使用しない場合でもデータを読み込めること() throws Exception {
        sut = new DataBindRecordReader<>(DataBindTestForm.class)
                .setDataFile("record")
                .setUseBuffer(false);

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
    public void バッファーサイズを変更してもデータを読み込めること() throws Exception {
        sut = new DataBindRecordReader<>(DataBindTestForm.class)
                .setDataFile("record")
                .setBufferSize(10);

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
    public void 不正なバッファーサイズ指定時には例外が発生すること() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("buffer size was invalid. buffer size must be greater than 0.");
        sut = new DataBindRecordReader<>(DataBindTestForm.class)
                .setDataFile("record")
                .setBufferSize(0);
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
        sut = new DataBindRecordReader<>(DataBindTestForm.class);
        sut.setObjectMapperIterator(new SleepingObjectMapperIterator<>(
                ObjectMapperFactory.create(DataBindTestForm.class, new FileInputStream(dataFile)),
                500));
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