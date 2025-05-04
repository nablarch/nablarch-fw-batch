package nablarch.fw.reader;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import nablarch.core.ThreadContext;
import nablarch.core.dataformat.DataRecord;
import nablarch.fw.DataReader;
import nablarch.fw.ExecutionContext;
import nablarch.fw.SynchronizedDataReaderWrapper;
import nablarch.fw.TestSupport;
import nablarch.test.support.SystemRepositoryResource;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * ファイルデータリーダのテストケース。
 * <p/>
 * 観点：
 * ファイルデータリーダを使用して、正常にファイルの読み書きができることのテスト、hasNext、read、closeメソッドの動作テスト、バファサイズの設定テストを行う。
 * また、ExecutionContextにレコード番号が設定されることを確認する。
 *
 * @author Masato Inoue
 */
@SuppressWarnings("NonAsciiCharacters")
public class FileDataReaderTest {

    @Rule
    public final SystemRepositoryResource resource = new SystemRepositoryResource(
            "nablarch/fw/reader/FileDataReaderTest.xml");

    @SuppressWarnings("deprecation")
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    private final File tempDir = new File(System.getProperty("java.io.tmpdir"));

    public FileDataReader sut = null;


    @Before
    public void setUp() throws Exception {
        ThreadContext.clear();
        ThreadContext.setRequestId("test");

        // データフォーマット定義ファイル
        File formatFile = new File(tempDir, "format.fmt");
        TestSupport.createFile(formatFile, StandardCharsets.UTF_8,
                "file-type:    \"Fixed\"",
                "text-encoding: \"sjis\"",
                "record-length: 80",
                "",
                "[Default]",
                "1    byteString     X(10)      ",
                "11   wordString     N(10)      ",
                "21   zoneDigits     Z(10)      ",
                "31   signedZDigits  SZ(10)     ",
                "41   packedDigits   P(10)      ",
                "51   signedPDigits  SP(10)     ",
                "61   nativeBytes    B(10)      ",
                "71   zDecimalPoint  Z(5, 3)    ",
                "76   pDecimalPoint  P(3, 2)    ",
                "79  ?endMark        X(2)   \"00\""
        );
    }

    @After
    public void tearDown() throws Exception {
        ThreadContext.clear();
        if (sut != null) {
            sut.close(new ExecutionContext());
        }
    }

    /** 正常系の読み込みテスト。 */
    @Test
    public void testReadFrom() throws Exception {
        sut = new FileDataReader()
                .setLayoutFile("format")
                .setDataFile("record");

        ExecutionContext ctx = new ExecutionContext();

        byte[] bytes = new byte[80];
        ByteBuffer buff = ByteBuffer.wrap(bytes);

        buff.put("ｱｲｳｴｵｶｷｸｹｺ".getBytes("sjis")); //X(10)
        buff.put("あいうえお".getBytes("sjis"));  //N(10)
        buff.put("1234567890".getBytes("sjis")); //9(10)
        buff.put("123456789".getBytes("sjis"))   //S9(10)
            .put((byte) 0x70); // -1234567890
        buff.put(new byte[] {                    //P(10)
                0x12, 0x34, 0x56, 0x78, (byte) 0x90,
                0x12, 0x34, 0x56, 0x78, (byte) 0x93
        }); // 1234567890123456789
        buff.put(new byte[] {                    //SP(10)
                0x12, 0x34, 0x56, 0x78, (byte) 0x90,
                0x12, 0x34, 0x56, 0x78, (byte) 0x97
        }); // -1234567890123456789
        buff.put(new byte[] {                    // B(10)
                (byte) 0xFF, (byte) 0xEE, (byte) 0xDD, (byte) 0xCC, (byte) 0xBB,
                (byte) 0xAA, (byte) 0x99, (byte) 0x88, (byte) 0x77, (byte) 0x66,
        });
        buff.put("12345".getBytes("sjis"));      //99.999
        // = 12.345
        buff.put(new byte[] {                    //PPP.PP
                0x12, 0x34, 0x53
        }); // = 123.45

        OutputStream dest = new FileOutputStream(new File(tempDir, "record.dat"), false);
        dest.write(bytes);
        dest.write(bytes);
        dest.write(bytes);
        dest.close();
        assertTrue(sut.hasNext(ctx));
        DataRecord record = sut.read(ctx);
        assertEquals(1, record.getRecordNumber()); // 現在のレコード番号の確認
        assertEquals(1, ctx.getLastRecordNumber()); // 物理的に読み込んでいるレコードのレコード番号の確認

        assertEquals(9, record.size());
        assertEquals("ｱｲｳｴｵｶｷｸｹｺ", record.get("byteString"));
        assertEquals("あいうえお", record.get("wordString"));
        assertEquals(new BigDecimal("1234567890"), record.get("zoneDigits"));
        assertEquals(new BigDecimal("-1234567890"), record.get("signedZDigits"));
        assertEquals(new BigDecimal("1234567890123456789"), record.get("packedDigits"));
        assertEquals(new BigDecimal("-1234567890123456789"), record.get("signedPDigits"));
        assertEquals(new BigDecimal("-1234567890123456789"), record.get("signedPDigits"));
        assertEquals(new BigDecimal("12.345"), record.get("zDecimalPoint"));
        assertEquals(new BigDecimal("123.45"), record.get("pDecimalPoint"));

        assertTrue(record.containsKey("nativeBytes"));

        byte[] nativeBytes = record.getValue("nativeBytes");
        assertEquals((byte) 0xFF, nativeBytes[0]);
        assertEquals((byte) 0xEE, nativeBytes[1]);
        assertEquals((byte) 0x66, nativeBytes[9]);

        assertTrue(sut.hasNext(ctx));
        record = sut.read(ctx); //2件め
        assertEquals(2, record.getRecordNumber()); // 現在のレコード番号の確認
        assertEquals(2, ctx.getLastRecordNumber()); // 物理的に読み込んでいるレコードのレコード番号の確認
        assertTrue(sut.hasNext(ctx));
        record = sut.read(ctx); //3件め
        assertEquals(3, record.getRecordNumber()); // 現在のレコード番号の確認
        assertEquals(3, ctx.getLastRecordNumber()); // 物理的に読み込んでいるレコードのレコード番号の確認
        assertFalse(sut.hasNext(ctx));
        assertNull(sut.read(ctx));
        assertEquals(3, ctx.getLastRecordNumber()); // 物理的に読み込んでいるレコードのレコード番号の確認（3からインクリメントされないことの確認）
    }

    /**
     * readメソッドの前にhasNextメソッドを実行するテスト
     * {@link this#testReadFrom()}と結果は変わらない。
     */
    @Test
    public void testReadBeforeHasNext() throws Exception {
        sut = new FileDataReader()
                .setLayoutFile("format")
                .setDataFile("record");

        ExecutionContext ctx = new ExecutionContext();

        byte[] bytes = new byte[80];
        ByteBuffer buff = ByteBuffer.wrap(bytes);

        buff.put("ｱｲｳｴｵｶｷｸｹｺ".getBytes("sjis")); //X(10)
        buff.put("あいうえお".getBytes("sjis"));  //N(10)
        buff.put("1234567890".getBytes("sjis")); //9(10)
        buff.put("123456789".getBytes("sjis"))   //S9(10)
            .put((byte) 0x70); // -1234567890
        buff.put(new byte[] {                    //P(10)
                0x12, 0x34, 0x56, 0x78, (byte) 0x90,
                0x12, 0x34, 0x56, 0x78, (byte) 0x93
        }); // 1234567890123456789
        buff.put(new byte[] {                    //SP(10)
                0x12, 0x34, 0x56, 0x78, (byte) 0x90,
                0x12, 0x34, 0x56, 0x78, (byte) 0x97
        }); // -1234567890123456789
        buff.put(new byte[] {                    // B(10)
                (byte) 0xFF, (byte) 0xEE, (byte) 0xDD, (byte) 0xCC, (byte) 0xBB,
                (byte) 0xAA, (byte) 0x99, (byte) 0x88, (byte) 0x77, (byte) 0x66,
        });
        buff.put("12345".getBytes("sjis"));      //99.999
        // = 12.345
        buff.put(new byte[] {                    //PPP.PP
                0x12, 0x34, 0x53
        }); // = 123.45

        OutputStream dest = new FileOutputStream(new File(tempDir, "record.dat"), false);
        dest.write(bytes);
        dest.write(bytes);
        dest.write(bytes);
        dest.close();
        DataRecord record = sut.read(ctx);
        assertTrue(sut.hasNext(ctx));

        assertEquals(9, record.size());
        assertEquals("ｱｲｳｴｵｶｷｸｹｺ", record.get("byteString"));
        assertEquals("あいうえお", record.get("wordString"));
        assertEquals(new BigDecimal("1234567890"), record.get("zoneDigits"));
        assertEquals(new BigDecimal("-1234567890"), record.get("signedZDigits"));
        assertEquals(new BigDecimal("1234567890123456789"), record.get("packedDigits"));
        assertEquals(new BigDecimal("-1234567890123456789"), record.get("signedPDigits"));
        assertEquals(new BigDecimal("-1234567890123456789"), record.get("signedPDigits"));
        assertEquals(new BigDecimal("12.345"), record.get("zDecimalPoint"));
        assertEquals(new BigDecimal("123.45"), record.get("pDecimalPoint"));

        assertTrue(record.containsKey("nativeBytes"));

        byte[] nativeBytes = record.getValue("nativeBytes");
        assertEquals((byte) 0xFF, nativeBytes[0]);
        assertEquals((byte) 0xEE, nativeBytes[1]);
        assertEquals((byte) 0x66, nativeBytes[9]);

        assertTrue(sut.hasNext(ctx));
        sut.read(ctx); //2件め
        assertTrue(sut.hasNext(ctx));
        sut.read(ctx); //3件め
        assertFalse(sut.hasNext(ctx));
        assertNull(sut.read(ctx));
    }

    /** 初期化前にクローズメソッドを実行した場合に、例外が発生しないことの確認。 */
    @Test
    public void testClose() throws Exception {
        DataReader<DataRecord> reader = new FileDataReader()
                .setLayoutFile("format")
                .setDataFile("record.dat");

        ExecutionContext ctx = new ExecutionContext();
        reader.close(ctx);
    }

    @Test
    public void formatFileNotSpecified_shouldThrowException() throws Exception {
        sut = new FileDataReader()
                .setDataFile("record.dat");

        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("layout file name was blank. layout file name must not be blank.");

        sut.read(new ExecutionContext());
    }

    @Test
    public void dataFileNotSpecified_shouldThrowException() throws Exception {
        sut = new FileDataReader()
                .setLayoutFile("format");

        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("data file name was blank. data file name must not be blank.");

        sut.read(new ExecutionContext());
    }

    @Test
    public void specifyNullForDataFilePath_shouldThrowException() throws Exception {
        sut = new FileDataReader()
                .setLayoutFile("format")
                .setDataFile(null, "record");

        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage(
                "data file base path name was blank. data file base path name must not be blank.");

        sut.read(new ExecutionContext());

    }

    @Test
    public void specifyNullForLayoutFilePath_shouldThrowException() throws Exception {
        sut = new FileDataReader()
                .setLayoutFile(null, "format")
                .setDataFile("record");

        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage(
                "layout file base path name was blank. layout file base path name must not be blank.");

        sut.read(new ExecutionContext());
    }

    /**
     * BufferSizeを設定し、正しく読み込みができること。
     * また、不正なBufferSizeを設定した場合に、例外がスローされること。
     */
    @Test
    public void testSetBufferSize() throws Exception {
        sut = new FileDataReader()
                .setLayoutFile("format")
                .setDataFile("record")
                .setBufferSize(30); // 正常なバッファサイズを設定

        ExecutionContext ctx = new ExecutionContext();


        byte[] bytes = new byte[80];
        ByteBuffer buff = ByteBuffer.wrap(bytes);

        buff.put("ｱｲｳｴｵｶｷｸｹｺ".getBytes("sjis")); //X(10)
        buff.put("あいうえお".getBytes("sjis"));  //N(10)
        buff.put("1234567890".getBytes("sjis")); //9(10)
        buff.put("123456789".getBytes("sjis"))   //S9(10)
            .put((byte) 0x70); // -1234567890
        buff.put(new byte[] {                    //P(10)
                0x12, 0x34, 0x56, 0x78, (byte) 0x90,
                0x12, 0x34, 0x56, 0x78, (byte) 0x93
        }); // 1234567890123456789
        buff.put(new byte[] {                    //SP(10)
                0x12, 0x34, 0x56, 0x78, (byte) 0x90,
                0x12, 0x34, 0x56, 0x78, (byte) 0x97
        }); // -1234567890123456789
        buff.put(new byte[] {                    // B(10)
                (byte) 0xFF, (byte) 0xEE, (byte) 0xDD, (byte) 0xCC, (byte) 0xBB,
                (byte) 0xAA, (byte) 0x99, (byte) 0x88, (byte) 0x77, (byte) 0x66,
        });
        buff.put("12345".getBytes("sjis"));      //99.999
        // = 12.345
        buff.put(new byte[] {                    //PPP.PP
                0x12, 0x34, 0x53
        }); // = 123.45

        OutputStream dest = new FileOutputStream(new File(tempDir, "./record.dat"), false);
        dest.write(bytes);
        dest.write(bytes);
        dest.write(bytes);
        dest.close();
        assertTrue(sut.hasNext(ctx));
        DataRecord record = sut.read(ctx);

        assertEquals(9, record.size());
        assertEquals("ｱｲｳｴｵｶｷｸｹｺ", record.get("byteString"));
        assertEquals("あいうえお", record.get("wordString"));
        assertEquals(new BigDecimal("1234567890"), record.get("zoneDigits"));
        assertEquals(new BigDecimal("-1234567890"), record.get("signedZDigits"));
        assertEquals(new BigDecimal("1234567890123456789"), record.get("packedDigits"));
        assertEquals(new BigDecimal("-1234567890123456789"), record.get("signedPDigits"));
        assertEquals(new BigDecimal("-1234567890123456789"), record.get("signedPDigits"));
        assertEquals(new BigDecimal("12.345"), record.get("zDecimalPoint"));
        assertEquals(new BigDecimal("123.45"), record.get("pDecimalPoint"));

        assertTrue(record.containsKey("nativeBytes"));

        byte[] nativeBytes = record.getValue("nativeBytes");
        assertEquals((byte) 0xFF, nativeBytes[0]);
        assertEquals((byte) 0xEE, nativeBytes[1]);
        assertEquals((byte) 0x66, nativeBytes[9]);

        assertTrue(sut.hasNext(ctx));
        sut.read(ctx); //2件め
        assertTrue(sut.hasNext(ctx));
        sut.read(ctx); //3件め
        assertFalse(sut.hasNext(ctx));
        assertNull(sut.read(ctx));
    }

    @Test
    public void specifyInvalidValueForBufferSize_shouldThrowException() throws Exception {
        sut = new FileDataReader()
                .setLayoutFile("format")
                .setDataFile("record.dat")
                .setBufferSize(0);

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("buffer size was invalid. buffer size must be bigger than 0.");

        sut.read(new ExecutionContext());
    }

    /** データファイルが存在しない場合のテスト。 */
    @Test
    public void dataFileNotFound() throws IOException {
        FileDataReader dataReader = new FileDataReader();
        dataReader.setDataFile("input", "notFound");
        dataReader.setLayoutFile("format", "format");

        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage("I/O error happened while opening the file. file path=");
        expectedException.expectMessage("notFound.dat");
        dataReader.createFileRecordReader();
    }

    /** レイアウトファイルが存在しない場合のテスト。 */
    @Test
    public void layoutFileNotFound() throws IOException {
        FileDataReader dataReader = new FileDataReader();
        dataReader.setDataFile("input", "data.txt");
        dataReader.setLayoutFile("format", "notFound");

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("invalid layout file path was specified. ");
        expectedException.expectMessage("notFound.fmt");
        dataReader.createFileRecordReader();
    }

    @Test
    public void synchronizedでラップした場合_スレッドセーフな挙動となっていること() throws Exception {
        // データフォーマット定義ファイル
        File formatFile = new File(tempDir, "format2.fmt");
        TestSupport.createFile(formatFile, StandardCharsets.UTF_8,
                "file-type:    \"Fixed\"",
                "text-encoding: \"sjis\"",
                "record-length: 10",
                "",
                "[Default]",
                "1  byteString X(10)"
        );

        File dataFile = new File(tempDir, "record.dat");
        OutputStream dest = new FileOutputStream(dataFile, false);
        dest.write("ｱｲｳｴｵｶｷｸｹｺ".getBytes("sjis"));
        dest.write("ｻｼｽｾｿﾀﾁﾂﾃﾄ".getBytes("sjis"));
        dest.write("ﾅﾆﾇﾈﾉﾊﾋﾌﾍﾎ".getBytes("sjis"));
        dest.write("ﾏﾐﾑﾒﾓﾔﾕﾖﾗﾘ".getBytes("sjis"));
        dest.close();

        // synchronizedでラップしたデータリーダの作成
        sut = new FileDataReader();
        sut.setFileReader(new SleepingFileRecordReader(dataFile, formatFile, 8192, 500));
        SynchronizedDataReaderWrapper<DataRecord> testReader = new SynchronizedDataReaderWrapper<>(sut);

        // 並列実行するタスクを作成
        List<DataReadTask<DataRecord>> tasks = new ArrayList<>(4);
        DataReadTask<DataRecord> task = new DataReadTask<>(testReader, new CountDownLatch(4), new ExecutionContext());
        for (int i = 0; i < 4; i++) {
            tasks.add(task);
        }

        // 並列実行し、結果を取得
        // DataReadTaskのラッチ機構で各スレッドのread()呼出タイミングを同期し、更にread()内で一定時間待つことで実行タイミングを重複させる。
        ExecutorService executor = Executors.newFixedThreadPool(4);
        List<Future<DataRecord>> result = executor.invokeAll(tasks);

        List<String> actualList = new ArrayList<>();
        for (Future<DataRecord> future : result) {
            actualList.add((String) future.get().get("byteString"));
        }

        assertThat(actualList, hasItems("ｱｲｳｴｵｶｷｸｹｺ","ｻｼｽｾｿﾀﾁﾂﾃﾄ","ﾅﾆﾇﾈﾉﾊﾋﾌﾍﾎ","ﾏﾐﾑﾒﾓﾔﾕﾖﾗﾘ"));
    }
}
