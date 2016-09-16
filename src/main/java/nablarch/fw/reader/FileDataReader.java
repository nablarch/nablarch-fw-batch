    package nablarch.fw.reader;

    import java.io.File;

    import nablarch.core.dataformat.DataRecord;
    import nablarch.core.dataformat.FileRecordReader;
    import nablarch.core.util.FilePathSetting;
    import nablarch.core.util.StringUtil;
    import nablarch.core.util.annotation.Published;
    import nablarch.fw.DataReader;
    import nablarch.fw.ExecutionContext;

/**
 * ファイルデータを１レコードづつ読み込み、
 * 読み込んだフィールドの内容を{@link DataRecord}にマッピングして返却するデータリーダ。
 * <p/>
 * 実際のレコード読み込み処理は、{@link FileRecordReader}に委譲する。
 * <p/>
 * このクラスを使用するにあたって設定が必須となるプロパティの実装例を下記に示す。
 * <pre><code>
 *     FileDataReader reader = new FileDataReader()
 *         //フォーマット定義ファイルのベースパス論理名とフォーマット定義ファイル名(拡張子無し)を設定する。
 *         .setLayoutFile("format", "formatFile")
 *         //データファイルベースパス論理名とデータファイル名(拡張子無し)を設定する。
 *         .setDataFile("input", "dataFile");
 * </code></pre>
 *
 * @author Masato Inoue
 */
public class FileDataReader implements DataReader<DataRecord> {

    /** ファイルからの読み込みを行うリーダ */
    private FileRecordReader fileReader;
    
    /** フォーマット定義ファイル名 */
    private String layoutFileName = null;
    
    /** データファイル名 */
    private String dataFileName = null;

    /** ファイル読み込みの際に使用するバッファのサイズ（デフォルトは8192B） */
    private int bufferSize = 8192;

    /** フォーマット定義ファイルのベースパス論理名 */
    private String layoutFileBasePathName;

    /** データファイルのベースパス論理名 */
    private String dataFileBasePathName;

    /**
     * {@code FileDataReader}オブジェクトを生成する。
     *
     */
    @Published(tag = "architect")
    public FileDataReader() {
    }

    /**
     * データファイルを1レコードづつ読み込む。
     * <p/>
     * 読み込んだ際のレコード番号を実行コンテキストに格納する。
     *
     * @param ctx 実行コンテキスト
     * @return 1レコード分のデータレコード（読み込むデータがなかった場合は{@code null}）
     */
    public synchronized DataRecord read(ExecutionContext ctx) {
        if (fileReader == null) {
            fileReader = createFileRecordReader();
        }
        DataRecord record = null;
        if (fileReader.hasNext()) {
            record = fileReader.read();
        }
        ctx.setLastRecordNumber(fileReader.getRecordNumber());
        return record;
    }
    
    /**
     * 次に読み込むデータが存在するかどうかを返却する。
     *
     * @param ctx 実行コンテキスト
     * @return 読み込むデータが存在する場合は {@code true}
     */
    public synchronized boolean hasNext(ExecutionContext ctx) {
        if (fileReader == null) {
            fileReader = createFileRecordReader();
        }
        return fileReader.hasNext();
    }

    /**
     * 指定されたデータファイルに対するストリームを閉じ、ファイルハンドラを開放する。
     * <p/>
     * このリーダを閉じる前に、読み込んだファイルの最終レコードのレコード番号を実行コンテキストに設定する。
     * <p/>
     * このリーダが既に閉じられている場合は何もしない。
     */ 
    public synchronized void close(ExecutionContext ctx) {
        if (fileReader == null) {
            return;
        }
        ctx.setLastRecordNumber(fileReader.getRecordNumber());
        fileReader.close();
    }

    /**
     * 拡張子を除いた、フォーマット定義ファイルのファイル名を設定する。
     * <p/>
     * "format"という論理名のベースパス配下に存在する当該ファイルがフォーマット定義ファイルとして使用される。
     * 
     * @param layoutFile フォーマット定義ファイル名
     * @return このオブジェクト自体
     */
    @Published(tag = "architect")
    public FileDataReader setLayoutFile(String layoutFile) {
        return setLayoutFile("format", layoutFile);
    }
    
    /**
     * フォーマット定義ファイルのベースパス論理名および拡張子を除いたファイル名を設定する。
     * <p/>
     * 設定した論理名のペースパス配下に存在する当該ファイルがフォーマット定義ファイルとして使用される。
     *
     * @param basePathName ベースパス論理名
     * @param fileName フォーマット定義ファイル名
     * @return このオブジェクト自体
     */
    @Published(tag = "architect")
    public FileDataReader setLayoutFile(String basePathName, String fileName) {
        this.layoutFileBasePathName = basePathName;
        this.layoutFileName = fileName;
        return this;
    }

    /**
     * データファイルのファイル名を設定する。
     * <p/>
     * "input"という論理名のベースパス配下に存在する当該ファイルがデータファイルとして使用される。
     * 
     * @param fileName データファイル名
     * @return このオブジェクト自体
     */
    @Published(tag = "architect")
    public FileDataReader setDataFile(String fileName) {
        return setDataFile("input", fileName);
    }
    
    /**
     * データファイルのベースパス論理名およびファイル名を設定する。
     * <p/>
     * 設定したベースパス配下に存在する当該のファイルがデータファイルとして使用される。
     * 
     * @param basePathName ベースパス論理名
     * @param fileName データファイル名
     * @return このオブジェクト自体
     */
    @Published(tag = "architect")
    public FileDataReader setDataFile(String basePathName, String fileName) {
        this.dataFileBasePathName = basePathName;
        this.dataFileName = fileName;
        return this;
    }

    /**
     * レコード読み込み時に使用するバッファのサイズを設定する。
     * <p/>
     * デフォルトでは8KBのバッファを使用する。
     * 
     * @param bufferSize レコード読み込み時に使用するバッファのサイズ
     * @return このオブジェクト自体
     */
    @Published(tag = "architect")
    public FileDataReader setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
        return this;
    }
    
    /**
     * {@code FileRecordReader}オブジェクトを生成する。
     *
     * @return FileRecordReaderオブジェクト
     * @throws IllegalStateException 必須であるプロパティが設定されていない場合
     */
    protected FileRecordReader createFileRecordReader() {
        if (StringUtil.isNullOrEmpty(dataFileName)) {
            throw new IllegalStateException("data file name was blank. data file name must not be blank.");
        }
        if (StringUtil.isNullOrEmpty(layoutFileName)) {
            throw new IllegalStateException("layout file name was blank. layout file name must not be blank.");
        }
        if (StringUtil.isNullOrEmpty(dataFileBasePathName)) {
            throw new IllegalStateException("data file base path name was blank. data file base path name must not be blank.");
        }
        if (StringUtil.isNullOrEmpty(layoutFileBasePathName)) {
            throw new IllegalStateException("layout file base path name was blank. layout file base path name must not be blank.");
        }
        
        FilePathSetting filePathSetting = FilePathSetting.getInstance();
        // データファイルオブジェクトの生成
        File dataFile = filePathSetting.getFileWithoutCreate(dataFileBasePathName, dataFileName);
        // レイアウトファイルオブジェクトの生成
        File layoutFile = filePathSetting.getFileWithoutCreate(layoutFileBasePathName, layoutFileName);

        return new FileRecordReader(dataFile, layoutFile, bufferSize);
    }

    /**
     * {@code FileRecordReader}オブジェクトを取得する。
     * @return FileRecordReaderオブジェクト（FileRecordReaderオブジェクトが生成されていない場合は{@code null}）
     */
    protected synchronized FileRecordReader getFileReader() {
        return fileReader;
    }

    /**
     * {@code FileRecordReader}オブジェクトを設定する。
     * @param fileReader {@code FileRecordReader}オブジェクト
     */
    protected synchronized void setFileReader(FileRecordReader fileReader) {
        this.fileReader = fileReader;
    }
    
    
}
