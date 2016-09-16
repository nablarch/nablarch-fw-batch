package nablarch.fw.reader;

import java.util.LinkedList;
import java.util.List;

import nablarch.core.dataformat.DataRecord;
import nablarch.core.log.Logger;
import nablarch.core.log.LoggerManager;
import nablarch.core.util.annotation.Published;
import nablarch.fw.DataReader;
import nablarch.fw.ExecutionContext;
import nablarch.fw.Handler;
import nablarch.fw.MethodBinder;

/**
 * ファイル内容のバリデーション機能を追加したデータリーダ。
 * <p/>
 * ファイル全件の読み込みを行い、このリーダが提供する{@link FileValidatorAction}に実装されたバリデーションロジックを
 * {@link #setValidatorAction(FileValidatorAction)}から設定することができる。
 * バリデーションが正常終了した場合は、入力ファイルを開きなおして本処理を行う。<br/>
 * また、{@link #setUseCache(boolean)}に{@code true}を設定することで、バリデーション時に読み込んだデータを
 * メモリ上にキャッシュし、都度2回の読み込みを1回に削減することができる。<br/>
 * ただし、データ量によってはメモリリソースを大幅に消費する点に注意すること。<br/>
 *
 * @author Iwauo Tajima
 */
public class ValidatableFileDataReader extends FileDataReader {

    /** ロガー */
    private static final Logger LOGGER = LoggerManager.get(ValidatableFileDataReader.class);

    /**
     * {@code ValidatableFileDataReader}オブジェクトを生成する。
     */
    @Published(tag = "architect")
    public ValidatableFileDataReader() {
    }

// ----------------------------------------------------- Validator

    /**
     * バリデーションを行うオブジェクトが実装するインタフェース。
     * <p/>
     * このインタフェースに定義されたメソッドの他に、
     * 以下のシグニチャを持ったメソッドをレコードタイプ毎に定義する必要がある。<br/>
     * このメソッドには、対象のレコードタイプに合わせたバリデーションロジックを実装する。
     * {@code
     *   public Result "do" + [レコードタイプ名](DataRecord record, ExecutionContext ctx);
     * }
     *
     * @see nablarch.fw.handler.RecordTypeBinding
     */
    @Published
    public static interface FileValidatorAction {
        /**
         * ファイル全件の読み込みが正常終了し、
         * ファイル終端に達するとコールバックされる
         * @param ctx 実行コンテキスト
         */
        void onFileEnd(ExecutionContext ctx);
    }
    
    // ----------------------------------------------------- structure
    /** バリデーションを実装したハンドラ */
    private FileValidatorAction validatorAction = null;
    
    /** バリデーション時に読み込んだデータを本処理で使用するかどうか。 */
    private boolean useCache = false;
    
    /** バリデーションで読み込んだデータを格納するキャッシュ */
    private List<DataRecord> recordCache = null;
    
    /** バリデーション済みフラグ */
    private boolean validated = false;
    
    // ----------------------------------------------------- DataReader I/F
    /**
     * データファイルを1レコードづつ読み込む。
     * <p/>
     * ファイルがバリデーション済みでない場合、バリデーションを行う。<br/>
     * 次に読み込むレコードが存在しない場合、{@code null}を返す。
     * <p/>
     * データをキャッシュしている場合、キャッシュからデータを読み込み返却する。<br/>
     * キャッシュしていない場合、データファイルからデータを読み込み返却する。
     *
     * @param ctx 実行コンテキスト
     * @return １レコード分のデータレコード
     */
    @Override
    public synchronized DataRecord read(ExecutionContext ctx) {
        if (!validated) {
            validate(ctx);
        }
        if (useCache) {
            return (recordCache == null || recordCache.isEmpty())
                  ? null
                  : recordCache.remove(0);
        }
        return super.read(ctx);
    }

    /**
     * 次に読み込むデータが存在するかどうかを返却する。
     * <p/>
     * データをキャッシュしている場合、キャッシュから結果を返却する。<br/>
     * キャッシュしていない場合、データファイルから結果を返却する。
     *
     * @param ctx 実行コンテキスト
     * @return 次に読み込むデータが存在する場合は {@code true}
     */
    @Override
    public synchronized boolean hasNext(ExecutionContext ctx) {
        if (validated && useCache) {
            synchronized (this) {
                return (recordCache == null) ? false
                                             : !recordCache.isEmpty();
            }
        }
        return super.hasNext(ctx);
    }

    /**
     * このリーダの利用を停止し、内部的に保持している各種リソースを解放する。
     * <p/>
     * キャッシュを有効にしていた場合、キャッシュを削除する。
     *
     * @param ctx 実行コンテキスト
     */
    @Override
    public synchronized void close(ExecutionContext ctx) {
        if (useCache) {
            recordCache.clear();
            recordCache = null;
        }
        super.close(ctx);
    }
    
    // ------------------------------------------------------ main routine
    /**
     * バリデーションを行う。
     * <p/>
     * キャッシュを有効にしている場合、読み込んだデータをキャッシュする。<br/>
     * 無効にしている場合、入力ファイルの再読み込みを行うため、{@link FileDataReader}を初期化する。
     *
     * @param ctx 実行コンテキスト
     * @throws IllegalStateException バリデーション処理を実装したオブジェクトが{@code null}の場合
     * @throws RuntimeException 実行時例外が発生した場合
     * @throws Error エラーが発生した場合
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    protected void validate(ExecutionContext ctx) {
        if (validatorAction == null) {
            throw new IllegalStateException(
                "FileValidatorAction was not set. an Object that implements the validation logic must be set."
            );
        }
        try {
            Handler validateHandler;
            if (validatorAction instanceof Handler) {
                validateHandler = (Handler) validatorAction;
            } else {
                MethodBinder<DataReader, ?> binding = ctx.getMethodBinder();
                if (binding == null) {
                    throw new RuntimeException(
                        "MethodBinder was not found. you must set a MethodBinder to the ExecutionHandler "
                      + "or make validator object implement Handler."
                    );
                }
                validateHandler = binding.bind(validatorAction);
            }
            
            if (useCache) {
                recordCache = new LinkedList<DataRecord>();
            }
            while (super.hasNext(ctx)) {
                DataRecord record = super.read(ctx);
                validateHandler.handle(record, ctx);
                if (useCache) {
                    recordCache.add(record);
                }
            }
            validatorAction.onFileEnd(ctx);
            super.close(ctx);
            if (!useCache) {
                initialize();
            }
        } catch (RuntimeException e) {
            try {
                close(ctx);
            } catch (Throwable t) {
                LOGGER.logWarn("failed to release file resource.", t);
            }
            throw e;
        } catch (Error e) {
            try {
                close(ctx);
            } catch (Throwable t) {
                LOGGER.logWarn("failed to release file resource.", t);
            }
            throw e;
        } finally {
            validated = true;
        }
    }
    
    /**
     * キャッシュを使用しない場合、ファイルリーダを初期化する。
     */
    protected void initialize() {
        getFileReader().close();
        setFileReader(null);
    }


    // ----------------------------------------------------- settings
    /**
     * バリデーション時に読み込んだデータをキャッシュし、本処理で使用するかどうかを設定する。
     *
     * @param useCache キャッシュを有効化する場合は{@code true}
     * @return このオブジェクト自体
     */
    @Published(tag = "architect")
    public synchronized ValidatableFileDataReader setUseCache(boolean useCache) {
        this.useCache = useCache;
        return this;
    }
    
    /**
     * バリデーション処理を実装したアクションクラスを設定する。
     *
     * @param validatorAction バリデーションを実装したアクションクラス
     * @return このオブジェクト自体
     * @throws IllegalArgumentException バリデーションを実装したアクションクラスが{@code null}の場合
     */
    @Published(tag = "architect")
    public ValidatableFileDataReader setValidatorAction(
            FileValidatorAction validatorAction) {
        if (validatorAction == null) {
            throw new IllegalArgumentException("validator action was null. validator action must not be null.");
        }
        this.validatorAction = validatorAction;
        return this;
    }
}
