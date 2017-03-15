package nablarch.fw.reader;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import nablarch.core.db.statement.SqlRow;
import nablarch.core.log.Logger;
import nablarch.core.log.LoggerManager;
import nablarch.core.util.annotation.Published;
import nablarch.fw.DataReader;
import nablarch.fw.ExecutionContext;

/**
 * データベースのテーブルを擬似的にキューのように扱うデータリーダ。
 * <p/>
 * 本リーダはデータベースのテーブルをキューのように扱えるようにするため、
 * 処理対象レコードが存在しない場合でも{@link #hasNext(nablarch.fw.ExecutionContext)}は
 * 常に{@code true}を返却し、処理対象が存在するように振る舞う。
 * これにより、データが存在しない場合でも{@link #read(nablarch.fw.ExecutionContext)}が呼び出され、
 * テーブルの最新情報を取得し直すことが可能となる。
 * <p/>
 * 本リーダは、処理対象レコードが存在しない場合、再度最新の情報を取得する。
 * この際に、他のスレッドで処理中のレコードが未処理のまま残っている可能性がある。
 * このため、各スレッドで処理中のレコードをヒープ上に保持し、
 * 読み込んだ対象が他のスレッドで処理中のレコードではないことを確認する。
 * <p/>
 * 対象のレコードが、他のスレッドで処理中である場合には、次のレコードを読み込み再度チェックを行う。
 * 対象のレコードが、他のスレッドで処理中でない場合には、読み込んだレコードをクライアントに返却する。
 *
 * @author hisaaki sioiri
 * @see DatabaseRecordReader
 */
public class DatabaseTableQueueReader implements DataReader<SqlRow> {

    /** ロガー */
    private static final Logger LOGGER = LoggerManager.get(DatabaseTableQueueReader.class);

    /** 実行中要求を保持するオブジェクト */
    private final WorkingInputDataHolder workingInputDataHolder = new WorkingInputDataHolder();

    /**
     * データベースリーダ。
     * <p/>
     * データベースへのアクセスは、このリーダに処理を移譲する。
     */
    private final DatabaseRecordReader originalReader;

    /**
     * データが存在しない場合の待機時間(ミリ秒)。
     */
    private final int waitTime;

    /** 主キーのカラム名リスト */
    private final String[] primaryKeys;

    /** リーダが閉じられているか否か */
    private boolean closed;

    /**
     * データベースをキューとして扱うリーダを生成する。
     *
     * @param originalReader データベースレコードリーダ
     * @param waitTime データが存在しない場合の待機時間（ミリ秒）
     * @param primaryKeys レコードを一意に識別する主キーのカラム名
     */
    @Published
    public DatabaseTableQueueReader(DatabaseRecordReader originalReader, int waitTime, String... primaryKeys) {
        this.originalReader = originalReader;
        this.waitTime = waitTime;
        this.primaryKeys = primaryKeys;
        verifyParameter();
    }

    /**
     * 次のレコードを読み込み返却する。
     * <p/>
     * 本実装では、次のレコードが存在しない場合（カーソルの終端に達した場合）、
     * カーソルを再度開き直し最新の情報を取得後にレコードを読み込み返却する。
     * <p/>
     * 読み込んだレコードが他スレッドで処理中のデータの場合には、
     * そのレコードを読み飛ばし、他のスレッドが処理していないレコードを返却する。
     * <p/>
     * 次に読み込むレコードが存在しない場合は{@code null}を返却する。
     *
     * @param ctx 実行コンテキスト
     * @return レコード
     */
    @Override
    public synchronized SqlRow read(ExecutionContext ctx) {
        if (!originalReader.hasNext(ctx)) {
            // データが存在しない場合は、待機時間分待機後にリソース(カーソル)を開き直す。
            waitThread();
            originalReader.reopen(ctx);
        }

        Thread key = Thread.currentThread();
        workingInputDataHolder.remove(key);
        while (true) {
            SqlRow record = originalReader.read(ctx);
            if (record == null) {
                return null;
            }

            InputDataIdentifier inputDataIdentifier = new InputDataIdentifier(primaryKeys, record);
            if (!workingInputDataHolder.isWorkingRequest(inputDataIdentifier)) {
                workingInputDataHolder.add(key, inputDataIdentifier);
                writeLog(inputDataIdentifier);
                return record;
            }
        }
    }

    /**
     * 要求の識別情報をログに出力する。
     * <p/>
     * 識別情報に個人情報等のようにログに出力すべきではない項目が含まれる場合には、
     * 本メソッドをオーバーライドしマスク処理などを実施後にログ出力を行うこと。
     *
     * @param inputDataIdentifier 要求識別情報
     */
    @Published
    protected void writeLog(InputDataIdentifier inputDataIdentifier) {
        LOGGER.logInfo("read database record. key info: " + inputDataIdentifier);
    }

    /**
     * 次に読み込むデータが存在するかどうかを返却する。
     * <p/>
     * 本実装ではデータベース上に処理対象データが無くなった場合でも処理を継続するため、常にデータ有り{@code true}を返す。<br/>
     * このリーダが閉じられた場合は{@code false}を返す。
     *
     * @param ctx 実行コンテキスト
     * @return 読み込むデータが存在する場合 {@code true}
     */
    @Override
    public synchronized boolean hasNext(ExecutionContext ctx) {
        return !closed;
    }

    /**
     * このリーダの利用を停止し、内部的に保持している各種リソースを解放する。
     *
     * @param ctx 実行コンテキスト
     */
    @Override
    public synchronized void close(ExecutionContext ctx) {
        closed = true;
        originalReader.close(ctx);
    }

    /**
     * 現在のスレッドを{@link #waitTime}分待機する。
     *
     * @throws RuntimeException 割り込みが発生した場合
     */
    private void waitThread() {
        try {
            Thread.sleep(waitTime);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * パラメータ情報が正しいことを検証する。
     *
     * @throws IllegalArgumentException 主キーのカラム名リストが{@code null}または空、
     *                                   もしくは、主キーのカラム名が重複している場合
     */
    private void verifyParameter() {
        if ((primaryKeys == null) || (primaryKeys.length == 0)) {
            throw new IllegalArgumentException("primary keys must be set.");
        }
        Set<String> set = new HashSet<String>(Arrays.asList(primaryKeys));
        if (set.size() != primaryKeys.length) {
            throw new IllegalArgumentException(String.format(
                    "duplicated primary key. must be unique column name. primary keys = %s",
                    Arrays.toString(primaryKeys)));
        }
    }

    /**
     * オリジナルのリーダ({@link DatabaseRecordReader})を取得する。
     * <p/>
     * 本メソッドは、以下の理由によりプロダクションコードからの呼び出しは推奨しない。<br/>
     *
     * オリジナルリーダを取得すること自体は問題ないが、取得したオリジナルのリーダを操作してしまうと、
     * 本リーダ内で保持しているオリジナルのリーダの状態も変更されてしまうため。
     *
     * @return オリジナルのリーダ
     */
    public DatabaseRecordReader getOriginalReader() {
        return originalReader;
    }

    /**
     * リクエストを識別するオブジェクト。
     *
     * @author hisaaki sioiri
     */
    public static class InputDataIdentifier extends LinkedHashMap<String, Object> {

        /**
         * リクエスト識別子を生成する。
         *
         * @param identifierKeys リクエストの識別項目
         * @param request 要求情報
         * @throws IllegalArgumentException 要求情報の中にリクエスト識別項目が含まれていない場合
         */
        public InputDataIdentifier(String[] identifierKeys, SqlRow request) {
            for (String key : identifierKeys) {
                if (!request.containsKey(key)) {
                    throw new IllegalArgumentException(
                            "primary key was not found in request. primary key name = [" + key + ']');
                }
                put(key, request.get(key));
            }
        }
    }

    /**
     * 実行中要求を保持するクラス。
     * <p/>
     * 実行中の要求データをスレッド単位で管理を行う。
     * <p/>
     * ※スレッドが破棄される場合には、{@link DatabaseTableQueueReader}が再作成されるため
     * {@link DatabaseTableQueueReader}内で保持される本クラスのインスタンスも破棄される。
     * このため、本クラス内でスレッド単位に保持されるデータが、
     * スレッド破棄→スレッド再作成を繰り返した場合でもヒープを圧迫することはない。
     *
     * @author hisaaki sioiri
     */
    private static final class WorkingInputDataHolder {

        /** {@code WorkingInputDataHolder}オブジェクトを生成する。 */
        private WorkingInputDataHolder() {
        }

        /**
         * 処理中データを保持するオブジェクト。
         * <p/>
         * テーブル再検索時に他のスレッドで処理中のレコードが取得される可能性がある。
         * このため、処理中のレコードを本オブジェクトで保持しておき、
         * レコード取得後にこのオブジェクト内に存在するレコードをスキップし次のレコードを読み込む
         */
        private final Map<Thread, InputDataIdentifier> executingRequests = new HashMap<Thread, InputDataIdentifier>();

        /**
         * 保持している要求を削除する。
         *
         * @param executor 実行スレッド
         */
        private void remove(Thread executor) {
            executingRequests.remove(executor);
        }

        /**
         * 処理中要求を追加する。
         *
         * @param executor 実行スレッド
         * @param inputDataIdentifier 要求識別子
         */
        private void add(Thread executor, InputDataIdentifier inputDataIdentifier) {
            executingRequests.put(executor, inputDataIdentifier);
        }

        /**
         * 処理中の要求に存在しているか否か。
         *
         * @param inputDataIdentifier 要求識別子
         * @return 引数の要求が処理中の場合には、{@code true}
         */
        private boolean isWorkingRequest(InputDataIdentifier inputDataIdentifier) {
            return executingRequests.containsValue(inputDataIdentifier);
        }
    }
}

