package nablarch.fw.reader;

import nablarch.core.ThreadContext;
import nablarch.core.util.Builder;
import nablarch.fw.DataReader;
import nablarch.fw.ExecutionContext;
import nablarch.fw.handler.LoopHandler;

/**
 * ファイルを読み込むデータリーダをラップして、レジューム機能を追加するデータリーダ。
 * <p>
 * 本クラスは、ラップしたデータリーダの{@link DataReader#read(ExecutionContext) read}メソッドの実行回数を、
 * 正常に処理できたポイントとして実行管理テーブルに保存する。<br/>
 * 正常に処理できたポイントは業務処理がコミットされるタイミングで保存される。
 * 業務処理が失敗しトランザクションがロールバックされる場合、正常に処理できたポイントの保存は行われない。<br/>
 * 障害発生時などに処理を再実行する場合、本クラスは正常に処理できたポイントまでのファイル読み込み（業務処理）をスキップし、
 * 再開ポイント(正常に処理できたポイントの次のポイント)からファイル読み込みを再開する。<br/>
 * そのため、再開ポイント以前のデータに対してパッチを当てた際は、正常に処理できたポイントを0クリアする必要がある。<br/>
 * なお、{@link nablarch.fw.action.FileBatchAction}を継承したバッチ業務アクションを作成する場合は、
 * {@code FileBatchAction}がデフォルトで{@link ValidatableFileDataReader}をラップした{@code ResumeDataReader}を生成するので、
 * アプリケーションプログラマが上記２つのオブジェクトを生成するコードを実装する必要はない。
 *
 * @param <TData> このクラスが読み込んだデータの型
 * @author Masato Inoue
 * @see FileDataReader
 * @see nablarch.fw.reader.ResumePointManager
 */
public class ResumeDataReader<TData> implements DataReader<TData> {

    /** レジューム機能を追加するデータリーダ */
    private DataReader<TData> sourceReader = null;

    /** 実行管理テーブルに格納されている、正常に処理できたポイントの参照・更新を行うクラスのインスタンス */
    private ResumePointManager resumePointManager;

    /** 正常に処理できたポイント */
    private int resumePoint = 0;

    /**
     * レジューム機能を追加するデータリーダからデータを読み込む。
     * <p/>
     * データを読み込んだ回数を正常に処理できたポイントとして実行管理テーブルに保存し、
     * 処理再開時にはその次のポイントから読み込みを再開する。<br/>
     * トランザクションループ制御ハンドラの設定により一定件数ごとにコミットを行なっている場合は、
     * コミット前の最後の処理で正常に処理できたポイントを実行管理テーブルに保存する。<br/>
     * 次に読み込むデータが存在しない場合は{@code null}を返す。
     *
     * @param ctx 実行コンテキスト
     * @return 入力データオブジェクト
     */
    @Override
    public synchronized TData read(ExecutionContext ctx) {
        if (!sourceReader.hasNext(ctx)) {
            return null;
        }
        if (resumePointManager == null) {
            resumePointManager = ResumePointManager.getInstance();
            readToResumePoint(ctx);
        }
        TData readData = sourceReader.read(ctx);
        resumePoint++;
        if (LoopHandler.isAboutToCommit(ctx)) {
            saveResumePoint();
        }
        return readData;
    }

    /**
     * 次に読み込むデータが存在するかどうかを返却する。
     *
     * @param ctx 実行コンテキスト
     * @return 次に読み込むデータが存在する場合は{@code true}
     */
    @Override
    public synchronized boolean hasNext(ExecutionContext ctx) {
        return sourceReader.hasNext(ctx);
    }

    /**
     * このリーダの利用を停止し、内部的に保持している各種リソースを解放する。
     *
     * @param ctx 実行コンテキスト
     */
    @Override
    public synchronized void close(ExecutionContext ctx) {
        sourceReader.close(ctx);
    }

    /**
     * レジューム機能が有効になっている場合、正常に処理できたポイントまでのレジュームを行う。
     *
     * @param ctx 実行コンテキスト
     * @throws IllegalStateException 次に読み込むデータが存在しない場合
     */
    protected void readToResumePoint(ExecutionContext ctx) {

        // 正常に処理できたポイントを取得する
        resumePoint = loadResumePoint();

        int numberOfReads = 0;

        // 正常に処理できたポイントまでレコードを読み込む
        for (; numberOfReads < resumePoint; numberOfReads++) {
            // 次に読み込むデータがなければ、例外をスローする
            if (!sourceReader.hasNext(ctx)) {
                throw new IllegalStateException(getInvalidResumePointMessage(
                        resumePoint, numberOfReads));
            }
            sourceReader.read(ctx);
        }
        // 次に読み込むデータがなければ、例外をスローする
        if (!sourceReader.hasNext(ctx)) {
            throw new IllegalStateException(getInvalidResumePointMessage(
                    resumePoint, numberOfReads) + " Perhaps this request has been completed.");
        }
    }

    /**
     * 実行管理テーブルに格納されている、正常に処理できたポイントが不正な場合の例外メッセージを取得する。
     * @param resumePoint 正常に処理できたポイント
     * @param numberOfReads 入力データの読み込み回数
     * @return 正常に処理できたポイントが不正な場合の例外メッセージ
     */
    private String getInvalidResumePointMessage(int resumePoint,
            int numberOfReads) {
        return Builder.concat(
                "invalid resume point was specified. ",
                "The total number of reads input data was [", numberOfReads , "], "
                , "but resume point was [", resumePoint, "]. "
                , "request id=[", ThreadContext.getRequestId(), "].");
    }

    /**
     * 正常に処理できたポイントを取得する。
     * <p/>
     * レジューム機能が無効になっている場合は0を返す。
     *
     * @return 正常に処理できたポイント
     */
    protected int loadResumePoint() {
        return resumePointManager.loadResumePoint(ThreadContext.getRequestId());
    }

    /**
     * 正常に処理できたポイントを保存する。
     * <p/>
     * レジューム機能が無効になっている場合は何もしない。
     */
    protected void saveResumePoint() {
        resumePointManager.saveResumePoint(ThreadContext.getRequestId(),
                resumePoint);
    }

    /**
     * レジューム機能を追加するデータリーダを設定する。
     *
     * @param sourceReader レジューム機能を追加するデータリーダ
     * @return このオブジェクト自体
     */
    public synchronized ResumeDataReader<TData> setSourceReader(DataReader<TData> sourceReader) {
        this.sourceReader = sourceReader;
        return this;
    }
}
