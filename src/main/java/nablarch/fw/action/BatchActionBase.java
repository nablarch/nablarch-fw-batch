package nablarch.fw.action;

import nablarch.core.db.support.DbAccessSupport;
import nablarch.core.log.Logger;
import nablarch.core.log.LoggerManager;
import nablarch.core.log.app.FailureLogUtil;
import nablarch.core.message.Message;
import nablarch.core.message.MessageLevel;
import nablarch.core.message.MessageUtil;
import nablarch.core.util.annotation.Published;
import nablarch.fw.ExecutionContext;
import nablarch.fw.Result;
import nablarch.fw.TransactionEventCallback;
import nablarch.fw.handler.ExecutionHandlerCallback;
import nablarch.fw.launcher.CommandLine;

/**
 * バッチ処理方式において、業務処理が継承する抽象基底クラス。
 * <p/>
 * このクラスには、{@link ExecutionHandlerCallback}インタフェースに関するNOP実装が与えられており、
 * 必要に応じてオーバーライドできるようになっている。
 *
 * @param <D> 本タスクが処理する入力データの型
 * @author Iwauo Tajima
 */
public abstract class BatchActionBase<D>
extends DbAccessSupport
implements ExecutionHandlerCallback<CommandLine, Result>,
           TransactionEventCallback<D> {
    // ---------------------------------------------- override if you need it

    /**
     * 実行管理ハンドラ({@link nablarch.fw.handler.ExecutionHandler})の本処理開始前に一度だけ実行される。
     * <p/>
     * デフォルトでは何もしない。
     * 必要に応じてオーバライドすること。
     *
     * @param command 起動コマンドライン
     * @param context 実行コンテキスト
     */
    @Published
    protected void initialize(CommandLine command, ExecutionContext context) {
        //nop
    }

    /**
     * 実行時例外/エラーの発生によって本処理が終了した場合に一度だけ実行される。
     * <p/>
     * デフォルトでは何もしない。
     * 必要に応じてオーバライドすること。
     *
     * @param error 本処理で発生した実行時例外/エラー
     * @param context 実行コンテキスト
     */
    @Published
    protected void error(Throwable error, ExecutionContext context) {
        //nop
    }

    /**
     * 本処理が終了した場合に一度だけ実行される。
     * （エラー終了した場合でも実行される。）
     * <p/>
     * デフォルトでは何もしない。
     * 必要に応じてオーバライドすること。
     *
     * @param result 本処理の実行結果
     * @param context 実行コンテキスト
     */
    @Published
    protected void terminate(Result result, ExecutionContext context) {
        //nop
    }

    /**
     * トランザクション処理が正常終了した場合に実行される。
     * <p/>
     * デフォルトでは何もしない。
     * 必要に応じてオーバライドすること。
     *
     * @param inputData 入力データ
     * @param context 実行コンテキスト
     */
    @Published
    protected void transactionSuccess(D inputData, ExecutionContext context) {
        // nop
    }

    /**
     * トランザクション処理が異常終了した場合に実行される。
     * <p/>
     * デフォルトでは何もしない。
     * 必要に応じてオーバライドすること。
     *
     * @param inputData 入力データ
     * @param context 実行コンテキスト
     */
    @Published
    protected void transactionFailure(D inputData, ExecutionContext context) {
        // nop
    }

    /** ロガー */
    private static final Logger LOG = LoggerManager.get(BatchActionBase.class);

    /**
     * INFOレベルでログ出力を行う。
     *
     * @param msgId メッセージID
     * @param msgOptions メッセージIDから取得したメッセージに埋め込む値
     */
    @Published
    protected void writeLog(String msgId, Object... msgOptions) {
        Message message = MessageUtil.createMessage(MessageLevel.INFO,
                msgId, msgOptions);
        LOG.logInfo(message.formatMessage());
    }

    /**
     * ERRORレベルで障害ログ出力を行う。
     * <p/>
     * 障害ログは、障害通知ログと障害解析ログの２種類に分けて出力される。
     *
     * @param data 処理対象データ
     * @param failureCode 障害コード（メッセージID）
     * @param msgOptions 障害コードから取得したメッセージに埋め込む値
     */
    @Published
    protected void writeErrorLog(Object data, String failureCode, Object... msgOptions) {
        FailureLogUtil.logError(data, failureCode, msgOptions);
    }

    /**
     * FATALレベルで障害ログ出力を行う。
     * <p/>
     * 障害ログは、障害通知ログと障害解析ログの２種類に分けて出力される。
     *
     * @param data 処理対象データ
     * @param failureCode 障害コード（メッセージID）
     * @param msgOptions 障害コードから取得したメッセージに埋め込む値
     */
    @Published
    protected void writeFatalLog(Object data, String failureCode, Object... msgOptions) {
        FailureLogUtil.logFatal(data, failureCode, msgOptions);
    }

    // -------------------------------------------------- No need to implement
    @Override
    @Published(tag = "architect")
    public final void preExecution(CommandLine commandLine, ExecutionContext context) {
        initialize(commandLine, context);
    }

    @Override
    @Published(tag = "architect")
    public final void errorInExecution(Throwable error, ExecutionContext context) {
        error(error, context);
    }

    @Override
    @Published(tag = "architect")
    public final void postExecution(Result result, ExecutionContext context) {
        terminate(result, context);
    }

    @Override
    @Published(tag = "architect")
    public void transactionNormalEnd(D data, ExecutionContext ctx) {
        transactionSuccess(data, ctx);
    }

    @Override
    @Published(tag = "architect")
    public void transactionAbnormalEnd(Throwable e, D data, ExecutionContext ctx) {
        transactionFailure(data, ctx);
    }
}

