package nablarch.fw.handler;

import nablarch.core.ThreadContext;
import nablarch.core.log.Logger;
import nablarch.core.log.LoggerManager;
import nablarch.core.util.annotation.Published;
import nablarch.fw.ExecutionContext;
import nablarch.fw.Handler;

/**
 * プロセスの２重起動をチェックするハンドラ。
 * <p/>
 * <p/>
 * プロセスの２重起動チェックは、{@link DuplicateProcessChecker}にて行う。
 *
 * @author hisaaki sioiri
 */
public class DuplicateProcessCheckHandler implements Handler<Object, Object> {

    /** 終了コード */
    private int exitCode = 500;

    /** プロセス２重起動チェックを行うクラス */
    private DuplicateProcessChecker duplicateProcessChecker;

    /** ロガー */
    private static final Logger LOGGER = LoggerManager.get(DuplicateProcessCheckHandler.class);

    /**
     * {@inheritDoc}
     * <p/>
     * プロセス（リクエスト）が２重起動でないことをチェックする。
     * ２重起動の場合には、例外を送出し処理を終了する。
     */
    public Object handle(Object o, ExecutionContext context) {

        try {
            duplicateProcessChecker.checkAndActive(ThreadContext.getRequestId());
        } catch (AlreadyProcessRunningException ignored) {
            throw new DuplicateProcess(exitCode);
        }

        Throwable originalError = null;
        try {
            return context.handleNext(o);
        } catch (RuntimeException e) {
            originalError = e;
            throw e;
        } catch (Error e) {
            originalError = e;
            throw e;
        } finally {
            inactive(originalError != null);
        }
    }

    /**
     * プロセスの非アクティブ化を行う。
     *
     * 非アクティブ化処理中に例外が発生し、かつ呼び出し元で例外が発生していない場合
     * {@link RuntimeException}を送出する。
     *
     * @param throwException 呼び出し元で例外が発生しているか否か
     */
    private void inactive(final boolean throwException) {
        try {
            duplicateProcessChecker.inactive(ThreadContext.getRequestId());
        } catch (Throwable t) {
            LOGGER.logWarn("failed to disable process.", t);
            if (!throwException) {
                throw new RuntimeException(t);
            }
        }
    }

    /**
     * 終了コードを設定する。
     *
     * @param exitCode 終了コード
     */
    public void setExitCode(int exitCode) {
        if (exitCode <= 0 || exitCode >= 256) {
            throw new IllegalArgumentException("exit code was invalid range. "
                    + "Please set it in the range of 255 from 1. "
                    + "specified value was:" + exitCode);
        }
        this.exitCode = exitCode;
    }

    /**
     * プロセス２重起動チェックを行うクラスを設定する。
     *
     * @param duplicateProcessChecker プロセス２重起動チェックを行うクラス。
     */
    public void setDuplicateProcessChecker(DuplicateProcessChecker duplicateProcessChecker) {
        this.duplicateProcessChecker = duplicateProcessChecker;
    }

    /** プロセスが２重に起動された場合に送出される例外クラス。 */
    @Published(tag = "architect")
    public static class DuplicateProcess extends nablarch.fw.results.InternalError {

        /** 終了コード */
        private int exitCode;

        /**
         * ２重起動を示す例外を生成する。
         *
         * @param exitCode 終了コード
         */
        public DuplicateProcess(int exitCode) {
            super("specified request_id is already used by another process. "
                    + "you can not start two or more processes with same request_id.");
            this.exitCode = exitCode;
        }

        @Override
        public int getStatusCode() {
            return exitCode;
        }
    }
}

