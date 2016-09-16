package nablarch.fw.action;

import nablarch.core.util.annotation.Published;
import nablarch.fw.DataReader;
import nablarch.fw.ExecutionContext;
import nablarch.fw.Result;

/**
 * 入力データを必要としないバッチ処理用の基本実装クラス。
 * <p/>
 * 本クラスの各メソッドがフレームワークによって呼び出される順序は以下のとおり。
 * <pre>
 * {@code
 * initialize()              <-- 本処理開始前に一度だけ呼ばれる。
 * try {
 *   handle()                <-- 1度だけ呼ばれる。
 * } catch(e) {
 *   error()                 <-- 本処理がエラー終了した場合に、一度だけ呼ばれる。
 * } finally {
 *   terminate()             <-- 本処理が全て終了した後、一度だけ呼ばれる。
 * }
 * }
 * </pre>
 * @author hisaaki sioiri
 */
public abstract class NoInputDataBatchAction extends BatchAction<Object> {

    /**
     * インスタンスを生成する。
     */
    @Published
    public NoInputDataBatchAction() {
    }

    /**
     * データリーダによって読み込まれた1件分の入力データに対する 業務処理を実行する。
     * <p/>
     * 処理を{@link #handle(nablarch.fw.ExecutionContext)}に委譲する。
     */
    @Override
    public final Result handle(Object inputData, ExecutionContext ctx) {
        return handle(ctx);
    }

    /**
     * 本処理を実行する。
     *
     * @param ctx 実行コンテキスト
     * @return 処理結果を表す{@link Result}
     */
    public abstract Result handle(ExecutionContext ctx);

    /**
     * 1度だけ本処理を呼び出すための{@link DataReader}を生成する。
     */
    @Override // SUPPRESS CHECKSTYLE @OverrideでJavaDocは継承されるので除外
    public final DataReader<Object> createReader(ExecutionContext ctx) { // SUPPRESS CHECKSTYLE @OverrideでJavaDocは継承されるので除外

        return new DataReader<Object>() {

            /** リードされたか否か */
            private boolean hasRead = false;

            /** {@inheritDoc} */
            public Object read(ExecutionContext ctx) {
                if (hasRead) {
                    return null;
                } else {
                    hasRead = true;
                    return new Object();
                }
            }

            /** {@inheritDoc} */
            public boolean hasNext(ExecutionContext ctx) {
                return !hasRead;
            }

            /** {@inheritDoc } */
            public void close(ExecutionContext ctx) {
                // nop
            }
        };
    }
}

