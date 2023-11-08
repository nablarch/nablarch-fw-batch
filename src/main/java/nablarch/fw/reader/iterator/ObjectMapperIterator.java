package nablarch.fw.reader.iterator;

import nablarch.common.databind.ObjectMapper;

import java.util.Iterator;

/**
 * {@link ObjectMapper}で読み込んだファイルの内容を一行ずつ返す{@link Iterator}実装クラス。
 *
 * @param <T> {@link ObjectMapper} でバインドするクラスの型
 */
public class ObjectMapperIterator <T> implements Iterator<T> {

    /**　オブジェクトマッパー */
    private final ObjectMapper<T> mapper;

    /** データ1行分のオブジェクト */
    private T form;

    /**
     * オブジェクトマッパーがクローズ済みかどうか。
     */
    private boolean closed = false;

    /**
     * {@link ObjectMapper}を引数にObjectMapperIteratorを生成する。
     *
     * @param mapper イテレートするマッパ
     */
    public ObjectMapperIterator(ObjectMapper<T> mapper) {
        this.mapper = mapper;

        // 初回分のデータを読み込む
        form = mapper.read();
    }

    /**
     * 次の行があるかどうかを返す。
     *
     * @return {@code true} 次の行がある場合、 {@code false} 次の行がない場合
     */
    @Override
    public boolean hasNext() {
        if(closed) {
            return false;
        }
        return (form != null);
    }

    /**
     * 1行分のデータを返す。
     *
     * @return 1行分のデータ
     */
    @Override
    public T next() {
        if(closed) {
            return null;
        }
        final T current = form;
        form = mapper.read();
        return current;
    }

    /**
     * このメソッドはサポートされない。
     */
    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    /**
     * マッパーをクローズする。
     */
    public void close() {
        if(!closed) {
            mapper.close();
        }
        closed = true;
    }
}