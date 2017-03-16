package nablarch.fw.batch.integration;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * テストバッチリクエスト
 *
 */
@Entity
@Table(name = "BATCH_REQUEST")
public class TestBatchRequest3 {

    public TestBatchRequest3() {
    };

    public TestBatchRequest3(String id, String act, String stop, String service) {
        this.id = id;
        this.act = act;
        this.stop = stop;
        this.service = service;
    }

    @Id
    @Column(name = "ID", length = 2)
    public String id;

    @Column(name = "ACT", length = 1,  nullable = false)
    public String act;

    @Column(name = "STOP", length = 1,  nullable = false)
    public String stop;

    @Column(name = "SERVICE", length = 1,  nullable = false)
    public String service;
}
