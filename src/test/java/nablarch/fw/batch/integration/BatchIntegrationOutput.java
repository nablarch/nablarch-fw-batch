package nablarch.fw.batch.integration;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * テストバッチアウトプット
 *
 */
@Entity
@Table(name = "BATCH_OUTPUT")
public class BatchIntegrationOutput {

    public BatchIntegrationOutput() {
    };

    public BatchIntegrationOutput(Integer id, String data) {
        this.id = id;
        this.data = data;
    }

    @Id
    @Column(name = "ID", length = 10)
    public Integer id;

    @Column(name = "DATA", length = 500,  nullable = false)
    public String data;
}
