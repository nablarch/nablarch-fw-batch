package nablarch.fw.batch.integration;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * テストバッチインプット
 *
 */
@Entity
@Table(name = "BATCH_INPUT")
public class BatchIntegrationInput {

    public BatchIntegrationInput() {
    };

    public BatchIntegrationInput(Integer id, String data, String status) {
        this.id = id;
        this.data = data;
        this.status = status;
    }

    @Id
    @Column(name = "ID", length = 10,  nullable = true)
    public Integer id;

    @Column(name = "DATA", length = 500,  nullable = false)
    public String data;

    @Column(name = "STATUS", length = 1,  nullable = false)
    public String status;
}
