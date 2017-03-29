package nablarch.fw.batch.integration;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

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
