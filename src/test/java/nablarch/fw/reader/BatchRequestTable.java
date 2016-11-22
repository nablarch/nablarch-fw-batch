package nablarch.fw.reader;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * BATCH_REQUEST_TABLE
 */
@Entity
@Table(name = "BATCH_REQUEST_TABLE")
public class BatchRequestTable {

    public BatchRequestTable() {
    }

    public BatchRequestTable(Long id, String data, String status) {
        this.id = id;
        this.data = data;
        this.status = status;
    }

    @Id
    @Column(name = "ID", length = 10, nullable = false)
    public Long id;

    @Column(name = "DATA", length = 200, nullable = false)
    public String data;

    @Column(name = "STATUS", length = 1, nullable = false)
    public String status;
}