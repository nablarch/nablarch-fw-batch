package nablarch.fw.reader;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * READER_BOOK
 */
@Entity
@Table(name = "READER_BOOK")
public class ReaderBook {

    public ReaderBook() {
    }

    public ReaderBook(String title, String publisher, String authors) {
        this.title = title;
        this.publisher = publisher;
        this.authors = authors;
    }

    @Id
    @Column(name = "TITLE", length = 128, nullable = false)
    public String title;

    @Column(name = "PUBLISHER", length = 128, nullable = false)
    public String publisher;

    @Column(name = "AUTHORS", length = 256, nullable = false)
    public String authors;
}