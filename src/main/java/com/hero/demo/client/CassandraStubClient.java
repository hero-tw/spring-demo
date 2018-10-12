package com.hero.demo.client;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.hero.demo.model.Book;
import com.hero.demo.repository.BookRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.UUID;

@Component
public class CassandraStubClient {

    @Autowired
    BookRepository bookRepository;

    @Value("${cassandra.keyspace}")
    private String keyspace;

    @Value("${cassandra.host}")
    private String host;

    @Value("${cassandra.port}")
    private int port;

    private final static String INIT_QUERY = "CREATE TABLE IF NOT EXISTS "
            + "book(title text, "
            + "isbn UUID, "
            + "publisher text, "
            + "PRIMARY KEY ((title), publisher, isbn));";

    @PostConstruct
    public void start() {
        Cluster cluster = Cluster.builder()
                .withPort(port)
                .addContactPoints(host)
                .build();
        Session session = cluster.connect(keyspace);

        session.execute(INIT_QUERY);
    }

    public void insertBook(String bookTitle) {
        String defaultPublisher = "The Publisher";
        Book book = new Book(UUID.randomUUID(), bookTitle, defaultPublisher);
        bookRepository.save(book);
    }

    public Book getBookByTitle(String bookTitle) {
        return bookRepository.findByTitle(bookTitle);
    }

    public List<Book> getAllBooks() {
        return bookRepository.findAll();
    }
}
