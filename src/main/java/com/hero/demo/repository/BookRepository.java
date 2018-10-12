package com.hero.demo.repository;

import com.hero.demo.model.Book;
import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface BookRepository extends CassandraRepository<Book, UUID> {

    Book findByTitle(String bookTitle);
}
