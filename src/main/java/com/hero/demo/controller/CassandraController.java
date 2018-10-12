package com.hero.demo.controller;

import com.hero.demo.client.CassandraStubClient;
import com.hero.demo.model.Book;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/cassandra")
public class CassandraController {
    private CassandraStubClient client;

    public CassandraController(CassandraStubClient client) {
        this.client = client;
    }

    @RequestMapping(method=RequestMethod.POST)
    public void create(@RequestBody String bookTitle) {
        client.insertBook(bookTitle);
    }


    @RequestMapping(method=RequestMethod.GET)
    public Book get(@RequestParam String bookTitle) {
        return client.getBookByTitle(bookTitle);
    }

    @RequestMapping(value="all", method=RequestMethod.GET)
    public List<Book> getAll() {
        return client.getAllBooks();
    }
}
