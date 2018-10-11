package com.hero.demo.models;

import org.springframework.data.cassandra.core.cql.Ordering;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.core.mapping.Table;

import java.io.Serializable;
import java.util.UUID;

@Table("users")
public class User implements Serializable {
    private static final long serialVersionUID = 1L;

    @PrimaryKeyColumn(
            name = "ssn",
            ordinal = 2,
            type = PrimaryKeyType.CLUSTERED,
            ordering = Ordering.DESCENDING)
    private UUID ssn;

    @PrimaryKeyColumn(
            name = "name", ordinal = 0, type = PrimaryKeyType.PARTITIONED)
    private String name;

    @PrimaryKeyColumn(
            name = "last", ordinal = 1, type = PrimaryKeyType.PARTITIONED)
    private String last;

    public User(UUID ssn, String name, String last) {
        this.ssn = ssn;
        this.name = name;
        this.last = last;
    }
}
