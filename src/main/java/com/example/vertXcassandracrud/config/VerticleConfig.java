package com.example.vertXcassandracrud.config;

import io.vertx.core.Vertx;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import com.example.vertXcassandracrud.verticle.CassandraVerticle;

@Configuration
public class VerticleConfig {
    @Autowired
    CassandraVerticle cassandraVerticle;

    @PostConstruct
    public void postConstruct(){
        Vertx vertx=Vertx.vertx();
        vertx.deployVerticle(cassandraVerticle);
    }
}
