package com.example.vertXcassandracrud.verticle;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.vertx.cassandra.CassandraClient;
import io.vertx.cassandra.CassandraClientOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.CorsHandler;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

@Component
public class CassandraVerticle extends AbstractVerticle {

    private CassandraClient cassandraClient;

    @Override
    public void start(Promise<Void> startPromise) {
        CassandraClientOptions options = new CassandraClientOptions()
                .addContactPoint("127.0.0.1", 9042)
                .setKeyspace("my_company");

        cassandraClient = CassandraClient.createShared(vertx, options);
        Router router = Router.router(vertx);

        // CORS configuration
        Set<String> allowedHeaders = new HashSet<>();
        allowedHeaders.add("x-requested-with");
        allowedHeaders.add("Access-Control-Allow-Origin");
        allowedHeaders.add("origin");
        allowedHeaders.add("Content-Type");
        allowedHeaders.add("accept");

        Set<HttpMethod> allowedMethods = new HashSet<>();
        allowedMethods.add(HttpMethod.GET);
        allowedMethods.add(HttpMethod.POST);
        allowedMethods.add(HttpMethod.PUT);
        allowedMethods.add(HttpMethod.DELETE);
        allowedMethods.add(HttpMethod.OPTIONS);

        router.route().handler(CorsHandler.create("*")
                .allowedHeaders(allowedHeaders)
                .allowedMethods(allowedMethods));

        router.options("/*").handler(ctx -> {
            ctx.response()
                    .putHeader("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
                    .putHeader("Access-Control-Allow-Headers", String.join(", ", allowedHeaders))
                    .putHeader("Content-Type", "application/json")
                    .end();
        });


        router.get("/products").handler(this::getProducts);
        router.post("/products").handler(this::addProduct);
        router.put("/products/:product_id").handler(this::updateProduct);
        router.delete("/products/:product_id").handler(this::removeProduct);

        vertx.createHttpServer()
                .requestHandler(router)
                .listen(9191, http -> {
                    if (http.succeeded()) {
                        startPromise.complete();
                        System.out.println("HTTP server started on port 9191");
                    } else {
                        startPromise.fail(http.cause());
                    }
                });
    }
    private boolean isNumeric(String str) {
        if (str == null || str.isEmpty()) {
            return false;
        }
        try {
            Double.parseDouble(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private void getProducts(RoutingContext context) {
        String filter = context.request().getParam("filter");
        cassandraClient.executeWithFullFetch("SELECT * FROM products ALLOW FILTERING").onComplete(ar -> {
            if (ar.succeeded()) {
                JsonArray products = new JsonArray();
                ar.result().forEach(row -> {
                    String productName = row.getString("product_name");
                    double retailPrice = row.getDouble("retail_price");

                    // Filter logic based on the filter string
                    boolean matches = false;
                    if (filter == null || filter.isEmpty()) {
                        matches = true;
                    } else {
                        String lowerCaseFilter = filter.toLowerCase();
                        if (productName.toLowerCase().startsWith(lowerCaseFilter)) {
                            matches = true;
                        } else if (isNumeric(filter)) {
                            String retailPriceStr = String.valueOf(retailPrice);
                            if (retailPriceStr.startsWith(filter)) {
                                matches = true;
                            }
                        }
                    }

                    if (matches) {
                        JsonObject product = new JsonObject()
                                .put("product_id", row.getUuid("product_id"))
                                .put("product_name", productName)
                                .put("retail_price", retailPrice);
                        products.add(product);
                    }
                });
                JsonObject responseJson = new JsonObject().put("products", products);
                context.response()
                        .putHeader("content-type", "application/json")
                        .end(responseJson.encode());
            } else {
                ar.cause().printStackTrace();
                context.response().setStatusCode(500).end(ar.cause().getMessage());
            }
        });
    }

    private void addProduct(RoutingContext routingContext) {
        routingContext.request().bodyHandler(buffer -> {
            try {
                JsonObject productJson = buffer.toJsonObject();
                if (productJson == null) {
                    routingContext.response()
                            .setStatusCode(400)
                            .putHeader("content-type", "application/json")
                            .end(new JsonObject().put("error", "Invalid JSON payload").encode());
                    return;
                }

                String productName = productJson.getString("product_name");
                Double retailPrice = productJson.getDouble("retail_price");

                UUID productId = UUID.randomUUID();

                cassandraClient.prepare("INSERT INTO products (product_id, product_name, retail_price) VALUES (?, ?, ?)")
                        .onSuccess(preparedStatement -> {
                            BoundStatement boundStatement = preparedStatement.bind(productId, productName, retailPrice);
                            cassandraClient.execute(boundStatement)
                                    .onSuccess(resultSet -> {
                                        routingContext.response()
                                                .setStatusCode(201)
                                                .putHeader("content-type", "application/json")
                                                .end(new JsonObject().put("message", "Product added successfully").encode());
                                    })
                                    .onFailure(throwable -> {
                                        throwable.printStackTrace();
                                        routingContext.response()
                                                .setStatusCode(500)
                                                .putHeader("content-type", "application/json")
                                                .end(new JsonObject().put("error", "Failed to add product").encode());
                                    });
                        })
                        .onFailure(throwable -> {
                            throwable.printStackTrace();
                            routingContext.response()
                                    .setStatusCode(500)
                                    .putHeader("content-type", "application/json")
                                    .end(new JsonObject().put("error", "Failed to prepare statement").encode());
                        });
            } catch (DecodeException e) {
                routingContext.response()
                        .setStatusCode(400)
                        .putHeader("content-type", "application/json")
                        .end(new JsonObject().put("error", "Invalid JSON payload").encode());
            }
        });
    }

    private void updateProduct(RoutingContext routingContext) {
        // Extracting productId from query parameters
        String productIdString = routingContext.request().getParam("product_id");

        if (productIdString == null || productIdString.isEmpty()) {
            routingContext.response()
                    .setStatusCode(400)
                    .putHeader("content-type", "application/json")
                    .end(new JsonObject().put("error", "Product ID is missing in the request").encode());
            return;
        }

        UUID productId = UUID.fromString(productIdString);

        routingContext.request().bodyHandler(buffer -> {
            try {
                JsonObject productJson = buffer.toJsonObject();
                if (productJson == null) {
                    routingContext.response()
                            .setStatusCode(400)
                            .putHeader("content-type", "application/json")
                            .end(new JsonObject().put("error", "Invalid JSON payload").encode());
                    return;
                }

                String productName = productJson.getString("product_name");
                Double retailPrice = productJson.getDouble("retail_price");

                if (productName == null || retailPrice == null) {
                    routingContext.response()
                            .setStatusCode(400)
                            .putHeader("content-type", "application/json")
                            .end(new JsonObject().put("error", "Product name or price missing in the request").encode());
                    return;
                }

                cassandraClient.prepare("UPDATE products SET product_name = ?, retail_price = ? WHERE product_id = ?")
                        .onSuccess(preparedStatement -> {
                            BoundStatement boundStatement = preparedStatement.bind(productName, retailPrice, productId);
                            cassandraClient.execute(boundStatement)
                                    .onSuccess(resultSet -> {
                                        if (resultSet.wasApplied()) {
                                            routingContext.response()
                                                    .setStatusCode(200)
                                                    .putHeader("content-type", "application/json")
                                                    .end(new JsonObject().put("message", "Product updated successfully").encode());
                                        } else {
                                            routingContext.response()
                                                    .setStatusCode(404)
                                                    .putHeader("content-type", "application/json")
                                                    .end(new JsonObject().put("error", "Product not found").encode());
                                        }
                                    })
                                    .onFailure(throwable -> {
                                        throwable.printStackTrace();
                                        routingContext.response()
                                                .setStatusCode(500)
                                                .putHeader("content-type", "application/json")
                                                .end(new JsonObject().put("error", "Failed to update product").encode());
                                    });
                        })
                        .onFailure(throwable -> {
                            throwable.printStackTrace();
                            routingContext.response()
                                    .setStatusCode(500)
                                    .putHeader("content-type", "application/json")
                                    .end(new JsonObject().put("error", "Failed to prepare statement").encode());
                        });
            } catch (DecodeException e) {
                routingContext.response()
                        .setStatusCode(400)
                        .putHeader("content-type", "application/json")
                        .end(new JsonObject().put("error", "Invalid JSON payload").encode());
            }
        });
    }

    private void removeProduct(RoutingContext routingContext) {
        // Extract product ID from the path parameters
        String productIdParam = routingContext.request().getParam("product_id");

        if (productIdParam == null || productIdParam.isEmpty()) {
            routingContext.response()
                    .setStatusCode(400)
                    .putHeader("content-type", "application/json")
                    .end(new JsonObject().put("error", "Product ID is missing in the request").encode());
            return;
        }

        UUID productId;
        try {
            productId = UUID.fromString(productIdParam);
        } catch (IllegalArgumentException e) {
            routingContext.response()
                    .setStatusCode(400)
                    .putHeader("content-type", "application/json")
                    .end(new JsonObject().put("error", "Invalid product ID").encode());
            return;
        }

        cassandraClient.prepare("DELETE FROM products WHERE product_id = ?")
                .onSuccess(preparedStatement -> {
                    BoundStatement boundStatement = preparedStatement.bind(productId);
                    cassandraClient.execute(boundStatement)
                            .onSuccess(resultSet -> {
                                routingContext.response()
                                        .setStatusCode(200)
                                        .putHeader("content-type", "application/json")
                                        .end(new JsonObject().put("message", "Product removed successfully").encode());
                            })
                            .onFailure(throwable -> {
                                throwable.printStackTrace();
                                routingContext.response()
                                        .setStatusCode(500)
                                        .putHeader("content-type", "application/json")
                                        .end(new JsonObject().put("error", "Failed to remove product").encode());
                            });
                })
                .onFailure(throwable -> {
                    throwable.printStackTrace();
                    routingContext.response()
                            .setStatusCode(500)
                            .putHeader("content-type", "application/json")
                            .end(new JsonObject().put("error", "Failed to prepare statement").encode());
                });

    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        if (cassandraClient != null) {
            cassandraClient.close();
        }
        stopPromise.complete();
    }
}