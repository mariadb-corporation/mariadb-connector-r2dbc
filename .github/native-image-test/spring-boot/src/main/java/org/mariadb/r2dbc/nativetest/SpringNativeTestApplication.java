package org.mariadb.r2dbc.nativetest;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.r2dbc.core.DatabaseClient;

@SpringBootApplication
public class SpringNativeTestApplication {

  public static void main(String[] args) {
    SpringApplication.run(SpringNativeTestApplication.class, args);
  }

  @Bean
  CommandLineRunner runner(DatabaseClient client) {
    return args -> {
      client.sql("DROP TABLE IF EXISTS native_spring_test").fetch().rowsUpdated().block();
      client.sql("CREATE TABLE native_spring_test (id INT PRIMARY KEY, val VARCHAR(100))")
          .fetch().rowsUpdated().block();

      client.sql("INSERT INTO native_spring_test (id, val) VALUES (1, 'hello from spring native')")
          .fetch().rowsUpdated().block();

      String val = client.sql("SELECT val FROM native_spring_test WHERE id = 1")
          .map(row -> row.get("val", String.class))
          .one()
          .block();

      if (!"hello from spring native".equals(val)) {
        throw new AssertionError("Unexpected result: " + val);
      }
      System.out.println("Read back: val=" + val);

      client.sql("DROP TABLE native_spring_test").fetch().rowsUpdated().block();
      System.out.println("Spring Boot native image test PASSED");

      // Exit explicitly since CommandLineRunner keeps the context alive
      System.exit(0);
    };
  }
}
