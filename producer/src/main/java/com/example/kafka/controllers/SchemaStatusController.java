package com.example.kafka.controllers;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import com.example.kafka.Order;

import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@RestController
@RequestMapping("/schema-status")
public class SchemaStatusController {

    private static final Logger logger = LoggerFactory.getLogger(SchemaStatusController.class);

    private final SchemaRegistryClient schemaRegistryClient;
    private final String subject;

    public SchemaStatusController(@Value("${schema.registry.url}") String schemaRegistryUrl) {
        this.schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 1000);
        this.subject = "store-orders"; // El subject en Schema Registry
    }

    @GetMapping
    public ResponseEntity<String> checkSchemaVersion() {
        try {
            // Verificar si el subject existe en el Schema Registry
            Collection<String> allSubjectsCollection = schemaRegistryClient.getAllSubjects();
            List<String> allSubjects = new ArrayList<>(allSubjectsCollection); // Convertir Collection a List

            if (!allSubjects.contains(subject)) {
                logger.warn("Subject '{}' not found in the Schema Registry.", subject);
                return ResponseEntity.status(HttpStatus.NOT_FOUND)
                        .body("❌ Subject '" + subject + "' not found in the Schema Registry.");
            }

            // Obtener el esquema más reciente como JSON desde el Schema Registry
            String latestSchemaJson = schemaRegistryClient.getLatestSchemaMetadata(subject).getSchema();

            // Parsear ambos esquemas como objetos Avro para compararlos
            Schema latestAvroSchema = new Schema.Parser().parse(latestSchemaJson);
            Schema currentAvroSchema = Order.getClassSchema();

            // Comparar los esquemas normalizados
            if (latestAvroSchema.equals(currentAvroSchema)) {
                logger.info("Schemas match: latest schema is up-to-date.");
                return ResponseEntity.ok("✅ Schema is up-to-date.");
            } else {
                logger.warn("Schemas do not match: latest schema and current schema differ.");
                return ResponseEntity.status(HttpStatus.EXPECTATION_FAILED)
                        .body("❌ Outdated schema.\nLatest Schema:\n" + latestAvroSchema.toString(true) + "\nCurrent Schema:\n" + currentAvroSchema.toString(true));
            }
        } catch (Exception e) {
            logger.error("Error checking schema version", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error checking schema: " + e.getMessage());
        }
    }
}