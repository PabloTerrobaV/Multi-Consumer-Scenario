package com.example.kafka.producer;

import com.example.kafka.Address;
import com.example.kafka.Item;
import com.example.kafka.Order;
import com.example.kafka.UserInfo;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.avro.Schema;
import org.apache.avro.JsonProperties;
import org.apache.avro.generic.GenericData;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.*;

/**
 * Aplicación Spring Boot que actúa como productor Kafka.
 * Permite al usuario introducir datos por consola que se mapean a un objeto Avro del tipo `Order`
 * (con subcampos y listas), y luego se envían al topic de Kafka correspondiente.
 */
@SpringBootApplication
public class OrderProducer {

    // Nombre del topic de Kafka al que se enviarán los mensajes
    private static final String TOPIC = "store-orders";
    private static final String BOOTSTRAP_SERVERS = "http://localhost:9092";
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";

    public static void main(String[] args) {
        SpringApplication.run(OrderProducer.class, args);

        // Configuración del productor Kafka
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url", SCHEMA_REGISTRY_URL);
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        Producer<String, Order> producer = new KafkaProducer<>(props);
        Scanner scanner = new Scanner(System.in);

        // Probar la conexión con Kafka antes de comenzar
        System.out.println("⚠ Probando conexión con Kafka...");
        try {
            producer.partitionsFor(TOPIC).forEach(p ->
                    System.out.println("✅ Nodo disponible: " + p.leader().host()));
        } catch (Exception e) {
            System.err.println("❌ NO SE PUDO CONECTAR A KAFKA: " + e.getMessage());
            System.exit(1);
        }

        // Bucle principal para generar múltiples órdenes
        while (true) {
            System.out.print("\n¿Crear nueva orden? (Enter/Sí | 'exit'): ");
            String command = scanner.nextLine();
            if ("exit".equalsIgnoreCase(command)) break;

            // Recoger valores para todos los campos del esquema
            Map<String, Object> fieldValues = new HashMap<>();
            for (Schema.Field field : Order.getClassSchema().getFields()) {
                Schema fieldSchema = getNonNullSchema(field.schema());

                Object value;
                if (fieldSchema.getType() == Schema.Type.RECORD) {
                    // Si el campo es un subregistro (user, address...)
                    value = promptForRecord(scanner, fieldSchema, field.name());
                    fieldValues.putAll((Map<String, Object>) value);
                } else if (fieldSchema.getType() == Schema.Type.ARRAY) {
                    // Si el campo es un array de elementos (items)
                    value = promptForArray(scanner, fieldSchema, field.name());
                    fieldValues.put(field.name(), value);
                } else {
                    // Campo primitivo o enum
                    value = promptForField(scanner, field, fieldSchema, field.name());
                    fieldValues.put(field.name(), value);
                }
            }

            // Construir objeto Order con los valores introducidos
            Order order = buildOrder(fieldValues);

            // Enviar el mensaje a Kafka
            ProducerRecord<String, Order> record = new ProducerRecord<>(
                    TOPIC, fieldValues.get("orderId").toString(), order);

            try {
                RecordMetadata metadata = producer.send(record).get();
                System.out.printf("%n✅ Orden enviada!%nPartición: %d | Offset: %d%n%s%n",
                        metadata.partition(), metadata.offset(), "=".repeat(50));
            } catch (Exception e) {
                System.err.printf("%n❌ Error enviando orden: %s%n", e.getMessage());
            }
        }

        // Cierre de recursos
        scanner.close();
        producer.close();
        System.out.println("\n🚪 Producer cerrado");
    }

    // Si el campo es una unión, obtiene el tipo no-nulo (evita problemas con optional fields)
    private static Schema getNonNullSchema(Schema schema) {
        if (schema.getType() == Schema.Type.UNION) {
            return schema.getTypes().stream()
                    .filter(s -> s.getType() != Schema.Type.NULL)
                    .findFirst().orElse(schema);
        }
        return schema;
    }

    // Determina si un campo Avro es opcional (contiene null en la unión)
    private static boolean isNullable(Schema schema) {
        return schema.getType() == Schema.Type.UNION &&
                schema.getTypes().stream().anyMatch(s -> s.getType() == Schema.Type.NULL);
    }

    // Pide al usuario un valor para un campo primitivo o enum
    private static Object promptForField(Scanner scanner, Schema.Field field, Schema fieldSchema, String displayName) {
        boolean isNullable = isNullable(field.schema());
        Object defaultVal = field.defaultVal();
        String defaultHint = (defaultVal != null && !(defaultVal instanceof JsonProperties.Null))
                ? " (por defecto: " + defaultVal + ")"
                : (isNullable ? " (opcional)" : "");

        while (true) {
            System.out.printf("✏ %s (%s)%s: ", displayName, fieldSchema.getType().getName().toLowerCase(), defaultHint);
            String input = scanner.nextLine().trim();

            try {
                if (input.isEmpty()) {
                    // Si el campo está vacío y tiene valor por defecto o es nulo
                    if (!(defaultVal instanceof JsonProperties.Null)) {
                        return convertDefault(defaultVal, fieldSchema);
                    } else if (isNullable) {
                        return null;
                    } else {
                        System.out.println("❌ Campo obligatorio, sin valor por defecto.");
                        continue;
                    }
                }
                return convertInput(input, fieldSchema);
            } catch (Exception e) {
                System.out.println("❌ Entrada inválida: " + e.getMessage());
            }
        }
    }

    // Convierte el texto introducido en consola al tipo correspondiente
    private static Object convertInput(String input, Schema schema) {
        return switch (schema.getType()) {
            case STRING -> input;
            case INT -> Integer.parseInt(input);
            case LONG -> Long.parseLong(input);
            case FLOAT -> Float.parseFloat(input);
            case DOUBLE -> Double.parseDouble(input);
            case BOOLEAN -> parseBoolean(input);
            case ENUM -> {
                if (!schema.getEnumSymbols().contains(input)) {
                    throw new IllegalArgumentException("Valores válidos: " + schema.getEnumSymbols());
                }
                yield new GenericData.EnumSymbol(schema, input);
            }
            case UNION -> convertInput(input, getNonNullSchema(schema));
            default -> throw new IllegalArgumentException("Tipo no soportado: " + schema.getType());
        };
    }

    // Admite múltiples formas de introducir booleanos
    private static boolean parseBoolean(String input) {
        return switch (input.toLowerCase()) {
            case "true", "yes", "1", "y", "sí", "si" -> true;
            case "false", "no", "0", "n" -> false;
            default -> throw new IllegalArgumentException("Valor booleano inválido");
        };
    }

    // Convierte un valor por defecto del esquema al tipo Java apropiado
    private static Object convertDefault(Object defaultVal, Schema schema) {
        return switch (schema.getType()) {
            case STRING -> defaultVal.toString();
            case INT -> ((Number) defaultVal).intValue();
            case FLOAT -> ((Number) defaultVal).floatValue();
            case DOUBLE -> ((Number) defaultVal).doubleValue();
            case LONG -> ((Number) defaultVal).longValue();
            case BOOLEAN -> Boolean.parseBoolean(defaultVal.toString());
            default -> defaultVal;
        };
    }

    // Solicita los subcampos de un record anidado como 'user' o 'shippingAddress'
    private static Map<String, Object> promptForRecord(Scanner scanner, Schema schema, String parentName) {
        Map<String, Object> values = new HashMap<>();
        for (Schema.Field field : schema.getFields()) {
            Schema subSchema = getNonNullSchema(field.schema());
            String fieldName = parentName + "." + field.name();
            Object value = promptForField(scanner, field, subSchema, fieldName);
            values.put(fieldName, value);
        }
        return values;
    }

    // Solicita la entrada de múltiples elementos de un array, como una lista de ítems
    private static List<Map<String, Object>> promptForArray(Scanner scanner, Schema schema, String arrayName) {
        List<Map<String, Object>> items = new ArrayList<>();
        Schema itemSchema = getNonNullSchema(schema.getElementType());

        System.out.printf("🔁 Introduciendo elementos para el array '%s' (escribe 'done' para terminar):%n", arrayName);
        while (true) {
            System.out.print("➕ ¿Añadir nuevo elemento? (Enter para sí, 'done' para terminar): ");
            String input = scanner.nextLine().trim();
            if ("done".equalsIgnoreCase(input)) break;

            Map<String, Object> itemFields = new HashMap<>();
            for (Schema.Field field : itemSchema.getFields()) {
                Schema subSchema = getNonNullSchema(field.schema());
                Object value = promptForField(scanner, field, subSchema, field.name());
                itemFields.put(field.name(), value);
            }
            items.add(itemFields);
        }

        return items;
    }

    // Construye un objeto Order a partir de los valores recogidos del usuario
    private static Order buildOrder(Map<String, Object> fieldValues) {
        // Crear objeto UserInfo
        UserInfo user = UserInfo.newBuilder()
                .setUserId((String) fieldValues.get("user.userId"))
                .setName((String) fieldValues.get("user.name"))
                .setEmail((String) fieldValues.get("user.email"))
                // .setPhone((Long) fieldValues.get("user.phone"))
                .build();

        // Crear objeto Address
        Address address = Address.newBuilder()
                .setStreet((String) fieldValues.get("shippingAddress.street"))
                .setCity((String) fieldValues.get("shippingAddress.city"))
                .setZipCode((String) fieldValues.get("shippingAddress.zipCode"))
                //.setCountry((String) fieldValues.get("shippingAddress.country"))
                .build();

        // Crear lista de Items
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> rawItems = (List<Map<String, Object>>) fieldValues.get("items");
        List<Item> items = new ArrayList<>();
        for (Map<String, Object> rawItem : rawItems) {
            Item item = Item.newBuilder()
                    .setProductId((Integer) rawItem.get("productId"))
                    .setProductName((String) rawItem.get("productName"))
                    .setQuantity((Integer) rawItem.get("quantity"))
                    .setPrice((Float) rawItem.get("price"))
                    .build();
            items.add(item);
        }

        // Construir y devolver la orden completa
        return Order.newBuilder()
                .setOrderId((Integer) fieldValues.get("orderId"))
                .setUser(user)
                .setShippingAddress(address)
                .setItems(items)
                .setTotalPrice((Float) fieldValues.get("totalPrice"))
                .build();
    }
}