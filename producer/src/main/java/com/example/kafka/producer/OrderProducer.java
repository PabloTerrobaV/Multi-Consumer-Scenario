package com.example.kafka.producer;

import com.example.kafka.Address;
import com.example.kafka.Item;
import com.example.kafka.Order;
import com.example.kafka.UserInfo;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import org.springframework.context.annotation.ComponentScan;

import com.example.kafka.Order; // Clases generadas por Avro
// import com.example.kafka.PaymentMethod.PaymentMethod;
// import com.example.kafka.OrderStatus.OrderStatus;

import org.apache.avro.Schema;
import org.apache.avro.JsonProperties;

import java.util.Properties;
import java.util.Scanner;
import java.util.Map;
import java.util.HashMap;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Aplicación Spring Boot que actúa como productor Kafka para enviar mensajes con esquema Avro tipo Order.
 * El usuario puede introducir los datos por consola, que son parseados e insertados en un objeto Order,
 * que luego se envía al topic Kafka correspondiente.
 */
@SpringBootApplication
@ComponentScan(basePackages = "com.example.kafka")
public class OrderProducer {

    // Configuración del topic de Kafka y las URLs de conexión
    private static final String TOPIC = "store-orders";
    private static final String BOOTSTRAP_SERVERS = "http://localhost:9092";
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";

    public static void main(String[] args) {
        SpringApplication.run(OrderProducer.class, args);

        // Configura las propiedades para el productor Kafka
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url", SCHEMA_REGISTRY_URL);
        props.put(ProducerConfig.ACKS_CONFIG, "all"); // Confirma solo cuando todos los nodos hayan replicado el mensaje

        // Inicializa el productor Kafka
        Producer<String, Order> producer = new KafkaProducer<>(props);
        Scanner scanner = new Scanner(System.in);

        // Prueba de conexión con Kafka: intenta recuperar particiones del topic
        System.out.println("⚠ Probando conexión con Kafka...");
        try {
            producer.partitionsFor(TOPIC).forEach(p ->
                    System.out.println("✅ Nodo disponible: " + p.leader().host()));
        } catch (Exception e) {
            System.err.println("❌ NO SE PUDO CONECTAR A KAFKA: " + e.getMessage());
            System.exit(1);
        }

        // Bucle principal que solicita datos de órdenes al usuario
        while (true) {
            System.out.print("\n¿Crear nueva orden? (Enter/Sí | 'exit'): ");
            String command = scanner.nextLine();
            if ("exit".equalsIgnoreCase(command)) break;

            Map<String, Object> fieldValues = new HashMap<>();
            for (Schema.Field field : Order.getClassSchema().getFields()) {
                Schema fieldSchema = getNonNullSchema(field.schema());

                Object value;
                if (fieldSchema.getType() == Schema.Type.RECORD) {
                    // Subregistro como UserInfo o Address
                    value = promptForRecord(scanner, fieldSchema, field.name());
                    Map<String, Object> subfields = (Map<String, Object>) value;
                    for (Map.Entry<String, Object> entry : subfields.entrySet()) {
                        fieldValues.put(entry.getKey(), entry.getValue());
                    }
                } else if (fieldSchema.getType() == Schema.Type.ARRAY) {
                    // Lista de elementos (Item[])
                    value = promptForArray(scanner, fieldSchema, field.name());
                    fieldValues.put(field.name(), value);
                } else {
                    // Campo simple (int, float, string, etc.)
                    value = promptForField(scanner, field, fieldSchema, field.name());
                    fieldValues.put(field.name(), value);
                }
            }

            // Construye el objeto Order desde el mapa
            Order order = buildOrder(fieldValues);

            // Crea el mensaje y lo envía a Kafka
            ProducerRecord<String, Order> record = new ProducerRecord<>(TOPIC, (String) fieldValues.get("id"), order);
            try {
                RecordMetadata metadata = producer.send(record).get(); // bloquea hasta recibir ACK
                System.out.printf("%n✅ Orden enviada!%nPartición: %d | Offset: %d%n%s%n",
                        metadata.partition(),
                        metadata.offset(),
                        "=".repeat(50));
            } catch (Exception e) {
                System.err.printf("%n❌ Error enviando orden:%n%s%n", e.getMessage());
                if (e.getCause() != null) {
                    System.err.println("Causa raíz: " + e.getCause().getMessage());
                }
            }
        }

        // Cierra recursos
        scanner.close();
        producer.close();
        System.out.println("\n🚪 Producer cerrado");
    }

    // Construye una instancia de Order a partir del mapa de valores
    private static Order buildOrder(Map<String, Object> fieldValues) {
        UserInfo user = UserInfo.newBuilder()
                .setUserId((String) fieldValues.get("user.userId"))
                .setName((String) fieldValues.get("user.name"))
                .setEmail((String) fieldValues.get("user.email"))
                .build();

        Address address = Address.newBuilder()
                .setStreet((String) fieldValues.get("shippingAddress.street"))
                .setCity((String) fieldValues.get("shippingAddress.city"))
                .setZipCode((String) fieldValues.get("shippingAddress.zipCode"))
                .setCountry((String) fieldValues.get("shippingAddress.country"))
                .build();

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

        float totalPrice = (Float) fieldValues.get("totalPrice");

        return Order.newBuilder()
                .setOrderId((Integer) fieldValues.get("orderId"))
                .setUser(user)
                .setShippingAddress(address)
                .setItems(items)
                .setTotalPrice(totalPrice)
                .build();
    }

    // Solicita valores para un subregistro (por ejemplo: UserInfo o Address)
    private static Map<String, Object> promptForRecord(Scanner scanner, Schema recordSchema, String parentFieldName) {
        Map<String, Object> recordMap = new HashMap<>();
        System.out.println("📦 Introduce los campos para: " + parentFieldName);
        for (Schema.Field subfield : recordSchema.getFields()) {
            String fullName = parentFieldName + "." + subfield.name();
            Object value = promptForField(scanner, subfield, getNonNullSchema(subfield.schema()), fullName);
            recordMap.put(fullName, value);
        }
        return recordMap;
    }

    // Solicita valores para una lista de elementos Avro
    private static List<Map<String, Object>> promptForArray(Scanner scanner, Schema arraySchema, String fieldName) {
        List<Map<String, Object>> itemList = new ArrayList<>();
        Schema itemSchema = getNonNullSchema(arraySchema.getElementType());

        System.out.printf("🧾 Introduce elementos para la lista '%s' (enter vacío para terminar):%n", fieldName);
        while (true) {
            System.out.printf("%n➡ ¿Nuevo elemento en '%s'? (Enter para continuar, 'no' para terminar): ", fieldName);
            String input = scanner.nextLine().trim();
            if (input.equalsIgnoreCase("no")) break;

            Map<String, Object> itemMap = new HashMap<>();
            for (Schema.Field subfield : itemSchema.getFields()) {
                String fullName = subfield.name();
                Object value = promptForField(scanner, subfield, getNonNullSchema(subfield.schema()), fullName);
                itemMap.put(fullName, value);
            }
            itemList.add(itemMap);
        }
        return itemList;
    }

    // Solicita valor para un campo primitivo (String, int, float...)
    private static Object promptForField(Scanner scanner, Schema.Field field, Schema fieldSchema, String displayName) {
        while (true) {
            System.out.printf("✏ %s (%s): ", displayName, fieldSchema.getType().getName().toLowerCase());
            String input = scanner.nextLine().trim();
            try {
                switch (fieldSchema.getType()) {
                    case STRING:
                        return input;
                    case INT:
                        return Integer.parseInt(input);
                    case FLOAT:
                        return Float.parseFloat(input);
                    case DOUBLE:
                        return Double.parseDouble(input);
                    case LONG:
                        return Long.parseLong(input);
                    case BOOLEAN:
                        return Boolean.parseBoolean(input);
                    default:
                        throw new IllegalArgumentException("Tipo no soportado: " + fieldSchema.getType());
                }
            } catch (Exception e) {
                System.out.println("❌ Entrada inválida, intenta de nuevo.");
            }
        }
    }


    // Método para convertir la entrada del usuario al tipo de dato correcto según el esquema
    private static Object convertInput(String input, Schema schema) throws Exception {
        // Si el schema es de tipo UNION, obtiene el schema no nulo
        Schema targetSchema = schema.getType() == Schema.Type.UNION ? getNonNullSchema(schema) : schema;

        switch (targetSchema.getType()) {
            case STRING: return input;
            case INT: return Integer.parseInt(input);
            case FLOAT: return Float.parseFloat(input);
            case BOOLEAN: return parseBoolean(input);
            case ENUM: return validateEnum(input, targetSchema);
            case UNION: return handleUnionType(input, schema);
            default: throw new IllegalArgumentException("Tipo no soportado: " + targetSchema.getType());
        }
    }

    // Método para validar y convertir valores de tipo ENUM
    private static Enum<?> validateEnum(String input, Schema enumSchema) throws Exception {
        List<String> validValues = enumSchema.getEnumSymbols();
        String upperInput = input.toUpperCase();

        // Verifica si el valor ingresado es válido para el ENUM
        if (!validValues.contains(upperInput)) {
            throw new IllegalArgumentException("Valores permitidos: " + validValues);
        }

        // Convierte el string a la instancia ENUM correspondiente
        return Enum.valueOf((Class<? extends Enum>) getEnumClass(enumSchema), upperInput);
    }

    // Método para manejar valores por defecto
    private static Object handleDefault(Schema.Field field) {
        // Si el valor por defecto es null, retorna null explícitamente
        if (field.defaultVal() instanceof JsonProperties.Null) {
            return null;
        }
        // Retorna el valor por defecto definido en el esquema
        return field.defaultVal();
    }

    // Método para manejar tipos UNION (generalmente para campos opcionales)
    private static Object handleUnionType(String input, Schema unionSchema) throws Exception {
        // Filtra los tipos no nulos del schema UNION
        List<Schema> nonNullTypes = unionSchema.getTypes().stream()
                .filter(s -> s.getType() != Schema.Type.NULL)
                .collect(Collectors.toList());

        // Verifica que solo haya un tipo no nulo (no soporta uniones complejas)
        if (nonNullTypes.size() != 1) {
            throw new IllegalArgumentException("Unión compleja no soportada");
        }

        // Convierte el input al tipo no nulo encontrado
        return convertInput(input, nonNullTypes.get(0));
    }

    // Método para obtener una representación legible de los valores por defecto
    private static String getHumanReadableDefault(Schema.Field field) {
        if (field.defaultVal() == null) {
            return isNullable(field.schema()) ? " [opcional]" : " [requerido]";
        }

        if (field.defaultVal() instanceof JsonProperties.Null) {
            return " [default: null]";
        }

        return " [default: " + field.defaultVal() + "]";
    }

    // Método para determinar si un campo es nullable (puede ser null)
    private static boolean isNullable(Schema schema) {
        return schema.getType() == Schema.Type.UNION &&
                schema.getTypes().stream().anyMatch(s -> s.getType() == Schema.Type.NULL);
    }

    // Método para obtener el esquema no nulo de un tipo UNION
    private static Schema getNonNullSchema(Schema schema) {
        if (schema.getType() == Schema.Type.UNION) {
            // Busca el primer tipo no nulo en la unión
            return schema.getTypes().stream()
                    .filter(s -> s.getType() != Schema.Type.NULL)
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Unión sin tipos válidos"));
        }
        return schema;
    }

    // Método para parsear valores booleanos de manera más flexible
    private static Boolean parseBoolean(String input) {
        if ("sí".equalsIgnoreCase(input) || "si".equalsIgnoreCase(input)) return true;
        if ("no".equalsIgnoreCase(input)) return false;
        return Boolean.parseBoolean(input);
    }

    // Método para obtener la clase Enum a partir del esquema
    @SuppressWarnings("unchecked")
    private static Class<? extends Enum<?>> getEnumClass(Schema schema) {
        try {
            // Intenta cargar la clase Enum basada en el nombre completo del schema
            return (Class<? extends Enum<?>>) Class.forName(schema.getFullName());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Clase Enum no encontrada: " + schema.getFullName(), e);
        }
    }
}