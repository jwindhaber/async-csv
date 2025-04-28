package concurrent.csv.queue.validation;

import concurrent.csv.queue.ChunkedFileProcessor;
import concurrent.csv.queue.validation.schema.OpenApiSpec;
import concurrent.csv.queue.validation.schema.Property;

import java.math.BigDecimal;
import java.nio.CharBuffer;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RowValidator {
    private final Map<Integer, Property> propertyByIndex;

    // Assuming the OpenApiSpec object is passed in for schema validation
    private final OpenApiSpec openApiSpec;

    public RowValidator(OpenApiSpec openApiSpec) {
        this.openApiSpec = openApiSpec;

        this.propertyByIndex = new HashMap<>();

        for (Map.Entry<String, Property> entry : openApiSpec.getComponents().getSchemas().get("Transaction").getProperties().entrySet()) {
            Property property = entry.getValue();
            propertyByIndex.put(property.getIndex(), property);
        }

    }

    public ValidationResult validate(ChunkedFileProcessor.Row row, CharBuffer charBuffer) {


        List<String> errors = new ArrayList<>();

        // Iterate over fields and validate based on index
        for (int i = 0; i < row.getFields().length; i++) {
            ChunkedFileProcessor.Field field = row.getFields()[i];

            // Extract the CharSequence (no String allocation here)
            CharSequence value = extractFieldValue(row, field, charBuffer);

            // Validate the value based on schema properties
            Property property = propertyByIndex.get(i);

            // Perform validation on each property
            if (!validateField(value, property)) {
                errors.add("Invalid value for field at index " + i + ": " + value);
            }
        }

        return new ValidationResult(errors);
    }

    private boolean validateField(CharSequence value, Property property) {
        // Validate based on the type
        if (property.getType().equals("string")) {
            if (property.getMaxLength() != null && value.length() > property.getMaxLength()) {
                return false;
            }
            if (property.getPattern() != null && !value.toString().matches(property.getPattern())) {
                return false;
            }
        } else if (property.getType().equals("boolean")) {
            if (!"true".equalsIgnoreCase(value.toString()) && !"false".equalsIgnoreCase(value.toString())) {
                return false;
            }
        } else if (property.getType().equals("number") && property.getFormat().equals("decimal")) {
            try {
                new BigDecimal(value.toString());  // Decimal parsing
            } catch (NumberFormatException e) {
                return false;
            }
        }
//        else if (property.getEnum() != null && !property.getEnum().contains(value.toString())) {
//            return false;
//        }
        else if (property.getFormat() != null && property.getFormat().equals("date-time")) {
            try {
                DateTimeFormatter.ISO_DATE_TIME.parse(value);
            } catch (DateTimeParseException e) {
                return false;
            }
        }

        return true;
    }

    // This method extracts a CharSequence directly from CharBuffer without creating a String
    private CharSequence extractFieldValue(ChunkedFileProcessor.Row row, ChunkedFileProcessor.Field field, CharBuffer buffer) {
        int start = field.getStart();
        int end = field.getEnd();
        return buffer.subSequence(start, end);
    }
}


