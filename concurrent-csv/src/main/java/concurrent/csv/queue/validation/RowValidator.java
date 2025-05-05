package concurrent.csv.queue.validation;

import concurrent.csv.queue.ChunkedFileProcessor;
import concurrent.csv.queue.validation.schema.OpenApiSpec;
import concurrent.csv.queue.validation.schema.Property;

import java.math.BigDecimal;
import java.nio.CharBuffer;
import java.text.ParsePosition;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class RowValidator {

    private static final Pattern FLEXIBLE_ISO_DATETIME_PATTERN = Pattern.compile(
            "^\\d{4}-\\d{2}-\\d{2}T" +                   // Date: YYYY-MM-DDT
                    "\\d{2}:\\d{2}:\\d{2}" +                     // Time: HH:mm:ss
                    "(\\.\\d{1,9})?" +                           // Optional .fractional seconds
                    "(Z|[+-]\\d{2}:\\d{2})?$"                    // Optional zone offset
    );

    private static final Map<String, Pattern> PRECOMPILED_PATTERNS = new HashMap<>();


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
                System.out.println("Invalid value for field at index " + i + ": " + value);
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
            if (property.getPattern() != null) {
                Pattern pattern = PRECOMPILED_PATTERNS.computeIfAbsent(property.getPattern(), Pattern::compile);
                if (!pattern.matcher(value).matches()) {
                    return false;
                }
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
        if (property.getFormat() != null && property.getFormat().equals("date-time")) {


            return FLEXIBLE_ISO_DATETIME_PATTERN.matcher(value).matches();

//             try {
////                DateTimeFormatter.ISO_DATE_TIME.parse(value);
//
//
//                DateTimeFormatter.ISO_DATE_TIME.parseUnresolved(value, new ParsePosition(0));
//
//                ISO_LOCAL_DATETIME_PATTERN
//
//
//            } catch (DateTimeParseException e) {
//                return false;
//            }
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


