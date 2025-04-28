package concurrent.csv.queue.validation;

import concurrent.csv.queue.ChunkedFileProcessor;
import concurrent.csv.queue.validation.schema.OpenApiSpec;
import concurrent.csv.queue.validation.schema.Property;
import concurrent.csv.queue.validation.schema.Schema;

import java.nio.CharBuffer;
import java.util.List;
import java.util.Map;

public class RowValidator1 {
    private final Map<String, Property> propertiesByName;

    public RowValidator1(OpenApiSpec spec) {
        // Assume one schema, e.g. "MyEntity"
        if (spec.getComponents().getSchemas().size() != 1) {
            throw new IllegalArgumentException("Expected exactly one schema for now.");
        }
        Schema schema = spec.getComponents().getSchemas().values().iterator().next();
        this.propertiesByName = schema.getProperties();
    }

    public void validate(ChunkedFileProcessor.ChunkResult result) throws ValidationException {
        List<ChunkedFileProcessor.Row> rows = result.rows();
        CharBuffer charBuffer = result.charBuffer();

        for (ChunkedFileProcessor.Row row : rows) {
            for (Property property : propertiesByName.values()) {
                Integer index = property.getIndex();
                if (index == null) continue; // no index defined, skip

                if (index >= row.getFields().length) {
                    throw new ValidationException("Missing field at index " + index + " for property " + property);
                }

                ChunkedFileProcessor.Field field = row.getFields()[index];
                if (field == null) {
                    throw new ValidationException("Null field at index " + index + " for property " + property);
                }

                // Optionally more validations here:
                // - Check length
                // - Check pattern
                // - Check allowed enum values
                // - etc.
            }
        }



    }

    public static class ValidationException extends Exception {
        public ValidationException(String message) {
            super(message);
        }
    }
}
