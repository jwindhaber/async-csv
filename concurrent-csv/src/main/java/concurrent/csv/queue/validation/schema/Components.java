package concurrent.csv.queue.validation.schema;

import java.util.Map;

public class Components {
    private Map<String, Schema> schemas;

    // Getters and setters
    public Map<String, Schema> getSchemas() {
        return schemas;
    }

    public void setSchemas(Map<String, Schema> schemas) {
        this.schemas = schemas;
    }
}