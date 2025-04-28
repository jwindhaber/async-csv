package concurrent.csv.queue.validation.schema;

import java.util.Map;

public class Schema {
    private String type;
    private Map<String, Property> properties;

    // Getters and setters
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Map<String, Property> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Property> properties) {
        this.properties = properties;
    }
}