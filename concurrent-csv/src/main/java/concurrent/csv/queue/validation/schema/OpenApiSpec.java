package concurrent.csv.queue.validation.schema;

import java.util.Map;

public class OpenApiSpec {
    private String openapi;
    private Info info;
    private Map<String, Object> paths;
    private Components components;

    // Getters and setters
    public String getOpenapi() {
        return openapi;
    }

    public void setOpenapi(String openapi) {
        this.openapi = openapi;
    }

    public Info getInfo() {
        return info;
    }

    public void setInfo(Info info) {
        this.info = info;
    }

    public Map<String, Object> getPaths() {
        return paths;
    }

    public void setPaths(Map<String, Object> paths) {
        this.paths = paths;
    }

    public Components getComponents() {
        return components;
    }

    public void setComponents(Components components) {
        this.components = components;
    }
}