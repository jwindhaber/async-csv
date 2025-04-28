package concurrent.csv.queue.validation;

import concurrent.csv.queue.validation.schema.OpenApiSpec;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.IOException;
import java.io.InputStream;

public class CsvSchemaLoader {

    public static OpenApiSpec loadSchema(String yamlFilePath) {
        LoaderOptions options = new LoaderOptions();

        Yaml yaml = new Yaml(new Constructor(OpenApiSpec.class, options));
        try (InputStream input = CsvSchemaLoader.class.getClassLoader().getResourceAsStream("schema.yaml")) {
            OpenApiSpec spec = yaml.load(input);
            // Now your OpenApiSpec is populated using record classes
            return spec;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public static void main(String[] args) throws IOException {
        OpenApiSpec openApiSpec = loadSchema("schema.yaml");

        System.out.println("OpenAPI Version: " + openApiSpec.getOpenapi());

    }
}