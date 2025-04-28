package concurrent.csv.queue.validation;

import java.util.List;

public class ValidationResult {
    private final List<String> errors;

    public ValidationResult(List<String> errors) {
        this.errors = errors;
    }

    public boolean isValid() {
        return errors.isEmpty();
    }

    public List<String> getErrors() {
        return errors;
    }
}