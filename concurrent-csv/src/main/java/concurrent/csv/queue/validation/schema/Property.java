package concurrent.csv.queue.validation.schema;

import java.util.HashSet;
import java.util.Set;

public class Property {
    private String type;
    private String format;
    private Integer index;
    private Integer maxLength;
    private String pattern;
    private Set<String> _enum = new HashSet<>();

    // Getters and setters
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public Integer getIndex() {
        return index;
    }

    public void setIndex(Integer index) {
        this.index = index;
    }

    public Integer getMaxLength() {
        return maxLength;
    }

    public void setMaxLength(Integer maxLength) {
        this.maxLength = maxLength;
    }

    public String getPattern() {
        return pattern;
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
    }

    public Set<String>  getEnum() {
        return _enum;
    }

    public void setEnum(Set<String>  _enum) {
        this._enum = _enum;
    }
}