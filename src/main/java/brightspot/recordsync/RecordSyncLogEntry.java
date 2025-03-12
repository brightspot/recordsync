package brightspot.recordsync;

import java.util.List;

import com.psddev.dari.db.Record;
import com.psddev.dari.db.Recordable;

@Recordable.Embedded
public class RecordSyncLogEntry extends Record {

    private long timestamp;

    private long executionMillis;

    private long recordChangeCount;

    private List<String> errors;

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void setExecutionMillis(long executionMillis) {
        this.executionMillis = executionMillis;
    }

    public long getExecutionMillis() {
        return executionMillis;
    }

    public void setRecordChangeCount(long recordChangeCount) {
        this.recordChangeCount = recordChangeCount;
    }

    public long getRecordChangeCount() {
        return recordChangeCount;
    }

    public void setErrors(List<String> errors) {
        this.errors = errors;
    }

    public List<String> getErrors() {
        return errors;
    }
}
