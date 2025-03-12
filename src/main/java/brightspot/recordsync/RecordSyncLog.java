package brightspot.recordsync;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import com.psddev.dari.db.Record;

public class RecordSyncLog extends Record {

    public static final int MAX_ENTRIES = 100;

    @Indexed(unique = true)
    private String name;

    private List<RecordSyncLogEntry> entries;

    @Override
    protected void beforeSave() {
        super.beforeSave();

        // Limit the number of entries; keep the newest MAX_ENTRIES entries
        sortEntries();
        List<RecordSyncLogEntry> entries = getEntries();
        if (entries.size() > MAX_ENTRIES) {
            setEntries(new ArrayList<>(entries.subList(0, MAX_ENTRIES)));
        }
    }

    private void sortEntries() {
        List<RecordSyncLogEntry> entries = getEntries();
        // Sort entries by timestamp
        entries.sort(Comparator.comparingLong(RecordSyncLogEntry::getTimestamp));
        // newest first
        entries.sort(Collections.reverseOrder());
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<RecordSyncLogEntry> getEntries() {
        if (entries == null) {
            entries = new ArrayList<>();
        }
        return entries;
    }

    public void setEntries(List<RecordSyncLogEntry> entries) {
        this.entries = entries;
    }

    public Optional<RecordSyncLogEntry> getLatestEntry() {
        return getEntries()
            .stream()
            .max(Comparator.comparingLong(RecordSyncLogEntry::getTimestamp));
    }
}
