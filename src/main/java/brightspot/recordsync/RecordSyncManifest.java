package brightspot.recordsync;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.psddev.dari.util.StorageItem;

/**
 * JSON Mapped object for /manifest.json.
 * This file describes the contents of the recordsync directory on S3, including all data files and the last exported record ID and update date.
 */
public class RecordSyncManifest {

    private static final String MANIFEST_JSON_PATH = "/manifest.json";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @JsonProperty("lastUpdateDate")
    private Long lastUpdateDate;

    @JsonProperty("lastId")
    private UUID lastId;

    @JsonProperty("dataFiles")
    private Map<Long, List<String>> dataFiles;

    @JsonProperty("recordCounts")
    private Map<Long, Long> recordCounts;

    public Long getLastUpdateDate() {
        return lastUpdateDate;
    }

    public void setLastUpdateDate(Long lastUpdateDate) {
        this.lastUpdateDate = lastUpdateDate;
    }

    public UUID getLastId() {
        return lastId;
    }

    public void setLastId(UUID lastId) {
        this.lastId = lastId;
    }

    public Map<Long, List<String>> getDataFiles() {
        if (dataFiles == null) {
            dataFiles = new LinkedHashMap<>();
        }
        return dataFiles;
    }

    public void setDataFiles(Map<Long, List<String>> dataFiles) {
        this.dataFiles = dataFiles;
    }

    public Map<Long, Long> getRecordCounts() {
        if (recordCounts == null) {
            recordCounts = new LinkedHashMap<>();
        }
        return recordCounts;
    }

    public void setRecordCounts(Map<Long, Long> recordCounts) {
        this.recordCounts = recordCounts;
    }

    @Override
    public String toString() {
        return "RecordSyncManifest{"
            + "lastUpdateDate=" + lastUpdateDate
            + ", lastId=" + lastId
            + ", dataFiles=" + dataFiles
            + ", recordCounts=" + recordCounts
            + '}';
    }

    /**
     * Save the manifest to the given storage in the given pathPrefix with the filename {@link #MANIFEST_JSON_PATH}.
     * @param storage The storage to save the manifest to.
     * @param pathPrefix The path prefix to save the manifest to.
     */
    public void save(String storage, String pathPrefix) {
        StorageItem storageItem = StorageItem.Static.createIn(storage);
        storageItem.setPath(pathPrefix + MANIFEST_JSON_PATH);
        try {
            ByteArrayInputStream manifestData = new ByteArrayInputStream(OBJECT_MAPPER.writeValueAsBytes(this));
            storageItem.setData(manifestData);
            storageItem.save();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Load the manifest from the given storage in the given pathPrefix with the filename {@link #MANIFEST_JSON_PATH}.
     * @param storage The storage to load the manifest from.
     * @param pathPrefix The path prefix to load the manifest from.
     * @return The loaded manifest.
     */
    public static RecordSyncManifest load(String storage, String pathPrefix) {
        StorageItem storageItem = StorageItem.Static.createIn(storage);
        storageItem.setPath(pathPrefix + MANIFEST_JSON_PATH);

        if (storageItem.isInStorage()) {
            try {
                RecordSyncManifest manifest = OBJECT_MAPPER.readValue(storageItem.getData(), RecordSyncManifest.class);
                return manifest;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            return new RecordSyncManifest();
        }
    }
}
