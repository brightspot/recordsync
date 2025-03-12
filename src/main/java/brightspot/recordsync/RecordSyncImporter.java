package brightspot.recordsync;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import com.psddev.dari.db.Database;
import com.psddev.dari.db.DatabaseEnvironment;
import com.psddev.dari.db.DatabaseException;
import com.psddev.dari.db.ObjectType;
import com.psddev.dari.db.Query;
import com.psddev.dari.db.Recordable;
import com.psddev.dari.db.State;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.StorageItem;
import com.psddev.dari.util.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Record Sync Importer.
 * This {@link Runnable} syncs records from a storage bucket and imports them into the Brightspot database.
 */
public class RecordSyncImporter implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(RecordSyncImporter.class);

    private final String name;
    private final String storage;
    private final String pathPrefix;
    private final long batchSize;
    private final Instant oldestDataTimestamp;
    private final Set<UUID> excludedTypeIds;
    private final Set<UUID> includedTypeIds;
    private final Task progressTask;

    // Errors encountered during the import process. This should be cleared at the start of each import.
    private final List<String> errors = new ArrayList<>();

    public RecordSyncImporter(String name, String storage, String pathPrefix, long batchSize, Instant oldestDataTimestamp, Set<UUID> excludedTypeIds, Set<UUID> includedTypeIds, Task progressTask) {
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("Name cannot be null or empty");
        }
        if (storage == null || storage.trim().isEmpty()) {
            throw new IllegalArgumentException("Storage cannot be null or empty");
        }
        if (batchSize < 1) {
            throw new IllegalArgumentException("Batch size cannot be less than 1");
        }
        if (excludedTypeIds == null) {
            excludedTypeIds = new HashSet<>();
        }

        this.name = name;
        this.storage = storage.trim();
        this.pathPrefix = Optional.ofNullable(pathPrefix)
            .map(String::trim)
            .map(s -> s.replaceAll("^/", "").replaceAll("/$", ""))
            .orElseThrow(() -> new IllegalArgumentException("Path Prefix cannot be null or empty"));
        this.batchSize = batchSize;
        this.oldestDataTimestamp = oldestDataTimestamp == null ? Instant.MIN : oldestDataTimestamp;
        this.excludedTypeIds = new HashSet<>(excludedTypeIds);
        this.includedTypeIds = new HashSet<>(includedTypeIds);
        this.progressTask = progressTask;
    }

    @Override
    public void run() {
        Instant now = Instant.now();
        LOGGER.info("Importer [{}]: Starting record import", name);
        RecordSyncManifest manifest = RecordSyncManifest.load(storage, pathPrefix);

        RecordSyncLog log = Query.from(RecordSyncLog.class).where("name = ?", name).first();
        if (log == null) {
            log = new RecordSyncLog();
            log.setName(name);
        }

        long previousTimestamp = log.getLatestEntry()
            .map(RecordSyncLogEntry::getTimestamp)
            .orElse(0L);

        List<Long> timestampsToImport = manifest.getDataFiles()
            .keySet()
            .stream()
            .filter(timestamp -> timestamp > previousTimestamp)
            .sorted()
            .collect(Collectors.toList());

        long recordCount = 0L;
        long totalRecordCount = 0L;
        for (Long timestamp : timestampsToImport) {
            totalRecordCount += manifest.getRecordCounts().get(timestamp);
        }
        progressTask.setProgressTotal(totalRecordCount);
        for (Long timestamp : timestampsToImport) {
            synchronized (this) {
                List<String> files = manifest.getDataFiles().get(timestamp);
                LOGGER.info("Importer [{}]: Importing [{}] files for timestamp [{}]: {}", name, files.size(), timestamp, files);
                long startTime = System.currentTimeMillis();

                errors.clear();
                long numberOfRecordsChanged = 0;
                int numFiles = files.size();
                int i = 0;
                for (String file : files) {
                    i++;
                    try {
                        LOGGER.info("Importer [{}]: Downloading and importing file [{}/{}]: [{}]", name, i, numFiles, file);
                        numberOfRecordsChanged += downloadAndImportFile(file);
                    } catch (IOException e) {
                        String stackTrace = Arrays.stream(e.getStackTrace()).map(StackTraceElement::toString).collect(Collectors.joining("\n"));
                        errors.add("Error importing [" + pathPrefix + '/' + file + "]: " + e.getMessage() + stackTrace);
                        LOGGER.error("Importer [{}]: Error importing file: {}", name, file, e);
                    }
                }

                recordCount += numberOfRecordsChanged;
                RecordSyncLogEntry entry = new RecordSyncLogEntry();
                entry.setTimestamp(timestamp);
                entry.setExecutionMillis(System.currentTimeMillis() - startTime);
                entry.setRecordChangeCount(numberOfRecordsChanged);
                entry.setErrors(List.copyOf(errors));
                log.getEntries().add(entry);
                log.save();
                errors.clear();
            }
        }

        LOGGER.info("Importer [{}]: Imported [{}] records in [{}] seconds", name, recordCount, Duration.between(now, Instant.now()).toSeconds());
    }

    private boolean transformAndSave(Database db, DatabaseEnvironment dbEnv, String recordJson) {
        Object recordObj = ObjectUtils.fromJson(recordJson);
        if (recordObj instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> recordMap = (Map<String, Object>) recordObj;
            // Check for deletes first
            if (recordMap.containsKey("delete") && recordMap.size() == 1) {
                Object deleteObj = recordMap.get("delete");
                if (deleteObj instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> deleteMap = (Map<String, Object>) deleteObj;
                    UUID deleteId = uuid(deleteMap.get("_id"));
                    UUID deleteTypeId = uuid(deleteMap.get("_type"));
                    if (deleteId != null && deleteTypeId != null) {
                        if ((includedTypeIds.isEmpty() || includedTypeIds.contains(deleteTypeId)) && !excludedTypeIds.contains(deleteTypeId)) {
                            db.deleteByQuery(Query.fromAll().where("_id = ? and _type = ?", deleteId, deleteTypeId));
                            return true;
                        }
                    }
                }
            } else {
                UUID typeId = uuid(recordMap.get("_type"));
                if (typeId != null && !excludedTypeIds.contains(typeId) && (includedTypeIds.isEmpty() || includedTypeIds.contains(typeId))) {
                    UUID id = uuid(recordMap.get("_id"));
                    if (id != null) {
                        ObjectType objectType = dbEnv.getTypeById(typeId);
                        Recordable record = (Recordable) objectType.createObject(id);
                        State state = record.getState();
                        state.putAll(recordMap);
                        db.saveUnsafely(state);
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private long downloadAndImportFile(String filename) throws IOException {
        long saved = 0;

        LOGGER.debug("Importer [{}]: Downloading and importing file: {}", name, filename);
        StorageItem storageItem = StorageItem.Static.createIn(storage);
        storageItem.setPath(pathPrefix + '/' + filename);
        InputStream data = storageItem.getData();
        String[] recordJsons = new String(gunzip(data), StandardCharsets.UTF_8).split("\n");
        Database db = Database.Static.getDefault();
        db.beginWrites();
        DatabaseEnvironment dbEnv = DatabaseEnvironment.getCurrent();
        try {
            int endIndex = 0;
            int startIndex = 0;
            for (String recordJson : recordJsons) {
                if (transformAndSave(db, dbEnv, recordJson)) {
                    saved++;
                    if (saved % batchSize == 0) {
                        commitSafely(db, dbEnv, filename, recordJsons, startIndex, endIndex);
                        LOGGER.debug("Saved {} records", saved);
                        startIndex = endIndex + 1;
                    }
                } else {
                    // Skipped due to included / excluded types
                    LOGGER.debug("Importer [{}]: Unable to save record: {}", name, recordJson);
                }
                progressTask.addProgressIndex(1);
                endIndex++;
            }
            commitSafely(db, dbEnv, filename, recordJsons, startIndex, endIndex);
        } finally {
            db.endWrites();
        }

        LOGGER.debug("Saved {} records", saved);
        return saved;
    }

    /**
     * Commit the current transaction. If it fails, iterate through the records to isolate the failure and commit them in smaller batches.
     * @param db The Database
     * @param dbEnv The DatabaseEnvironment
     * @param jsonStrings The complete array of JSON Strings in this batch
     * @param startIndex The start index of the subset of jsonStrings to commit
     * @param endIndex The end index of the subset of jsonStrings to commit
     */
    private void commitSafely(Database db, DatabaseEnvironment dbEnv, String filename, String[] jsonStrings, int startIndex, int endIndex) {
        LOGGER.debug("Committing {} records, from {} to {}", endIndex - startIndex + 1, startIndex, endIndex);
        // Processing the subset of jsonStrings from jsonStrings[startIndex] to jsonStrings[endIndex]
        try {
            db.commitWrites();
            LOGGER.debug("Successfully committed {} record(s), from {} to {}.", endIndex - startIndex + 1, startIndex, endIndex);
        } catch (DatabaseException e) {
            if (startIndex == endIndex) {
                String skipped = jsonStrings[startIndex];
                int length = skipped.length();
                String skippedId = "N/A";
                String skippedTypeId = "N/A";
                Object skippedObj = ObjectUtils.fromJson(skipped);
                if (skippedObj instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> skippedMap = (Map<String, Object>) skippedObj;
                    skippedId = (String) skippedMap.get("_id");
                    skippedTypeId = (String) skippedMap.get("_type");
                }
                errors.add("Error committing writes for record ID [" + skippedId + "] of type [" + skippedTypeId + "] found on line [" + startIndex + "] of file [" + filename + "] (Exception: [" + e.getMessage() + "]). Skipping.");
                LOGGER.error("Importer [{}]: Error committing writes for record ID [{}] of type [{}] found on line [{}] of file [{}] (Exception: [{}]). Skipping.\n{} [...] {}",
                    name,
                    skippedId,
                    skippedTypeId,
                    startIndex,
                    filename,
                    e.getMessage(),
                    skipped.substring(0, Math.min(100, length)),
                    skipped.substring(length - 100));
                return;
            }
            LOGGER.debug("Error committing writes. Trying again to isolate the failure.", e);
            int batchSize = Math.max(1, (endIndex - startIndex) / 2);
            int localStartIndex = startIndex;
            int localEndIndex = startIndex;
            long saved = 0;
            for (int i = startIndex; i <= endIndex; i++) {
                String jsonRecord = jsonStrings[i];
                if (transformAndSave(db, dbEnv, jsonRecord)) {
                    saved++;
                    if (saved % batchSize == 0) {
                        LOGGER.debug("Recursively committing {} records, from {} to {}", localEndIndex - localStartIndex + 1, localStartIndex, localEndIndex);
                        commitSafely(db, dbEnv, filename, jsonStrings, localStartIndex, localEndIndex);
                        try {
                            Thread.sleep(500);
                        } catch (InterruptedException ex) {
                            throw new RuntimeException(ex);
                        }
                        localStartIndex = localEndIndex + 1;
                    }
                }
                localEndIndex++;
            }
            LOGGER.debug("Again, recursively committing {} records, from {} to {}", localEndIndex - localStartIndex + 1, localStartIndex, localEndIndex);
            commitSafely(db, dbEnv, filename, jsonStrings, localStartIndex, localEndIndex);
        }
    }

    private static UUID uuid(Object uuid) {
        return uuid instanceof UUID
            ? (UUID) uuid
            : uuid instanceof String
                ? UUID.fromString((String) uuid)
                : null;
    }

    private static byte[] gunzip(InputStream byteIn) throws IOException {
        try (GZIPInputStream gzipIn = new GZIPInputStream(byteIn)) {
            ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            int len;
            while ((len = gzipIn.read(buffer)) != -1) {
                byteOut.write(buffer, 0, len);
            }
            return byteOut.toByteArray();
        }
    }

    @Override
    public String toString() {
        return "RecordSyncImporter{"
            + "name='" + name + '\''
            + ", storage='" + storage + '\''
            + ", pathPrefix='" + pathPrefix + '\''
            + ", batchSize=" + batchSize
            + ", oldestDataTimestamp=" + oldestDataTimestamp
            + ", excludedTypeIds=" + excludedTypeIds
            + ", includedTypeIds=" + includedTypeIds
            + '}';
    }
}
