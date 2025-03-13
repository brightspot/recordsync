package brightspot.recordsync;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import com.psddev.dari.db.AsyncDatabaseWriter;
import com.psddev.dari.db.Database;
import com.psddev.dari.db.DatabaseEnvironment;
import com.psddev.dari.db.Query;
import com.psddev.dari.db.State;
import com.psddev.dari.db.WriteOperation;
import com.psddev.dari.util.AsyncQueue;
import com.psddev.dari.util.ClassFinder;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.StorageItem;
import com.psddev.dari.util.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Record Sync Importer.
 * This {@link Runnable} syncs records from a storage bucket and imports them into the Brightspot database.
 * <p>It is intended to be instantiated once per import operation and run once.
 */
public class RecordSyncImporter implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(RecordSyncImporter.class);

    private final String name;
    private final String storage;
    private final String pathPrefix;
    private final int batchSize;
    private final Instant oldestDataTimestamp;
    private final Set<UUID> excludedTypeIds;
    private final Set<UUID> includedTypeIds;
    private final Task progressTask;

    private final Database database;
    private final DatabaseEnvironment dbEnv;
    private final AsyncQueue<State> saveQueue;
    private final AsyncQueue<State> deleteQueue;
    private final AsyncDatabaseWriter<State> saver;
    private final AsyncDatabaseWriter<State> deleter;
    private final List<RecordSyncImportSaveFilter> saveFilters;
    private final List<RecordSyncImportDeleteFilter> deleteFilters;

    public RecordSyncImporter(String name, String storage, String pathPrefix, int batchSize, Instant oldestDataTimestamp, Set<UUID> excludedTypeIds, Set<UUID> includedTypeIds, Task progressTask) {
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
        this.database = Database.Static.getDefault();
        this.dbEnv = DatabaseEnvironment.getCurrent();
        this.saveQueue = new AsyncQueue<>(new ArrayBlockingQueue<>(batchSize));
        this.deleteQueue = new AsyncQueue<>(new ArrayBlockingQueue<>(batchSize));
        this.saver = new AsyncDatabaseWriter<>(progressTask.getExecutor().getName(),
                saveQueue,
                database,
                WriteOperation.SAVE_UNSAFELY,
                this.batchSize,
                true);
        this.deleter = new AsyncDatabaseWriter<>(progressTask.getExecutor().getName(),
            deleteQueue,
            database,
            WriteOperation.DELETE,
            this.batchSize,
            true);

        this.saveFilters = ClassFinder.findConcreteClasses(RecordSyncImportSaveFilter.class)
            .stream()
            .map(cls -> {
                try {
                    return cls.getDeclaredConstructor().newInstance();
                } catch (InstantiationException | IllegalAccessException
                    | InvocationTargetException | NoSuchMethodException e) {
                    throw new RuntimeException(e);
                }
            })
            .map(RecordSyncImportSaveFilter.class::cast)
            .collect(Collectors.toList());

        this.deleteFilters = ClassFinder.findConcreteClasses(RecordSyncImportDeleteFilter.class)
            .stream()
            .map(cls -> {
                try {
                    return cls.getDeclaredConstructor().newInstance();
                } catch (InstantiationException | IllegalAccessException
                    | InvocationTargetException | NoSuchMethodException e) {
                    throw new RuntimeException(e);
                }
            })
            .map(RecordSyncImportDeleteFilter.class::cast)
            .collect(Collectors.toList());
    }

    /**
     * Run the import task.
     * <p>Note that this should be run only once per instance.
     */
    @Override
    public void run() {
        if (saveQueue.isClosed() || deleteQueue.isClosed()) {
            throw new IllegalStateException("Save and/or Delete queues are already closed. Create a new instance for each import.");
        }
        Instant now = Instant.now();
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

        long totalRecordCount = 0L;
        for (Long timestamp : timestampsToImport) {
            totalRecordCount += manifest.getRecordCounts().get(timestamp);
        }
        if (totalRecordCount == 0) {
            LOGGER.info("Importer [{}]: Nothing to import.", name);
            return;
        }
        LOGGER.info("Importer [{}]: Starting record import", name);
        progressTask.setProgressTotal(totalRecordCount);
        long recordCount = 0L;

        // Start async writers
        saver.submit();
        deleter.submit();

        for (Long timestamp : timestampsToImport) {
            synchronized (this) {
                List<String> files = manifest.getDataFiles().get(timestamp);
                LOGGER.info("Importer [{}]: Importing [{}] files for timestamp [{}]: {}", name, files.size(), timestamp, files);
                long startTime = System.currentTimeMillis();

                List<String> errors = new ArrayList<>();
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
                entry.setErrors(errors);
                log.getEntries().add(entry);
                log.save();
            }
        }

        saveQueue.closeAutomatically();
        deleteQueue.closeAutomatically();

        while (saver.isRunning() || deleter.isRunning()) {
            try {
                //noinspection BusyWait
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        LOGGER.info("Importer [{}]: Imported [{}] records in [{}] seconds", name, recordCount, Duration.between(now, Instant.now()).toSeconds());
    }

    private Optional<StateAndOperation> transformJson(String recordJson) {
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
                    if (deleteTypeId != null && !excludedTypeIds.contains(deleteTypeId) && (includedTypeIds.isEmpty() || includedTypeIds.contains(deleteTypeId))) {
                        State state = new State();
                        state.setTypeId(deleteTypeId);
                        state.setId(deleteId);
                        return Optional.of(new StateAndOperation(state, Operation.DELETE));
                    }
                }
            } else {
                UUID typeId = uuid(recordMap.get("_type"));
                if (typeId != null && !excludedTypeIds.contains(typeId) && (includedTypeIds.isEmpty() || includedTypeIds.contains(typeId))) {
                    UUID id = uuid(recordMap.get("_id"));
                    if (id != null) {
                        State state = State.getInstance(dbEnv.createObject(typeId, id));
                        if (state != null) {
                            state.putAll(recordMap);
                            return Optional.of(new StateAndOperation(state, Operation.SAVE));
                        } else {
                            LOGGER.error("Importer [{}]: Unable to create State for typeID [{}]", name, typeId);
                        }
                    }
                }
            }
        }
        return Optional.empty();
    }

    private long downloadAndImportFile(String filename) throws IOException {
        long saved = 0;

        LOGGER.debug("Importer [{}]: Downloading and importing file: {}", name, filename);
        StorageItem storageItem = StorageItem.Static.createIn(storage);
        storageItem.setPath(pathPrefix + '/' + filename);
        InputStream data = storageItem.getData();
        String[] recordJsons = new String(gunzip(data), StandardCharsets.UTF_8).split("\n");
        for (String recordJson : recordJsons) {
            Optional<StateAndOperation> sao = transformJson(recordJson);
            if (sao.isPresent()) {
                StateAndOperation stateAndOperation = sao.get();
                queueWrite(stateAndOperation);
                saved++;
            }
            progressTask.addProgressIndex(1);
        }
        LOGGER.debug("Saved {} records", saved);
        return saved;
    }

    private void queueWrite(StateAndOperation stateAndOperation) {
        State state = stateAndOperation.getState();
        switch (stateAndOperation.getOperation()) {
            case SAVE:
                boolean shouldSave = true;
                for (RecordSyncImportSaveFilter saveFilter : saveFilters) {
                    try {
                        shouldSave = shouldSave && saveFilter.shouldSave(name, state);
                    } catch (RuntimeException e) {
                        LOGGER.warn("Importer [{}]: Exception invoking [{}#shouldSave] attempting to import record ID [{}]. Not saving.", name, saveFilter.getClass().getName(), state.getId(), e);
                        shouldSave = false;
                    }
                }
                if (shouldSave) {
                    saveQueue.add(state);
                }
                break;
            case DELETE:
                boolean shouldDelete = true;
                for (RecordSyncImportDeleteFilter deleteFilter : deleteFilters) {
                    try {
                        shouldDelete = shouldDelete && deleteFilter.shouldDelete(name, state);
                    } catch (RuntimeException e) {
                        LOGGER.warn("Importer [{}]: Exception invoking [{}#shouldDelete] attempting to delete record ID [{}]. Not deleting.", name, deleteFilter.getClass().getName(), state.getId(), e);
                        shouldDelete = false;
                    }
                }
                if (shouldDelete) {
                    saveQueue.add(state);
                }
                break;
            default:
                throw new IllegalStateException("Importer [" + name + "]: Unknown state: " + stateAndOperation.getState());
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
            gzipIn.transferTo(byteOut);
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

    /**
     * Internal enum to represent a write operation.
     */
    private enum Operation {
        SAVE,
        DELETE
    }

    /**
     * Internal tuple-like class to represent a State and an Operation.
     */
    private static class StateAndOperation {
        private final State state;
        private final Operation operation;

        public StateAndOperation(State state, Operation operation) {
            this.state = state;
            this.operation = operation;
        }

        public State getState() {
            return state;
        }

        public Operation getOperation() {
            return operation;
        }
    }
}
