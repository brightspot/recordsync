package brightspot.recordsync;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;

import com.psddev.dari.db.AggregateDatabase;
import com.psddev.dari.db.Database;
import com.psddev.dari.db.ForwardingDatabase;
import com.psddev.dari.mysql.MySQLDatabase;
import com.psddev.dari.sql.AbstractSqlDatabase;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.StorageItem;
import com.psddev.dari.util.Task;
import com.psddev.dari.util.UuidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Record Sync Exporter.
 * This {@link Runnable} syncs records to a storage bucket using the timestamp in the RecordUpdate table.
 */
public class RecordSyncExporter implements Runnable {

    private static final String DATA_FILE_PREFIX = "recordsync.data.";

    private static final Logger LOGGER = LoggerFactory.getLogger(RecordSyncExporter.class);

    private final String name;
    private final String storage;
    private final String pathPrefix;
    private final long maximumFileSizeMB;
    private final long batchSize;
    private final Duration dataRetention;
    private final Instant oldestDataTimestamp;
    private final Set<UUID> excludedTypeIds;
    private final Set<UUID> includedTypeIds;
    private final Task progressTask;

    public RecordSyncExporter(String name, String storage, String pathPrefix, long maximumFileSizeMB, long batchSize, Duration dataRetention, Instant oldestDataTimestamp, Set<UUID> excludedTypeIds, Set<UUID> includedTypeIds, Task progressTask) {
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("Name cannot be null or empty");
        }
        if (storage == null || storage.trim().isEmpty()) {
            throw new IllegalArgumentException("Storage cannot be null or empty");
        }
        if (dataRetention.isNegative()) {
            throw new IllegalArgumentException("Data Retention cannot be negative");
        }
        if (batchSize < 1) {
            throw new IllegalArgumentException("Batch size cannot be less than 1");
        }
        if (maximumFileSizeMB < 1) {
            throw new IllegalArgumentException("Max Size in MB cannot be less than 1");
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
        this.maximumFileSizeMB = maximumFileSizeMB;
        this.batchSize = batchSize;
        this.dataRetention = dataRetention;
        this.oldestDataTimestamp = oldestDataTimestamp == null ? Instant.EPOCH : oldestDataTimestamp;
        this.excludedTypeIds = new HashSet<>(excludedTypeIds);
        this.includedTypeIds = new HashSet<>(includedTypeIds);
        this.progressTask = progressTask;
    }

    /**
     * Run the export task.
     */
    @Override
    public void run() {

        Instant now = Instant.now();
        LOGGER.info("Exporter [{}]: Starting record export", name);

        // Find the manifest.json file in the storage bucket
        RecordSyncManifest manifest = RecordSyncManifest.load(storage, pathPrefix);

        // Find the previous timestamp and last ID in the manifest file
        Instant previousTimestamp = Instant.ofEpochMilli(Optional.ofNullable(manifest.getLastUpdateDate()).orElse(0L));
        UUID previousId = Optional.ofNullable(manifest.getLastId()).orElse(UuidUtils.ZERO_UUID);
        Map<Long, List<String>> dataFiles = manifest.getDataFiles();
        Map<Long, Long> recordCounts = manifest.getRecordCounts();

        // Delete a previous work-in-progress that may have stalled
        try {
            deleteWipFiles();
            deleteWip();
        } catch (IOException e) {
            LOGGER.error("Failed to delete wip files.", e);
        }

        // Find all records that have been updated since the previous timestamp and before now
        ExportDetails exportDetails = exportRecords(previousTimestamp, previousId, now);

        // Delete expired data
        deleteExpired(dataFiles, recordCounts, now);

        // Delete the WIP file so the next export can start fresh
        try {
            deleteWip();
        } catch (IOException e) {
            LOGGER.error("Failed to delete wip.txt.", e);
        }

        // Add the new export filenames to the dataFiles map for the manifest
        if (!exportDetails.getExportFilenames().isEmpty()) {
            dataFiles.put(now.toEpochMilli(), exportDetails.getExportFilenames());
            recordCounts.put(now.toEpochMilli(), exportDetails.getRecordCount());
        }

        // Update the manifest file with the current timestamp and save it back to the storage bucket
        manifest.setLastUpdateDate(Optional.ofNullable(exportDetails.getLastUpdateDate()).map(Instant::toEpochMilli).orElse(null));
        manifest.setLastId(exportDetails.getLastId());
        manifest.setDataFiles(dataFiles);
        manifest.setRecordCounts(recordCounts);
        manifest.save(storage, pathPrefix);

        LOGGER.info("Exporter [{}]: Exported [{}] records in [{}] seconds", name, exportDetails.getRecordCount(), Duration.between(now, Instant.now()).toSeconds());
    }

    /**
     * Delete expired data from the storage bucket according to the data retention duration specified in the constructor.
     * @param dataFiles The manifest's "dataFiles"
     * @param recordCounts The manifest's "recordCounts"
     * @param now An instant representing "now," calculated at the beginning of the export task.
     */
    private void deleteExpired(Map<Long, List<String>> dataFiles, Map<Long, Long> recordCounts, Instant now) {
        Set<Long> deleteKeys = new HashSet<>();
        for (Map.Entry<Long, List<String>> entry : dataFiles.entrySet()) {
            Instant timestamp = Instant.ofEpochMilli(entry.getKey());
            if (Duration.between(timestamp, now).compareTo(dataRetention) > 0) {
                LOGGER.debug("Deleting expired data at {} because it is {} old, which exceeds the data retention of {}",
                    timestamp, Duration.between(timestamp, now), dataRetention);
                for (String filename : entry.getValue()) {
                    StorageItem storageItem = StorageItem.Static.createIn(storage);
                    if (filename.startsWith(DATA_FILE_PREFIX)) {
                        storageItem.setPath(pathPrefix + "/" + filename);
                        if (storageItem.isInStorage()) {
                            try {
                                storageItem.delete();
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    } else {
                        LOGGER.warn("Ignoring file {} because it doesn't start with {}", filename, DATA_FILE_PREFIX);
                    }
                }
                deleteKeys.add(entry.getKey());
            }
        }
        dataFiles.keySet().removeAll(deleteKeys);
        recordCounts.keySet().removeAll(deleteKeys);
    }

    /**
     * Connect to the underlying SQL database.
     * @return a {@link Connection}
     */
    private static Connection connect() {
        Database db = Database.Static.getDefault();
        while (db instanceof ForwardingDatabase && ((ForwardingDatabase) db).getDelegate() != null) {
            db = ((ForwardingDatabase) db).getDelegate();
        }
        while (db instanceof AggregateDatabase) {
            db = ((AggregateDatabase) db).getDefaultDelegate();
        }
        if (!(db instanceof MySQLDatabase)) {
            throw new IllegalStateException("Database must be an MySQLDatabase");
        }
        return ((AbstractSqlDatabase) db).openReadConnection();
    }

    /**
     * Select specified data from the Record table and upload it to a collection of files in the storage bucket.
     * @param previousTimestamp The timestamp of the last record that was exported on a previous export.
     * @param previousId The ID of the last record that was exported on a previous export.
     * @param now An instant representing "now," calculated at the beginning of the export task.
     * @return {@link ExportDetails} containing the details of the export.
     */
    private ExportDetails exportRecords(Instant previousTimestamp, UUID previousId, Instant now) {
        List<String> exportFilenames = new ArrayList<>();
        List<RecordRow> records;
        UUID lastRecordId = previousId;
        Instant lastTimestamp = previousTimestamp;
        String filePrefix = DATA_FILE_PREFIX + now.toEpochMilli();
        long recordCount = 0L;
        do {
            LOGGER.debug("Selecting records for last timestamp: {} and last record id: {}", lastTimestamp, lastRecordId);
            records = selectRecords(lastTimestamp, now, lastRecordId);
            if (!records.isEmpty()) {
                RecordRow lastRow = records.get(records.size() - 1);
                lastRecordId = lastRow.getId();
                lastTimestamp = lastRow.getUpdateDate();
                LOGGER.debug("Exporter [{}] saving {} records.", name, records.size());
                String filename = saveRecords(filePrefix, records);
                LOGGER.info("Exporter [{}] saved [{}] records at [{}].", name, records.size(), filename);
                exportFilenames.add(filename);
                recordCount += records.size();
            }
            LOGGER.debug("Exporter [{}] found {} records. Last ID: {}, Last Timestamp: {}", name, records.size(), lastRecordId, lastTimestamp);
        } while (!records.isEmpty());

        ExportDetails details = new ExportDetails(lastRecordId, lastTimestamp, exportFilenames, recordCount);
        return details;
    }

    /**
     * Save records using the configured storage and prefix.
     * @param filePrefix File prefix. This should not include the configured path prefix.
     * @param records A list of {@link RecordRow}s {@see #selectRecords}.
     * @return The filename, excluding the configured path prefix.
     */
    private String saveRecords(String filePrefix, List<RecordRow> records) {
        if (records.isEmpty()) {
            throw new IllegalArgumentException("No records found");
        }
        String filename = filePrefix
            + "." + records.get(0).getUpdateDate().toEpochMilli()
            + "." + records.get(0).getId().toString().replaceAll("-", "")
            + ".json.gz";
        StorageItem storageItem = StorageItem.Static.createIn(storage);
        storageItem.setPath(pathPrefix + "/" + filename);
        InputStream data;
        try {
            InputStream recordData = new ByteArrayInputStream(
                Stream.concat(
                records.stream()
                .map(RecordRow::getData),
                Stream.of("")) // One blank line at the end of the file
                .reduce((a, b) -> a + '\n' + b)
                .orElse("")
                .getBytes(StandardCharsets.UTF_8));
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            gzip(recordData, os);
            data = new ByteArrayInputStream(os.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        storageItem.setData(data);
        try {
            updateWip(filename);
            storageItem.save();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return filename;
    }

    /**
     * Update the Work in Progress file with the given filename.
     * @param filename A file that is currently a work in progress and not yet committed to the manifest.
     * @throws IOException if the file is unable to be updated.
     */
    private void updateWip(String filename) throws IOException {
        StorageItem storageItem = StorageItem.Static.createIn(storage);
        storageItem.setPath(pathPrefix + "/wip.txt");
        String wipContents;
        if (storageItem.isInStorage()) {
            wipContents = new String(storageItem.getData().readAllBytes(), StandardCharsets.UTF_8);
        } else {
            wipContents = "";
        }
        wipContents = wipContents + filename + "\n";
        storageItem.setData(new ByteArrayInputStream(wipContents.getBytes(StandardCharsets.UTF_8)));
        storageItem.save();
    }

    /**
     * Delete the Work in Progress file. This should be done after the manifest has been updated.
     * @throws IOException if the file is unable to be deleted.
     */
    private void deleteWip() throws IOException {
        StorageItem storageItem = StorageItem.Static.createIn(storage);
        storageItem.setPath(pathPrefix + "/wip.txt");
        if (storageItem.isInStorage()) {
            storageItem.delete();
        }
    }

    /**
     * Delete files that are in the Work in Progress file but are not already in the manifest.
     * @throws IOException if the files are unable to be deleted.
     */
    private void deleteWipFiles() throws IOException {
        RecordSyncManifest manifest = RecordSyncManifest.load(storage, pathPrefix);
        Set<String> manifestFiles = manifest.getDataFiles().values().stream().flatMap(List::stream).collect(Collectors.toSet());
        StorageItem storageItem = StorageItem.Static.createIn(storage);
        storageItem.setPath(pathPrefix + "/wip.txt");
        if (storageItem.isInStorage()) {
            String wipContents = new String(storageItem.getData().readAllBytes(), StandardCharsets.UTF_8);
            for (String file : wipContents.split("\n")) {
                if (!file.isEmpty()) {
                    if (manifestFiles.contains(file)) {
                        LOGGER.warn("NOT deleting WIP file {} because it is already in the manifest", file);
                    } else {
                        StorageItem fileStorageItem = StorageItem.Static.createIn(storage);
                        fileStorageItem.setPath(pathPrefix + "/" + file);
                        if (fileStorageItem.isInStorage()) {
                            LOGGER.info("Deleting Work in Progress Export File: {}", file);
                            fileStorageItem.delete();
                        }
                    }
                }
            }
        }
    }

    /**
     * Utility to gzip an InputStream
     * @param is The InputStream to read from
     * @param os The OutputStream to write to
     * @throws IOException on failure
     */
    private static void gzip(InputStream is, OutputStream os) throws IOException {
        GZIPOutputStream gzipOs = new GZIPOutputStream(os);
        byte[] buffer = new byte[1024];
        int bytesRead;
        while ((bytesRead = is.read(buffer)) > -1) {
            gzipOs.write(buffer, 0, bytesRead);
        }
        gzipOs.close();
    }

    /**
     * Select records with the provided predicates.
     * @param startTimestamp Lower bound for updateDate. Use the updateDate from the last row of the previous resultset.
     * @param endTimestamp Upper bound for updateDate.
     * @param lastId ID of last row from previous resultset.
     * @return List of {@link RecordRow}.
     */
    private List<RecordRow> selectRecords(Instant startTimestamp, Instant endTimestamp, UUID lastId) {
        if (lastId == null) {
            lastId = UuidUtils.ZERO_UUID;
        }
        List<RecordRow> records = new ArrayList<>();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Exporter [{}] selecting records from {} to {} with last ID {} and limit {}", name,
                startTimestamp.toEpochMilli() / 1000d,
                endTimestamp.toEpochMilli() / 1000d, lastId, batchSize);
        }
        String excludedTypeIdsHexStringList = excludedTypeIds
            .stream()
            .map(uuid -> "0x" + uuid.toString().replaceAll("-", ""))
            .collect(Collectors.joining(","));
        Optional<String> includedTypeIdsPredicate = includedTypeIds.isEmpty()
            ? Optional.empty()
            : Optional.of("AND ru.typeId IN ("
                + includedTypeIds.stream()
                    .map(uuid -> "0x" + uuid.toString().replaceAll("-", ""))
                    .collect(Collectors.joining(","))
                + ") ");
        // Note: This query is written for MySQL. It will need to be adjusted to support other databases.
        String sql = "SELECT HEX(ru.id) AS id, HEX(ru.typeId) AS typeId, r.data, ru.updateDate "
            + "FROM RecordUpdate ru "
            + "LEFT JOIN Record r ON ru.id = r.id AND ru.typeId = r.typeId "
            + "WHERE (ru.updateDate > ? OR (ru.updateDate = ? AND ru.id > UNHEX(?))) "
            + "AND ru.updateDate <= ? "
            + "AND ru.typeId NOT IN (" + excludedTypeIdsHexStringList + ") "
            + includedTypeIdsPredicate.orElse("")
            + "AND ru.updateDate >= ? "
            + "ORDER BY ru.updateDate, ru.id "
            + "LIMIT ?";
        long fileSize = 0;
        try (Connection connection = connect()) {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setDouble(1, startTimestamp.toEpochMilli() / 1000d);
                statement.setDouble(2, startTimestamp.toEpochMilli() / 1000d);
                statement.setString(3, lastId.toString().replaceAll("-", ""));
                statement.setDouble(4, endTimestamp.toEpochMilli() / 1000d);
                statement.setDouble(5, oldestDataTimestamp.getEpochSecond());
                statement.setLong(6, batchSize);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Executing SQL: {}", statement);
                }
                if (statement.execute()) {
                    try (ResultSet resultSet = statement.getResultSet()) {
                        while (resultSet.next()) {
                            RecordRow row = new RecordRow(
                                UuidUtils.fromString(resultSet.getString(1)),
                                UuidUtils.fromString(resultSet.getString(2)),
                                resultSet.getString(3),
                                Instant.ofEpochMilli((long) (resultSet.getDouble(4) * 1000)));
                            if (row.getData() != null) {
                                fileSize += row.getData().length();
                            }
                            if (fileSize >= (maximumFileSizeMB * 1024 * 1024)) {
                                LOGGER.warn("Exporter [{}] stopping at [{}] bytes before reaching maximum file size of [{}] MB with [{}] records", name, fileSize, maximumFileSizeMB, records.size());
                                break;
                            }
                            records.add(row);
                            progressTask.addProgressIndex(1);
                        }
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return records;
    }

    @Override
    public String toString() {
        return "RecordSyncExporter{"
            + "name='" + name + '\''
            + ", storage='" + storage + '\''
            + ", pathPrefix='" + pathPrefix + '\''
            + ", maximumFileSizeMB=" + maximumFileSizeMB
            + ", batchSize=" + batchSize
            + ", dataRetention=" + dataRetention
            + ", oldestDataTimestamp=" + oldestDataTimestamp
            + ", excludedTypeIds=" + excludedTypeIds
            + ", includedTypeIds=" + includedTypeIds
            + '}';
    }

    /**
     * Internal class to represent a resultset row.
     */
    private static class RecordRow {
        private final UUID id;
        private final UUID typeId;
        private final String data;
        private final Instant updateDate;

        public RecordRow(UUID id, UUID typeId, String data, Instant updateDate) {
            this.id = id;
            this.typeId = typeId;
            this.data = data;
            this.updateDate = updateDate;
        }

        public String getData() {
            if (data == null) {
                Map<String, Map<String, String>> result = new LinkedHashMap<>();
                Map<String, String> inner = new LinkedHashMap<>();
                inner.put("_id", id.toString());
                inner.put("_type", typeId.toString());
                result.put("delete", inner);
                return ObjectUtils.toJson(result);
            }
            return data;
        }

        public Instant getUpdateDate() {
            return updateDate;
        }

        public UUID getId() {
            return id;
        }
    }

    /**
     * Internal class to shuttle details from export method.
     */
    private static class ExportDetails {
        private final UUID lastId;
        private final Instant lastUpdateDate;
        private final List<String> exportFilenames;
        private final long recordCount;

        public ExportDetails(UUID lastId, Instant lastUpdateDate, List<String> exportFilenames, long recordCount) {
            this.lastId = lastId;
            this.lastUpdateDate = lastUpdateDate;
            this.exportFilenames = List.copyOf(exportFilenames);
            this.recordCount = recordCount;
        }

        public UUID getLastId() {
            return lastId;
        }

        public Instant getLastUpdateDate() {
            return lastUpdateDate;
        }

        public List<String> getExportFilenames() {
            return exportFilenames;
        }

        public long getRecordCount() {
            return recordCount;
        }
    }
}
