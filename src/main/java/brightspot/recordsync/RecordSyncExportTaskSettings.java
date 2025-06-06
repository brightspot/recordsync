package brightspot.recordsync;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import brightspot.task.dispatcher.Dispatchable;
import brightspot.task.dispatcher.DispatchableDescriptor;
import brightspot.task.repeating.RepeatingTaskConfiguration;
import brightspot.task.repeating.global.cron.GlobalRepeatingCronTask;
import brightspot.task.repeating.global.cron.GlobalRepeatingCronTaskConfiguration;
import com.psddev.dari.db.DatabaseEnvironment;
import com.psddev.dari.db.Record;
import com.psddev.dari.util.CronUtils;
import com.psddev.dari.util.Settings;
import com.psddev.dari.util.SettingsBackedObject;
import com.psddev.dari.util.UuidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Settings for {@link RecordSyncExportTask}.
 * Configure any number of exporters, all with distinct values for [NAME] in the example below:
 *
 * <ul>
 *     <li>brightspot/recordsync/exporters/[NAME]/cron=0 25 4 * * ? // For example, run at 4:25 AM local time every morning. Required.
 *     <li>brightspot/recordsync/exporters/[NAME]/taskHost=job.brightspot // The host name or ip address of the tomcat instance that will execute this task. Required.
 *     <li>brightspot/recordsync/exporters/[NAME]/storage=example-project-ops // Reference to a dari/storage setting. Required.
 *     <li>brightspot/recordsync/exporters/[NAME]/pathPrefix=recordsync // Appended to the storage base path. Required.
 *     <li>brightspot/recordsync/exporters/[NAME]/enabled=true // Enabled flag. If all other settings are valid, the default is true.
 *     <li>brightspot/recordsync/exporters/[NAME]/maxFileSizeMB=50 // Maximum file size of each export file (before compression). Default is {@link #DEFAULT_MAX_FILE_SIZE_MB}.
 *     <li>brightspot/recordsync/exporters/[NAME]/batchSize=2000 // Maximum number of records in each export file; Also the LIMIT clause in the export SQL query. Default is {@link #DEFAULT_BATCH_SIZE}.
 *     <li>brightspot/recordsync/exporters/[NAME]/dataRetentionHours=72 // Retain exported files for this number of hours. Default is {@link #DEFAULT_DATA_RETENTION_HOURS}.
 *     <li>brightspot/recordsync/exporters/[NAME]/excludedTypes=com.psddev.cms.db.ToolEntity,com.psddev.cms.db.SiteSettings // Comma-separated type names to exclude from this export. There are already reasonable defaults in place; this adds to that set. Optional.
 *     <li>brightspot/recordsync/exporters/[NAME]/includedTypes=com.psddev.cms.db.ToolEntity,com.psddev.cms.db.SiteSettings // Comma-separated type names to include in this export. Optional.
 *     <li>brightspot/recordsync/exporters/[NAME]/earliestDate=2024-12-31T00:00:00Z // The {@link DateTimeFormatter#ISO_INSTANT} formatted date of the earliest data to be exported. Records older than this timestamp will never be exported. Optional.
 * </ul>
 *
 * A note on data retention: The importer will use the data in the manifest to
 * ensure that it is not importing the same data multiple times, and also to
 * ensure that it is not skipping files that should be imported. For this
 * reason, the data retention should be at least twice the importer's interval. For
 * example, if the import interval is every 24 hours, the data retention should
 * be at least 48 hours, or to stay on the safe side: 72 hours. The default is
 * 168 hours, or one week, to allow for a reasonable amount of downtime on the importers.
 */
public class RecordSyncExportTaskSettings implements SettingsBackedObject, GlobalRepeatingCronTaskConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(RecordSyncExportTaskSettings.class);
    private static final ConcurrentMap<String, Optional<RecordSyncExportTaskSettings>> INSTANCES = new ConcurrentHashMap<>();

    // public static final String CRON_EXAMPLE = "0/15 * * * * ?"; // Every 15 seconds

    public static final String SETTINGS_PREFIX = "brightspot/recordsync/exporters";
    public static final String CRON_SETTING = "cron"; // The cron schedule for this instance of this task. Required.
    public static final String TASK_HOST_SETTING = "taskHost"; // The host name or ip address of the tomcat instance that will execute this task. Required.
    public static final String STORAGE_SETTING = "storage"; // The named storage configuration that export data is written to. Required.
    public static final String STORAGE_PATH_PREFIX_SETTING = "pathPrefix"; // The prefix appended to the base storage. Required.
    public static final String ENABLED_SETTING = "enabled"; // Enabled flag for this task. Default is true.
    public static final String MAX_FILE_SIZE_MB_SETTING = "maxFileSizeMB"; // Size of each export file in megabytes (before compression). Default is DEFAULT_MAX_FILE_SIZE_MB.
    public static final long DEFAULT_MAX_FILE_SIZE_MB = 100; // 100 MB
    public static final String BATCH_SIZE_SETTING = "batchSize"; // Limit clause of SQL query and max number of records of each export file. Default is DEFAULT_BATCH_SIZE.
    public static final long DEFAULT_BATCH_SIZE = 5000; // 5000 records
    public static final String DATA_RETENTION_HOURS_SETTING = "dataRetentionHours"; // number of hours to retain exports. Default is DEFAULT_DATA_RETENTION_HOURS.
    public static final long DEFAULT_DATA_RETENTION_HOURS = 168; // 1 week
    public static final String EXCLUDED_TYPES_SETTING = "excludedTypes"; // comma-separated list of fully qualified type names. Optional.
    public static final String INCLUDED_TYPES_SETTING = "includedTypes"; // comma-separated list of fully qualified type names. Optional.
    public static final String EARLIEST_DATE_SETTING = "earliestDate"; // ISO 8601 format. Records older than this timestamp will never be exported. Optional.

    public static final String[] DEFAULT_EXCLUDED_TYPES = {
        "brightspot.recordsync.RecordSyncLog",
        "com.psddev.cms.db.Preview",
        "com.psddev.cms.db.ToolUserAction",
        "com.psddev.cms.db.ToolUserDevice",
        "com.psddev.cms.db.ToolUserLoginToken",
        "com.psddev.cms.db.WorkInProgress",
        "com.psddev.cms.rtc.RtcEvent",
        "com.psddev.cms.rtc.RtcSession",
        "com.psddev.job.Job"
    };

    private String name;
    private String cron;
    private String taskHost;
    private String storage;
    private String pathPrefix;
    private boolean enabled;
    private long maxFileSizeMB;
    private long batchSize;
    private Duration dataRetention;
    private Set<UUID> excludedTypes;
    private Set<UUID> includedTypes;
    private Instant earliestDate;

    /**
     * Get all configured instances of this task settings.
     * @return A set of all instances of this task settings.
     */
    @Override
    public Set<? extends RepeatingTaskConfiguration> getAllInstances() {
        Object mapsObj = Settings.get(SETTINGS_PREFIX);
        if (mapsObj instanceof Map) {
            @SuppressWarnings("unchecked")
            Set<String> names = ((Map<String, ?>) mapsObj).keySet();
            if (!names.isEmpty()) {
                return names.stream()
                    .map(RecordSyncExportTaskSettings::getInstance)
                    .collect(Collectors.toSet());
            }
        }
        return Set.of();
    }

    /**
     * Get the instance of this class for the given name.
     * @param name The name of the settings to load
     * @return The settings object
     */
    public static RecordSyncExportTaskSettings getInstance(String name) {
        return INSTANCES.computeIfAbsent(name, RecordSyncExportTaskSettings::createInstance).orElse(null);
    }

    /**
     * Create an instance from settings.
     * @param name The name of the settings to load
     * @return An Optional of the settings object
     */
    private static Optional<RecordSyncExportTaskSettings> createInstance(String name) {
        Object mapObj = Settings.get(SETTINGS_PREFIX + "/" + name);
        if (mapObj instanceof Map) {
            RecordSyncExportTaskSettings settings = new RecordSyncExportTaskSettings();
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) mapObj;
            settings.initialize(SETTINGS_PREFIX + "/" + name, map);
            return Optional.of(settings);
        }
        return Optional.empty();
    }

    /**
     * Initialize this object from the given settings map.
     * @param settingsKey Key used to retrieve the given {@code settings}.
     * @param settings The settings map.
     */
    @Override
    public void initialize(String settingsKey, Map<String, Object> settings) {
        if (settingsKey.startsWith(SETTINGS_PREFIX)) {
            name = settingsKey.substring(SETTINGS_PREFIX.length() + 1);
        } else {
            name = settingsKey;
        }
        cron = Optional.ofNullable(settings.get(CRON_SETTING))
            .map(Object::toString)
            .orElseThrow(() -> new IllegalArgumentException("Missing required setting: " + settingsKey + "/" + CRON_SETTING));
        taskHost = Optional.ofNullable(settings.get(TASK_HOST_SETTING))
            .map(Object::toString)
            .orElseThrow(() -> new IllegalArgumentException("Missing required setting: " + settingsKey + "/" + TASK_HOST_SETTING));
        storage = Optional.ofNullable(settings.get(STORAGE_SETTING))
            .map(Object::toString)
            .orElseThrow(() -> new IllegalArgumentException("Missing required setting: " + settingsKey + "/" + STORAGE_SETTING));
        pathPrefix = Optional.ofNullable(settings.get(STORAGE_PATH_PREFIX_SETTING))
            .map(Object::toString)
            .orElseThrow(() -> new IllegalArgumentException("Missing required setting: " + settingsKey + "/" + STORAGE_PATH_PREFIX_SETTING));
        enabled = Optional.ofNullable(settings.get(ENABLED_SETTING))
            .map(Object::toString)
            .map(Boolean::parseBoolean)
            .orElse(true); // Default is true
        maxFileSizeMB = Optional.ofNullable(settings.get(MAX_FILE_SIZE_MB_SETTING))
            .map(Object::toString)
            .map(Long::parseLong)
            .orElse(DEFAULT_MAX_FILE_SIZE_MB);
        batchSize = Optional.ofNullable(settings.get(BATCH_SIZE_SETTING))
            .map(Object::toString)
            .map(Long::parseLong)
            .orElse(DEFAULT_BATCH_SIZE);
        dataRetention = Optional.ofNullable(settings.get(DATA_RETENTION_HOURS_SETTING))
            .map(Object::toString)
            .map(Long::parseLong)
            .map(Duration::ofHours)
            .orElse(Duration.ofHours(DEFAULT_DATA_RETENTION_HOURS));
        DatabaseEnvironment dbEnv = DatabaseEnvironment.getDefault();
        excludedTypes = Stream.concat(Stream.of(UuidUtils.ZERO_UUID), // Exclude DistributedLock by default
            Stream.concat(Stream.of(DEFAULT_EXCLUDED_TYPES), // Exclude the default types
                Optional.ofNullable(settings.get(EXCLUDED_TYPES_SETTING)) // Add any additional types
                .map(Object::toString)
                .map(s -> s.split(","))
                .stream()
                .flatMap(Arrays::stream))
            .map(String::trim)
            .map(dbEnv::getTypesByGroup)
            .flatMap(Collection::stream)
            .map(Record::getId))
            .collect(Collectors.toSet());
        includedTypes = Optional.ofNullable(settings.get(INCLUDED_TYPES_SETTING))
                    .map(Object::toString)
                    .map(s -> s.split(","))
                    .stream()
                    .flatMap(Arrays::stream)
                    .map(String::trim)
                    .map(dbEnv::getTypesByGroup)
                    .flatMap(Collection::stream)
                    .map(Record::getId)
            .collect(Collectors.toSet());
        earliestDate = Optional.ofNullable(settings.get(EARLIEST_DATE_SETTING))
            .map(Object::toString)
            .map(Instant::parse)
            .orElse(null);
        if (enabled) {
            LOGGER.info("Record Sync Export task [{}] scheduled to run [{}] on [{}]", name, CronUtils.getCronDescription(cron), taskHost);
        } else {
            LOGGER.info("Record Sync Export task [{}] is not enabled, otherwise it would run [{}] on [{}]", name, CronUtils.getCronDescription(cron), taskHost);
        }
    }

    @Override
    public Class<? extends GlobalRepeatingCronTask> getType() {
        return RecordSyncExportTask.class;
    }

    @Override
    public DispatchableDescriptor<? extends Dispatchable> getDescriptor() {
        UUID descriptorId = UuidUtils.createVersion3Uuid(getClass().getName() + "/" + name);
        return new DispatchableDescriptor<>(descriptorId, RecordSyncExportTask.class);
    }

    @Override
    public GlobalRepeatingCronTaskConfiguration get() {
        return this;
    }

    @Override
    public GlobalRepeatingCronTask create(String taskExecutorName, String taskName) {
        return new RecordSyncExportTask("Record Sync Exporters", taskName + ": " + name, name);
    }

    @Override
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Check Task Host to see if it is enabled.
     * @return true if the task host matches the current host, false otherwise.
     */
    @Override
    public boolean isAllowedToRun() {
        try {
            InetAddress localhost = InetAddress.getLocalHost();
            InetAddress taskhost = InetAddress.getByName(taskHost.trim());
            return (localhost.getHostAddress().equals(taskhost.getHostAddress()));
        } catch (UnknownHostException e) {
            LOGGER.warn("Exception thrown when trying to resolve host name! Message: {}", e.getMessage());
            return false;
        }
    }

    @Override
    public String getCronExpression() {
        return cron;
    }

    public String getStoragePathPrefix() {
        return pathPrefix;
    }

    public String getStorageSetting() {
        return storage;
    }

    public long getBatchSize() {
        return batchSize;
    }

    public Duration getDataRetention() {
        return dataRetention;
    }

    public Set<UUID> getExcludedTypeIds() {
        return new HashSet<>(excludedTypes);
    }

    public Set<UUID> getIncludedTypeIds() {
        return new HashSet<>(includedTypes);
    }

    public Instant getOldestDataTimestamp() {
        return earliestDate;
    }

    public long getMaximumFileSizeMB() {
        return maxFileSizeMB;
    }
}
