package brightspot.recordsync;

import java.time.Instant;

import brightspot.task.repeating.global.cron.AbstractGlobalRepeatingCronTask;
import com.psddev.dari.db.Database;
import com.psddev.dari.db.DistributedLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Global Repeating Cron Task wrapper for {@link RecordSyncExporter}.
 * See {@link RecordSyncExportTaskSettings} for task settings.
 */
public class RecordSyncExportTask extends AbstractGlobalRepeatingCronTask {

    private final String name;
    private static final Logger LOGGER = LoggerFactory.getLogger(RecordSyncExportTask.class);

    public RecordSyncExportTask(String executorName, String taskName, String name) {
        super(executorName, taskName);
        this.name = name;
    }

    @Override
    public void doRepeatingTask(Instant runTime) {
        DistributedLock lock = DistributedLock.Static.getInstance(Database.Static.getDefault(), getClass().getName() + ':' + name);

        if (lock.tryLock()) {
            try {
                RecordSyncExportTaskSettings settings = getConfiguration();

                RecordSyncExporter exporter = new RecordSyncExporter(name,
                    settings.getStorageSetting(),
                    settings.getStoragePathPrefix(),
                    settings.getMaximumFileSizeMB(),
                    settings.getBatchSize(),
                    settings.getDataRetention(),
                    settings.getOldestDataTimestamp(),
                    settings.getExcludedTypeIds(),
                    settings.getIncludedTypeIds(),
                    this);

                exporter.run();
            } finally {
                lock.unlock();
            }
        } else {
            LOGGER.warn("Task is already running.");
        }
    }

    @Override
    public RecordSyncExportTaskSettings getConfiguration() {
        return RecordSyncExportTaskSettings.getInstance(name);
    }
}
