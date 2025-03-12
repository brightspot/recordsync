package brightspot.recordsync;

import java.time.Instant;

import brightspot.task.repeating.global.cron.AbstractGlobalRepeatingCronTask;
import com.psddev.dari.db.Database;
import com.psddev.dari.db.DistributedLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Global Repeating Cron Task wrapper for {@link RecordSyncImporter}.
 * See {@link RecordSyncImportTaskSettings} for task settings.
 */
public class RecordSyncImportTask extends AbstractGlobalRepeatingCronTask {

    private final String name;
    private static final Logger LOGGER = LoggerFactory.getLogger(RecordSyncImportTask.class);

    public RecordSyncImportTask(String executorName, String taskName, String name) {
        super(executorName, taskName);
        this.name = name;
    }

    @Override
    public void doRepeatingTask(Instant runTime) throws Exception {

        DistributedLock lock = DistributedLock.Static.getInstance(Database.Static.getDefault(), getClass().getName() + ':' + name);

        if (lock.tryLock()) {
            try {
                RecordSyncImportTaskSettings settings = getConfiguration();

                RecordSyncImporter importer = new RecordSyncImporter(name,
                    settings.getStorageSetting(),
                    settings.getStoragePathPrefix(),
                    settings.getBatchSize(),
                    settings.getOldestDataTimestamp(),
                    settings.getExcludedTypeIds(),
                    settings.getIncludedTypeIds(),
                    this);

                importer.run();
            } finally {
                lock.unlock();
            }
        } else {
            LOGGER.warn("Task is already running.");
        }
    }

    @Override
    public RecordSyncImportTaskSettings getConfiguration() {
        return RecordSyncImportTaskSettings.getInstance(name);
    }
}
