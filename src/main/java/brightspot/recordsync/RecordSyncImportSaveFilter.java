package brightspot.recordsync;

import com.psddev.dari.db.State;

/**
 * Interface for injecting custom behavior while importing records into the database.
 *
 * <p>Implementations of this interface can be used to customize the behavior of
 * the {@link RecordSyncImporter} when importing records into the database.
 * <p>Specifically, the importer finds all implementations of RecordSyncImportSaveFilter,
 * iterates through them in typical ClassFinder order (alphabetical by full class name),
 * and invokes {@link #shouldSave(String, State)} on each record.
 * <p>If none exist, the record will be imported.
 * <p>If any return false or throw a RuntimeException, the record will not be imported.
 * <p>If all return true, the record will be imported.
 * <p>The state is mutable and can be modified by the filter before saving.
 */
public interface RecordSyncImportSaveFilter {

    /**
     * Filter states before importing them into the database.
     * <p>This method should NOT save the state. It is only used to determine
     * if the state should be saved.
     * @param importerName The configured name of the importer.
     * @param state The record state that is to be imported.
     * @return true if the record should be imported.
     */
    boolean shouldSave(String importerName, State state);

}
