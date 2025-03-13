package brightspot.recordsync;

import com.psddev.dari.db.State;

/**
 * Interface for injecting custom behavior while deleting records from the database.
 *
 * <p>Implementations of this interface can be used to customize the behavior of
 * the {@link RecordSyncImporter} when deleting records from the database.
 * <p>Specifically, the importer finds all implementations of RecordSyncImportDeleteFilter,
 * iterates through them in typical ClassFinder order (alphabetical by full class name),
 * and invokes {@link #shouldDelete(String, State)} on each record.
 * <p>If none exist, the record will be deleted.
 * <p>If any return false or throw a RuntimeException, the record will not be deleted.
 * <p>If all return true, the record will be deleted.
 * <p>The state is a "reference only" state; it only contains _id and _type.
 */
public interface RecordSyncImportDeleteFilter {

    /**
     * Filter states before deleting them from the database.
     * <p>This method should NOT delete the state. It is only used to determine
     * if the state should be deleted.
     * @param importerName The configured name of the importer.
     * @param state The record state that is to be deleted.
     * @return true if the record should be deleted.
     */
    boolean shouldDelete(String importerName, State state);

}
