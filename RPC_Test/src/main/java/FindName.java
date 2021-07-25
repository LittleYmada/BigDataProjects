import org.apache.hadoop.ipc.VersionedProtocol;

public interface FindName extends VersionedProtocol {
    long versionID = 1L;
    String findName(long student_id);
}
