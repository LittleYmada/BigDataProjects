import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.ipc.ProtocolSignature;

import java.io.IOException;

public class FindNameImpl implements FindName{
    @Override
    public String findName(long student_id) {
        if (student_id == 20210735010137L) {
            System.out.println("Get student_id: " + student_id + ", & return name: localityV");
            return "localityV";
        } else {
            System.out.println("Get student_id: " + student_id + ", & it is illegal");
            return null;
        }
    }

    @Override
    public long getProtocolVersion(String s, long l) throws IOException {
        return versionID;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String s, long l, int i) throws IOException {
        return null;
    }
}
