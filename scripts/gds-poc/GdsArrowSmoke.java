import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import com.sun.jna.Memory;
import com.sun.jna.Pointer;

import ai.relatedb.Database;

/** Opens a GDS graph.arrow from native memory and runs each argument as a query. */
public class GdsArrowSmoke {
    public static void main(String[] args) throws Exception {
        long start = System.nanoTime();
        Database db;
        try (FileChannel file = FileChannel.open(Path.of(args[0]), StandardOpenOption.READ)) {
            long size = file.size();
            Memory memory = new Memory(size);
            long chunk = 1L << 30;
            for (long at = 0; at < size; at += chunk) {
                long length = Math.min(chunk, size - at);
                file.read(memory.getByteBuffer(at, length), at);
            }
            db = Database.openGdsArrow(Pointer.nativeValue(memory), size);
            memory.close();
        }
        System.out.printf("opened in %.1f s%n", (System.nanoTime() - start) / 1e9);
        try (db) {
            Database.Connection conn = db.connect();
            for (int i = 1; i < args.length; i++) {
                long t = System.nanoTime();
                Database.Result result = conn.query(args[i]);
                System.out.printf("%s%n  %s %s (%.1f ms)%n", args[i], result.columns(), result.rows(),
                        (System.nanoTime() - t) / 1e6);
            }
            try {
                conn.query("CREATE ()");
                throw new AssertionError("a write succeeded");
            } catch (ai.relatedb.RelateException e) {
                System.out.println("write refused: " + e.getMessage());
            }
        }
    }
}
