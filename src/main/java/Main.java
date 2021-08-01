import java.util.Arrays;

public class Main {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("USAGE: ZGroups <listen | TODO> host-port path [args]");
            System.exit(2);
        }

        String cmd = args[0];
        String hostPort = args[1];
        String znode = args[2];

        Program program = null;
        if ("listen".compareTo(cmd) == 0) {
            try {
                program = new Executor(hostPort, znode, Arrays.copyOfRange(args, 3, args.length));
            } catch (Exception e) {
                e.printStackTrace();
                return;
            }
        }

        if (program == null) {
            System.err.println("USAGE: ZGroups <command>");
            System.exit(2);
        }
        
        program.main();
    }
}
