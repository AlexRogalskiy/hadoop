//cc JoinGroup Program dołączający członka do grupy

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;

// vv JoinGroup
public class JoinGroup extends ConnectionWatcher {
  
  public void join(String groupName, String memberName) throws KeeperException,
      InterruptedException {
    String path = "/" + groupName + "/" + memberName;
    String createdPath = zk.create(path, null/*dane*/, Ids.OPEN_ACL_UNSAFE,
      CreateMode.EPHEMERAL);
    System.out.println("Utworzono " + createdPath);
  }
  
  public static void main(String[] args) throws Exception {
    JoinGroup joinGroup = new JoinGroup();
    joinGroup.connect(args[0]);
    joinGroup.join(args[1], args[2]);
    
    // Kontynuuje pracę do momentu zamknięcia procesu lub przerwania wątku
    Thread.sleep(Long.MAX_VALUE);
  }
}
// ^^ JoinGroup
