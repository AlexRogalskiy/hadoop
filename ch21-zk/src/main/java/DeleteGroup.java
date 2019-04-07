//cc DeleteGroup Program usuwający grupę i jej członków
import java.util.List;

import org.apache.zookeeper.KeeperException;

// vv DeleteGroup
public class DeleteGroup extends ConnectionWatcher {
    
  public void delete(String groupName) throws KeeperException,
      InterruptedException {
    String path = "/" + groupName;
    
    try {
      List<String> children = zk.getChildren(path, false);
      for (String child : children) {
        zk.delete(path + "/" + child, -1);
      }
      zk.delete(path, -1);
    } catch (KeeperException.NoNodeException e) {
      System.out.printf("Grupa %s nie istnieje\n", groupName);
      System.exit(1);
    }
  }
  
  public static void main(String[] args) throws Exception {
    DeleteGroup deleteGroup = new DeleteGroup();
    deleteGroup.connect(args[0]);
    deleteGroup.delete(args[1]);
    deleteGroup.close();
  }
}
// ^^ DeleteGroup
