import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class LeaderElection implements Watcher {

  private static final String ZOOKEEPER_HOST = "localhost:2181";
  private static final int SESSION_TIMEOUT_MS = 3000;

  private static final String LEADER_ELECTION_SAMPLE_APP = "leader_election_sample_app";

  private static  final String APPLICATION_ROOT_ZNODE_PATH = "/" + LEADER_ELECTION_SAMPLE_APP;
  private String currentZnodeName;

  private ZooKeeper zooKeeper;

  public static void main(String[] args) throws IOException, InterruptedException, KeeperException {

    LeaderElection leaderElection = new LeaderElection();
    leaderElection.connectToZooKeeperServer();
    leaderElection.createZnode();
    leaderElection.becomeLeaderOrWatchPredecessor();

    leaderElection.run();
    leaderElection.close();
    System.out.println("Disconnected from ZooKeeper server! Current Thread ID: " + Thread.currentThread().getId());
  }

  public void connectToZooKeeperServer() throws IOException {
    zooKeeper = new ZooKeeper(ZOOKEEPER_HOST, SESSION_TIMEOUT_MS, this);
  }

  /**
   *
   * @return Znode name created under application's parent Znode
   * @throws InterruptedException
   * @throws KeeperException
   */
  public void createZnode() throws InterruptedException, KeeperException {

    String znodePath = APPLICATION_ROOT_ZNODE_PATH + "/" + LEADER_ELECTION_SAMPLE_APP + "_";
    String znodeActualCreationPath = zooKeeper.create(znodePath, new byte[]{}, Ids.OPEN_ACL_UNSAFE,
        CreateMode.EPHEMERAL_SEQUENTIAL);

    System.out.println("Znode path requested to create: " + znodePath);
    System.out.println("Znode actual creation path: " + znodeActualCreationPath);

    currentZnodeName = znodeActualCreationPath.replace(APPLICATION_ROOT_ZNODE_PATH + "/", "");
  }

  public void becomeLeaderOrWatchPredecessor() throws InterruptedException, KeeperException {

    List<String> znodeChildren = zooKeeper.getChildren(APPLICATION_ROOT_ZNODE_PATH, false);

    Collections.sort(znodeChildren);
    String firstZnodeChild = znodeChildren.get(0);

    Optional<Stat> predecessorStat = Optional.empty();
    boolean becameLeader = false;

    while(predecessorStat.isEmpty() && !becameLeader) {
      if (firstZnodeChild.equals(currentZnodeName)) {
        System.out.println("I am the leader");
        becameLeader = true;
      } else {
        System.out.println("I am not the leader");
        int currentZnodeIndex = Collections.binarySearch(znodeChildren, currentZnodeName);
        int predecessorZnodeIndex = currentZnodeIndex - 1;
        String predecessorZnodeName = znodeChildren.get(predecessorZnodeIndex);

        predecessorStat = Optional.of(zooKeeper.exists(APPLICATION_ROOT_ZNODE_PATH + "/" + predecessorZnodeName, this));

        System.out.println("Watching predecessor Znode: " + predecessorZnodeName);
      }
    }
  }

  private void run() throws InterruptedException {
    synchronized (zooKeeper) {
      zooKeeper.wait();
    }
  }

  public void close() throws InterruptedException {
    zooKeeper.close();
  }

  @Override
  public void process(WatchedEvent event) {
    switch (event.getType()){
      case None:
        if (event.getState().equals(KeeperState.SyncConnected)) {
          System.out.println("Successful connection event to ZooKeeper server received! Current Thread ID: " + Thread.currentThread().getId());
        } else {
          System.out.println("Zookeeper Disconnection event to Zookeeper server received! Current Thread ID: " + Thread.currentThread().getId());
          synchronized (zooKeeper) {
            zooKeeper.notifyAll();
          }
        }
        break;
      case NodeDeleted:
        try {
          becomeLeaderOrWatchPredecessor();
        } catch (InterruptedException | KeeperException e) {
          throw new RuntimeException(e);
        }
    }
  }
}
