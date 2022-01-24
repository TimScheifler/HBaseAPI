package de.hfu.infosys;

import de.hfu.infosys.hbase.HBase;
import de.hfu.infosys.hbase.IHBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.util.List;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Application {

    private static final Logger logger =  Logger.getLogger("Application");

    private static long startTime = -1;

    public static void main(String[] args) throws Exception {
        Application a = new Application();
        a.run();
    }

    public void run () throws Exception {
        Configuration config = HBaseConfiguration.create();
        IHBase hBase = new HBase(config);
        FileProcessor fileProcessor = new FileProcessor(hBase);

        String path = Objects.requireNonNull(this.getClass()
                        .getClassLoader()
                        .getResource("hbase-site.xml"))
                .getPath();
        config.addResource(new Path(path));
        try {
            HBaseAdmin.available(config);
        } catch (MasterNotRunningException e) {
            logger.log(Level.SEVERE, e.getMessage());
            return;
        }
        startTimer();
        fileProcessor.processFile("src/main/resources/posts.dat", FileType.POST);
        printTimerInSeconds();

        fileProcessor.processFile("src/main/resources/comments.dat", FileType.COMMENT);
        printTimerInSeconds();

        System.out.println(hBase.userNameByID(998));

        System.out.println(hBase.postByID(1048865));

        System.out.println(hBase.commentByID(571692));

        List<Long> arrayList = hBase.articleIDsByUserID(998);
        System.out.println("FOUND " + arrayList.size() + " articles of User " + 998+"\n");
        int counter = 0;
        for (Long l: arrayList) {
            counter++;
            System.out.print(l + ", ");
            if(counter%15==0)
                System.out.println();
        }
        printTimerInSeconds();

        hBase.closeConnection();
        System.out.println("Connection closed.");
    }

    private static void startTimer() {
        startTime = System.currentTimeMillis();
    }

    private static void printTimerInSeconds() {
        long estimatedTime = System.currentTimeMillis() - startTime;
        logger.log(Level.INFO, "Done. Time: {0}", estimatedTime / 1000 + "sec.");
    }
}
