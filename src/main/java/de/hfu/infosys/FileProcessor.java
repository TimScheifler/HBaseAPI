package de.hfu.infosys;

import de.hfu.infosys.hbase.IHBase;
import de.hfu.infosys.models.Comment;
import de.hfu.infosys.models.Post;
import de.hfu.infosys.models.User;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FileProcessor {

    private static final Logger logger =  Logger.getLogger("FileProcessor");
    private static final int MAX_COUNT = 10000;
    private final IHBase hbase;



    public FileProcessor(final IHBase hbase) {
        this.hbase = hbase;
    }

    public void processFile(final String path, final FileType fileType) throws Exception {
        if (fileType.equals(FileType.POST))
            processPost(path);
        else if (fileType.equals(FileType.COMMENT))
            processComment(path);
        else
            logger.log(Level.WARNING, "Unknown FileType");
    }

    private void processPost(final String path) throws IOException {
        File file = new File(path);
        FileReader fileReader = new FileReader(file);
        List<Post> posts = new ArrayList<>();
        Map<String, User> userMap = new HashMap<>();
        int count = 0;

        String line;
        try (BufferedReader br = new BufferedReader(fileReader)) {
            while ((line = br.readLine()) != null) {

                count++;
                String[] sl = splitLine(line);

                Post post = new Post(sl[0], sl[1], sl[2], sl[3]);
                User user = new User(sl[2], sl[4]);

                posts.add(post);
                userMap.put(sl[2], user);

                if (count > MAX_COUNT) {
                    count = 0;
                    hbase.writePosts(posts);
                    posts.clear();
                }
                if(userMap.size() > MAX_COUNT){
                    hbase.writeUsers(userMap);
                    userMap.clear();
                }
            }
        }
        hbase.writePosts(posts);
        posts.clear();
        hbase.writeUsers(userMap);
        userMap.clear();
        fileReader.close();
    }

    private void processComment(final String path) throws IOException {
        File file = new File(path);
        FileReader fileReader = new FileReader(file);
        List<Comment> comments = new ArrayList<>();
        Map<String, User> userMap = new HashMap<>();
        int count = 0;
        String line;
        try (BufferedReader br = new BufferedReader(fileReader)) {
            while ((line = br.readLine()) != null) {
                count++;
                String[] sl = splitLine(line);
                Comment comment;
                if(sl[5].isEmpty()){
                    comment = new Comment(sl[0], sl[1], sl[3], sl[2], "", sl[6]);
                }else{
                    comment = new Comment(sl[0], sl[1], sl[3], sl[2], sl[5], "");
                }
                User user = new User(sl[2], sl[4]);

                comments.add(comment);
                userMap.put(sl[2], user);

                if (count > MAX_COUNT) {
                    count = 0;
                    hbase.writeComments(comments);
                    comments.clear();
                }
                if(userMap.size() > MAX_COUNT){
                    hbase.writeUsers(userMap);
                    userMap.clear();
                }
            }
        }
        hbase.writeComments(comments);
        comments.clear();
        hbase.writeUsers(userMap);
        userMap.clear();
        fileReader.close();

    }

    private String[] splitLine(final String line) {
        return line.split("\\|");
    }

}