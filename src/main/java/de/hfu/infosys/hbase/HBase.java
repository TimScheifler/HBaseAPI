package de.hfu.infosys.hbase;

import de.hfu.infosys.models.Comment;
import de.hfu.infosys.models.Post;
import de.hfu.infosys.models.User;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hbase.client.ConnectionFactory.*;

public class HBase implements IHBase {

    private static final String POST_ID = "post_id";
    private static final String USER_ID = "user_id";
    private static final String COMMENT_ID = "comment_id";

    private static final String POST = "post";
    private static final String COMMENT = "comment";
    private static final String USER = "user";
    private static final String TS = "ts";

    private static final String ARTICLES = "articles";

    private static final String REFERENCING_TO = "referencing_to";
    private static final String COMMENT_REPLIED = "comment_replied";
    private static final String POST_COMMENTED = "post_commented";

    private final TableName postTableName = TableName.valueOf("Post");
    private final TableName commentTableName = TableName.valueOf("Comment");
    private final TableName userTableName = TableName.valueOf("User");

    private final Admin admin;

    private final Table postTable;
    private final Table userTable;
    private final Table commentTable;

    private final Connection connection;

    public HBase(Configuration config) throws IOException {
        connection = createConnection(config);
        admin = connection.getAdmin();

        postTable = connection.getTable(postTableName);
        commentTable = connection.getTable(commentTableName);
        userTable = connection.getTable(userTableName);

        deleteTable(postTableName);
        deleteTable(commentTableName);
        deleteTable(userTableName);

        initiateTablePost();
        initiateTableComment();
        initiateTableUser();
    }


    @Override
    public void writePosts(List<Post> posts) {
        List<Put> puts = new ArrayList<>();
        for(Post post : posts){
            byte[] row = Bytes.toBytes(post.getPostId());
            Put p = new Put(row);
            p.addColumn(POST_ID.getBytes(), Bytes.toBytes(POST_ID), Bytes.toBytes(post.getPostId()));
            p.addColumn(TS.getBytes(), Bytes.toBytes(TS), Bytes.toBytes(post.getTs()));
            p.addColumn(POST.getBytes(), Bytes.toBytes(POST), Bytes.toBytes(post.getPost()));
            p.addColumn(USER_ID.getBytes(), Bytes.toBytes(USER_ID), Bytes.toBytes(post.getUserId()));
            puts.add(p);
        }
        try {
            postTable.put(puts);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.print(".");
    }

    @Override
    public void writeComments(List<Comment> comments) {
        List<Put> puts = new ArrayList<>();
        for(Comment comment : comments){
            byte[] row = Bytes.toBytes(comment.getCommentId());
            Put p = new Put(row);
            p.addColumn(COMMENT_ID.getBytes(), Bytes.toBytes(COMMENT_ID), Bytes.toBytes(comment.getCommentId()));
            p.addColumn(TS.getBytes(), Bytes.toBytes(TS), Bytes.toBytes(comment.getTs()));
            p.addColumn(COMMENT.getBytes(), Bytes.toBytes(COMMENT), Bytes.toBytes(comment.getComment()));
            p.addColumn(USER_ID.getBytes(), Bytes.toBytes(USER_ID), Bytes.toBytes(comment.getUserId()));

            if(comment.getCommentReplied().isEmpty()){
                p.addColumn(REFERENCING_TO.getBytes(), Bytes.toBytes(POST_COMMENTED), Bytes.toBytes(comment.getPostCommented()));
            }else{
                p.addColumn(REFERENCING_TO.getBytes(), Bytes.toBytes(COMMENT_REPLIED), Bytes.toBytes(comment.getCommentReplied()));
            }
            puts.add(p);
        }
        try {
            commentTable.put(puts);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.print(".");
    }

    @Override
    public void writeUsers(Map<String, User> users) {
        List<Put> puts = new ArrayList<>();
        for(User user : users.values()){
            byte[] row = Bytes.toBytes(user.getUserId());
            Put p = new Put(row);
            p.addColumn(USER_ID.getBytes(), Bytes.toBytes(USER_ID), Bytes.toBytes(user.getUserId()));
            p.addColumn(USER.getBytes(), Bytes.toBytes(USER), Bytes.toBytes(user.getUser()));
            puts.add(p);
        }
        try {
            userTable.put(puts);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.print(".");
    }

    @Override
    public String userNameByID(long userID) {
        String ret = "";
        byte[] row = Bytes.toBytes(Long.toString(userID));
        Get g = new Get(row);
        try {
            Result r = userTable.get(g);
            ret = getResultValue(r, USER, USER);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ret;
    }

    @Override
    public Post postByID(long postID) {
        Post p = null;
        byte[] row = Bytes.toBytes(Long.toString(postID));
        Get g = new Get(row);
        try {
            Result r = postTable.get(g);
            String postId = getResultValue(r, POST_ID, POST_ID);
            String ts = getResultValue(r, TS, TS);
            String post = getResultValue(r, POST, POST);
            String userId = getResultValue(r, USER_ID, USER_ID);
            p = new Post(ts, postId, userId, post);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return p;
    }

    @Override
    public Comment commentByID(long commentID) {
        Comment c = null;
        byte[] row = Bytes.toBytes(Long.toString(commentID));
        Get g = new Get(row);
        try {
            Result r = commentTable.get(g);

            String commentId = getResultValue(r, COMMENT_ID, COMMENT_ID);
            String ts = getResultValue(r, TS, TS);
            String comment = getResultValue(r, COMMENT, COMMENT);
            String userId = getResultValue(r, USER_ID, USER_ID);

            String commentReplied = getResultValue(r, REFERENCING_TO+":"+COMMENT_REPLIED,
                    REFERENCING_TO+":"+COMMENT_REPLIED);
            String postCommented = getResultValue(r, REFERENCING_TO+":"+POST_COMMENTED,
                    REFERENCING_TO+":"+POST_COMMENTED);

            if(commentReplied == null){
                c = new Comment(ts, commentId, comment, userId, postCommented, "");
            }else{
               c = new Comment(ts, commentId, comment, userId, "", commentReplied);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return c;
    }


    @Override
    public List<Long> articleIDsByUserID(long userID) {
        List<Long> articleList = new ArrayList<>();

        String userIdString = Long.toString(userID);

        SingleColumnValueFilter filter = new SingleColumnValueFilter(
                Bytes.toBytes(USER_ID),
                Bytes.toBytes(USER_ID),
                CompareOperator.EQUAL,
                Bytes.toBytes(userIdString));

        Scan scan = new Scan();
        scan.setFilter(filter);

        articleList.addAll(getArticlesByScanningField(scan, POST_ID, postTable));
        articleList.addAll(getArticlesByScanningField(scan, COMMENT_ID, commentTable));

        return articleList;
    }

    @Override
    public void closeConnection(){
        try {
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private List<Long> getArticlesByScanningField(Scan scan, String fieldToScan, Table table){
        List<Long> articleList = new ArrayList<>();

        try (ResultScanner scanner = table.getScanner(scan)) {
            for (Result r : scanner)
                articleList.add(Long.parseLong(getResultValue(r, fieldToScan, fieldToScan)));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return articleList;
    }

    private String getResultValue(Result r, String family, String qualifier){
        return Bytes.toString(r.getValue(family.getBytes(), qualifier.getBytes()));
    }

    private void initiateTablePost() throws IOException {
        TableDescriptorBuilder desc = TableDescriptorBuilder.newBuilder(postTableName);
        desc.setColumnFamily(ColumnFamilyDescriptorBuilder.of(POST_ID));
        desc.setColumnFamily(ColumnFamilyDescriptorBuilder.of(POST));
        desc.setColumnFamily(ColumnFamilyDescriptorBuilder.of(TS));
        desc.setColumnFamily(ColumnFamilyDescriptorBuilder.of(USER_ID));

        admin.createTable(desc.build());
    }

    private void initiateTableUser() throws IOException {
        TableDescriptorBuilder desc = TableDescriptorBuilder.newBuilder(userTableName);
        desc.setColumnFamily(ColumnFamilyDescriptorBuilder.of(USER_ID));
        desc.setColumnFamily(ColumnFamilyDescriptorBuilder.of(USER));
        desc.setColumnFamily(ColumnFamilyDescriptorBuilder.of(ARTICLES));

        admin.createTable(desc.build());
    }

    private void initiateTableComment() throws IOException {
        TableDescriptorBuilder desc = TableDescriptorBuilder.newBuilder(commentTableName);
        desc.setColumnFamily(ColumnFamilyDescriptorBuilder.of(COMMENT_ID));
        desc.setColumnFamily(ColumnFamilyDescriptorBuilder.of(COMMENT));
        desc.setColumnFamily(ColumnFamilyDescriptorBuilder.of(TS));
        desc.setColumnFamily(ColumnFamilyDescriptorBuilder.of(USER_ID));
        desc.setColumnFamily(ColumnFamilyDescriptorBuilder.of(REFERENCING_TO));

        admin.createTable(desc.build());
    }

    private void deleteTable(TableName table) throws IOException {
        if (admin.tableExists(table)) {
            if(admin.isTableEnabled(table)){
                admin.disableTable(table);
            }
            admin.deleteTable(table);
        }
    }
}
