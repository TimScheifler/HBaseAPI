package de.hfu.infosys.hbase;

import de.hfu.infosys.models.Comment;
import de.hfu.infosys.models.Post;
import de.hfu.infosys.models.User;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
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

    public HBase(Configuration config) throws IOException {
        Connection connection = createConnection(config);
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
            byte[] value = r.getValue("user".getBytes(), "user".getBytes());
            ret = Bytes.toString(value);
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
            String postId = Bytes.toString(r.getValue(POST_ID.getBytes(), POST_ID.getBytes()));
            String ts = Bytes.toString(r.getValue(TS.getBytes(), TS.getBytes()));
            String post = Bytes.toString(r.getValue(POST.getBytes(), POST.getBytes()));
            String userId = Bytes.toString(r.getValue(USER_ID.getBytes(), USER_ID.getBytes()));
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

            String commentId = Bytes.toString(r.getValue(COMMENT_ID.getBytes(), COMMENT_ID.getBytes()));
            String ts = Bytes.toString(r.getValue(TS.getBytes(), TS.getBytes()));
            String comment = Bytes.toString(r.getValue(COMMENT.getBytes(), COMMENT.getBytes()));
            String userId = Bytes.toString(r.getValue(USER_ID.getBytes(), USER_ID.getBytes()));

            String commentReplied = Bytes.toString(r.getValue((REFERENCING_TO+":"+COMMENT_REPLIED).getBytes(),
                    (REFERENCING_TO+":"+COMMENT_REPLIED).getBytes()));
            String postCommented = Bytes.toString(r.getValue((REFERENCING_TO+":"+POST_COMMENTED).getBytes(),
                    (REFERENCING_TO+":"+POST_COMMENTED).getBytes()));

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
    public List<Long> articleIDsByUserID(long userID) throws IOException {
        List<Long> articleList = new ArrayList<>();

        String userIdString = Long.toString(userID);

        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(USER_ID), Bytes.toBytes(USER_ID), CompareOperator.EQUAL, Bytes.toBytes(userIdString));
        Scan scan = new Scan();
        scan.setFilter(filter);

        articleList.addAll(getArticlesByScanningField(scan, POST_ID, postTable));
        articleList.addAll(getArticlesByScanningField(scan, COMMENT_ID, commentTable));

        return articleList;
    }

    private List<Long> getArticlesByScanningField(Scan scan, String fieldToScan, Table table){
        List<Long> articleList = new ArrayList<>();

        try (ResultScanner scanner = table.getScanner(scan)) {
            for (Result r : scanner)
                articleList.add(Long.parseLong(Bytes.toString(r.getValue(fieldToScan.getBytes(), fieldToScan.getBytes()))));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return articleList;
    }

    private void initiateTablePost() throws IOException {
        HTableDescriptor desc = new HTableDescriptor(postTableName);
        //TableDescriptorBuilder desc = TableDescriptorBuilder.newBuilder(postTableName);
        //desc.modifyColumnFamily(ColumnFamilyDescriptorBuilder.of(Bytes.toBytes(POST_ID)));
        //desc.modifyColumnFamily(ColumnFamilyDescriptorBuilder.of(POST_ID));
        //desc.modifyColumnFamily(ColumnFamilyDescriptorBuilder.of(POST));
        //desc.modifyColumnFamily(ColumnFamilyDescriptorBuilder.of(TS));
        //desc.modifyColumnFamily(ColumnFamilyDescriptorBuilder.of(USER_ID));
        desc.addFamily(new HColumnDescriptor(POST_ID));
        desc.addFamily(new HColumnDescriptor(POST));
        desc.addFamily(new HColumnDescriptor(TS));
        desc.addFamily(new HColumnDescriptor(USER_ID));
        admin.createTable(desc);
    }

    private void initiateTableUser() throws IOException {
        HTableDescriptor desc = new HTableDescriptor(userTableName);
        desc.addFamily(new HColumnDescriptor(USER_ID));
        desc.addFamily(new HColumnDescriptor("user"));
        desc.addFamily(new HColumnDescriptor("articles"));
        admin.createTable(desc);
    }

    private void initiateTableComment() throws IOException {
        HTableDescriptor desc = new HTableDescriptor(commentTableName);
        desc.addFamily(new HColumnDescriptor(COMMENT_ID));
        desc.addFamily(new HColumnDescriptor(COMMENT));
        desc.addFamily(new HColumnDescriptor(TS));
        desc.addFamily(new HColumnDescriptor(USER_ID));
        desc.addFamily(new HColumnDescriptor(REFERENCING_TO));
        admin.createTable(desc);
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
