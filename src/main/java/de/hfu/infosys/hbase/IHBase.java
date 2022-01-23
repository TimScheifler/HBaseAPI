package de.hfu.infosys.hbase;

import de.hfu.infosys.models.Comment;
import de.hfu.infosys.models.Post;
import de.hfu.infosys.models.User;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface IHBase {

    void writePosts(List<Post> posts) throws IOException;
    void writeComments(List<Comment> comments);
    void writeUsers(Map<String, User> users) throws IOException;

    String userNameByID(long userID);

    Post postByID(long postID);

    Comment commentByID(long commentID);

    List<Long> articleIDsByUserID(long userID) throws IOException;

}
