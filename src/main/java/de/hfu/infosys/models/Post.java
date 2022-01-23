package de.hfu.infosys.models;


public class Post {

    private final String ts;
    private final String postId;
    private final String userId;
    private final String post;


    public Post(String ts, String postId, String userId, String post) {
        this.ts = ts;
        this.postId = postId;
        this.userId = userId;
        this.post = post;
    }

    public String getTs() {
        return ts;
    }

    public String getPostId() {
        return postId;
    }

    public String getUserId() {
        return userId;
    }

    public String getPost() {
        return post;
    }

    @Override
    public String toString() {
        return "Post{" +
                "ts='" + ts + '\'' +
                ", postId='" + postId + '\'' +
                ", userId='" + userId + '\'' +
                ", post='" + post + '\'' +
                '}';
    }
}

