package de.hfu.infosys.models;

public class Comment {

    private final String ts;
    private final String commentId;
    private final String comment;
    private final String userId;
    private final String postCommented;
    private final String commentReplied;

    public Comment(String ts, String commentId, String comment, String userId, String postCommented, String commentReplied) {
        this.ts = ts;
        this.commentId = commentId;
        this.comment = comment;
        this.userId = userId;
        this.postCommented = postCommented;
        this.commentReplied = commentReplied;
    }

    public String getTs() {
        return ts;
    }

    public String getCommentId() {
        return commentId;
    }

    public String getComment() {
        return comment;
    }

    public String getUserId() {
        return userId;
    }

    public String getPostCommented() {
        return postCommented;
    }

    public String getCommentReplied() {
        return commentReplied;
    }

    @Override
    public String toString() {
        return "Comment{" +
                "ts='" + ts + '\'' +
                ", commentId='" + commentId + '\'' +
                ", comment='" + comment + '\'' +
                ", userId='" + userId + '\'' +
                ", postCommented='" + postCommented + '\'' +
                ", commentReplied='" + commentReplied + '\'' +
                '}';
    }
}

