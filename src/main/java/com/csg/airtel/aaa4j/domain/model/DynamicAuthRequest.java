package com.csg.airtel.aaa4j.domain.model;

/**
 * Represents a client request containing an IP address and a shared secret.
 * This class is used to encapsulate the necessary information for client authentication.
 * @author Chathuri_107499
 */
public class DynamicAuthRequest {
    private String username;
    private String password;
    private String nasIdentifier;

    public DynamicAuthRequest(String username, String password, String nasIdentifier){
        this.username = username;
        this.password = password;
        this.nasIdentifier = nasIdentifier;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getNasIdentifier() {
        return nasIdentifier;
    }

    public void setNasIdentifier(String nasIdentifier) {
        this.nasIdentifier = nasIdentifier;
    }
}
