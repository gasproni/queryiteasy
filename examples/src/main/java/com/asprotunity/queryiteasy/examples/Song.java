package com.asprotunity.queryiteasy.examples;

import java.io.Serializable;

public class Song implements Serializable {

    public final String title;
    public final String band;
    public final Integer year;

    public Song(String title, String band, Integer year) {
        this.title = title;

        this.band = band;
        this.year = year;
    }

    @Override
    public String toString() {
        return String.join("\n", "Title: " + title, "Band: " + band, "Year: " +year);
    }

}
