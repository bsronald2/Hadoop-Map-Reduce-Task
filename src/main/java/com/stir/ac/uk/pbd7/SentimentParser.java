package com.stir.ac.uk.pbd7;

import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Student ID: 2833077
 *
 *
 * Utility class to Parse sentiment Text values.
 */
public class SentimentParser {

    private List<String> columns;
    private List<String> words;

    public List<String> getColumns() {
        return columns;
    }

    /**
     * This method return a list of words gathered from the input text.
     *
     * @return list of words
     */
    public List<String> getWords() {
        return words;
    }

    public String getColumn(int index) {
        if (index >= 0 && index < this.columns.size()) {
            return this.columns.get(index);
        }
        throw new IllegalArgumentException("Index " + index + " is greater/lower than array length");
    }

    /**
     * Parse input line text into list of Strings.
     *
     * @param value line text.
     */
    public void parse(Text value) {
        final int textIndex = 1;
        this.columns = new ArrayList<>(Arrays.asList(value.toString().split("\t")));
        String review = columns.get(textIndex);
        this.words = new ArrayList<>(Arrays.asList(review.split("\\s+")));
        this.columns.remove("");

        // replace any non-word character then remove empty string
        this.words.replaceAll(s -> s.replaceAll("([\\W]+)", ""));
        this.words.remove("");

        this.words.replaceAll(String::toLowerCase);
        this.words.replaceAll(String::trim);
    }

    public String getSentiment() {
        return this.columns.get(2);
    }

    public String getRowType() {
        return this.columns.get(0);
    }

    /**
     * This method return a composite key between item type and sentiment.
     *
     * @return String value as "<item> <sentiment>"
     */
    public String getCompositeKey() { return this.columns.get(0) + " " + this.getColumn(2);}

}
