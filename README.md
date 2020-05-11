## Hadoop Map-Reduce task

This Map-Reduce task will find the 5 most common words associated with a given item type and sentiment. 
The most common words (exclude.txt) have been deleted from this analyses.

The output will consist in 6 rows of data. Each row contains a key word & sentiment (i.e. Restaurant 0) follow by 
the top 5 words for each item-sentiment. The output should be persisted in the following way.

```
 Restaurant 0 brother again law night eating  
 Restaurant 1 you'd any bean fry stir  
 Product 0 anyone must industrial study interested  
 Product 1 phone use restored simple performance  
 Movie  0 enter script watch unethical rated  
 Movie  1 however both superb rickman complex
```

