---
title: Clustering
summary: "In this page, we describe async compaction in Hudi."
toc: true
last_modified_at:
---

Trade-offs exist between write speeds (data latency or data freshness) and query performance in many different data systems. Data writes are faster with small files because you process the data as soon as it’s available, avoiding an intermediate aggregation step to combine smaller files into larger ones. However, query performance degrades poorly with a lot of small files. In particular, with data lakes, small files cost more disks seeks and slows down compute in downstream distributed computing applications. You can refer to the file size documentation about the small file problem. In Hudi, you can use the clustering table service to:
- improve the data freshness by stitching small files to larger ones.
- improve query performance by changing the data layout via sorting data on different columns.

## Data ingestion and file sizing trade offs
When you write data to a Hudi table, the write mode you choose will have a tradeoff between write speed and file size. For example, BULK_INSERT without any additional configurations will have a very fast ingestion, but this mode may create a lot of small files. You can optionally set the sorting mode for BULK_INSERT to fix the file sizing when data is ingested and inserted into a Hudi table, but you’ll encounter a higher write latency (i.e. data ingestion will be slower).  

Another trade off example is when you want to manage how fast you want to INSERT data and file size them. For example, Hudi has a configuration called `hoodie.parquet.small.file.limit` to specify the maximum file size for a small Parquet file. This configuration helps Hudi determine where INSERTs will get written to. Small file groups have higher probability of accepting new inserts than larger file groups. By setting an appropriate limit, you can control how Hudi handles small files to improve query performance and storage efficiency while maintaining a balance with write speed.

For example, if you set the `hoodie.parquet.small.file.limit` to a value such as 50MB, Hudi will consider any Parquet file with a size less than 50MB as a small file. These small files will then be targeted for merging and insertion into file groups to create larger files. 


Please refer to the write operations documentation for more details about the different write operations and their configurations. 

## Clustering allows fast ingestion without compromising query performance 

The clustering service was introduced to optimize the Hudi data lake file layout by rewriting data without compromising on query performance. When a clustering service is performed, a “REPLACECOMMIT” action will appear in the Hudi [timeline](https://hudi.apache.org/docs/timeline). 

By default, the clustering service is not enabled. By default, the clustering service is not enabled. The following section will cover how to enable, manage and deploy the clustering service.

## Schedule, plan, execute and deploy a clustering service
There are three processes in a clustering service: you'll need to schedule, plan and execute it. From there, separately, there is a deployment model which we'll cover in a later section. Below, we'll go over each of the processes:

- Schedule clustering: You'll set a clustering schedule so the clustering process commences. When a threshold is reached, the clustering plan is triggered. 
- Plan clustering: You'll plan how to configure which file groups, layout and/or file sizing is optimal for your application. 
- Execute clustering: Hudi reads the clustering plan and applies the execution strategy to rewrite the data. 

### Schedule clustering

For the clustering service to run in your application, you need to set  `hoodie.clustering.inline` or `hoodie.clustering.async.enabled` to enable inline or async, respectively.

Once one of those configurations is set to true, you need to set a schedule by identifying a commit threshold it takes so the clustering plan phase can commence. You can set the commit threshold through the `hoodie.clustering.inline.max.commits`​ or `hoodie.clustering.async.max.commits`​ configuration depending on whether you want to enable inline or async clustering, respectively. 











