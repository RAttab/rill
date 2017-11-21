# Rill

A one-off specialized database for a 2 column schema that focuses on compressed
storage and read-only mmap files.

## Building

```shell
$ mkdir build && cd build
$ PREFIX=.. ../compile.sh
```

Currently only used in the `rill-rs` project which means that no install target
is currently provided. Build artifacts are the following:
- `src/rill.h`
- `build/rill.a`

## Design Space

- A pair is composed of a `u64` key and a `u64` value
- Key cardinality is in the order of 1 million
- Value cardinality is in the order of 100 million
- Infrequent batch query of keys over entire dataset
- Batch queries must finish within 5 minutes
- Pair ingestion must happen in real-time (order of 100k/sec)
- Pairs duplicates are very common
- Expire entire month of data older then 15 months
- Expect around 50 billion unique pairs in a single month
- Servers have around 250Gb of RAM and 2TB of SSD disk space


## Architecture

Rill is split into the following major components:

- `acc`: real-time data ingestion
- `store`: file storage format
- `rotation`: progressive merging and expiration of store files

### Ingestion


### Storage

Basic design philosophy:

- Immutable
- Mutation through merge operation
- All pairs are sorted


#### Compression

The main goal for storage is to fit the entire dataset on the disks of a single
server:

  50B pairs * 15 months * 16bytes per pair = 12TB of disk space

Given our 2TB of available disk space, we need to do some compression to store
everything. A general sketch of the compression is as follows:

- Don't repeat keys
- Uniformize the namespace of the values
- Block encode (LEB128) the uniformized values

Implemention basically begins by extracting all the unique values in the dataset
sorting them and storing them in a table. Using this table, we can then encode
indexes into our table instead of the values themselves. This means our
compression is dependent on the cardinality of the value set and not so much the
values themselves.

Encoding the pairs is a simple scheme of writting the key in full, followed by a
list of all the value associated with that key. The list of values is a
block-encoding (LEB128) of the indexes into the value table. In other words, the
smaller the cardinality of the set the less byte we'll use on average to write a
value.

Empirically, we were are able compress a single month of data down to less then
100GB which means that our dataset now sits comfortably on our 2TB disks.


#### Index

We must also be able to quickly query a single key and extract all the
associated values for that key. Our compression requirements puts a bound on the
size of our index. A general sketch of the index is as follows:

- Don't duplicate keys
- Store the keys along with the offset of their value location in a table
- Search the table via tweaked binary search

Implementation starts by building a table of all the keys and filling in their
offset as we encode the pairs. We also no longer store the keys with the pairs
as we can simply recover the key for a given list of value via it's implicit
index in the file. The index table is stored as is at the end of the file.

Searching is done via a tweaked binary search over the index table. Empirically
this has proven to be fast enough to meet our 5 minutes batch query
requirements. Further optimizations are possible. We've also experimented with a
single pass interpolation search followed by a vectorized linear scan but
changes in the input data meant that the keys were no longer well distributed
which made the approach unusable.


#### Stamp

Safe persistence is accomplished via a pseudo-2-phase commit scheme that uses a
stamp to mark the file as complete. Steps are as follow:

- Write the entire file
- Flush to disk
- Write a magic stamp value in the header
- Flush to disk

This guarantees that if the stamp is found at the beginning of the file then the
file has been completely written and persisted to disk. Note that rill relies on
the underlying file system to detect file corruption as no checksums are
computed or maintained.

Note that after rill files are frequently deleted after being merged so the
stamping mechanism is critical to avoid deleting files that were not properly
merged.


### Rotation
