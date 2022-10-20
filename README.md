# TeepeeDB

Simple Log-Structured Merge tree in go

Useful as a batch database for fast reads

Only allows sorted batches. individual writes should be queued and sorted before attempting insert.

Naive merging. Does not split files for faster merging. Instead merges whole files into each level every time.

Supports LZ4 compression. Handles 100 million keys with smallish values with no problems as long as inserts aren't in too small a batches or too constant.

Merges happen in background go routines. No prefix key compression but LZ4 should accomplish the same thing.
Intended for one LSM DB per table/dataset and no transactions across tables. Sharing a block cache across multiple databases so having many open in the
same process is not an issue.

