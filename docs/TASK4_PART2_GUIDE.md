# Task 4 (Part 2) – what is new, how to test it, what to write

This guide covers only what was **added in Part 2** to Task 4:

| Part 2 addition | Section |
|---|---|
| Failures are detected from **missed heartbeats** and start a repair job | A |
| **Erasure-coded data is repaired** (lost shards are rebuilt) | B |
| Erasure-coded **uploads work while a node is down** (ring placement, 5 of 6 quorum) | C |
| A **corrupted copy is deleted and written again** | D |
| **Benchmark**: replication vs erasure coding on the 2 required workloads | E |

Run everything in **PowerShell** in the project folder. 📸 = take a screenshot.

---

## Step 0 – Start the system and create test data

1. Start the stack. Use `--profile scale-out` so all **8** storage nodes run
   (node8 was added in Task 8; without it, node8 shows as failed).
   ```powershell
   docker compose -f compose.storage.yaml --env-file .env --profile scale-out up -d
   ```
2. Prepare PowerShell:
   ```powershell
   .\.venv\Scripts\Activate.ps1
   ```
   ```powershell
   $key = (Select-String -Path .env -Pattern '^GATEWAY_API_KEY=').Line.Split('=',2)[1]; $h = @{"X-API-Key" = $key}; $env:GATEWAY_API_KEY = $key
   ```
3. Wait about 1 minute, then check that the 8 nodes are online (8 rows with `True`):
   ```powershell
   (Invoke-RestMethod http://localhost:8010/nodes) | Format-Table node_id, online, seconds_since_heartbeat
   ```
4. Create two random 10 MB test files:
   ```powershell
   foreach ($f in "t4.bin","t4b.bin") { $b = New-Object byte[] (10MB); (New-Object Random).NextBytes($b); [IO.File]::WriteAllBytes("$PWD\$f", $b) }
   ```
5. Create one bucket per mode and upload `t4.bin` to both:
   ```powershell
   Invoke-RestMethod -Method Post http://localhost:8020/buckets -Headers $h -ContentType "application/json" -Body '{"name":"t4-rep","mode":"replication"}'
   ```
   ```powershell
   Invoke-RestMethod -Method Post http://localhost:8020/buckets -Headers $h -ContentType "application/json" -Body '{"name":"t4-ec","mode":"erasure"}'
   ```
   ```powershell
   Invoke-RestMethod -Method Put http://localhost:8020/objects/t4-rep/t4.bin -Headers $h -InFile t4.bin -ContentType application/octet-stream
   ```
   ```powershell
   Invoke-RestMethod -Method Put http://localhost:8020/objects/t4-ec/t4.bin -Headers $h -InFile t4.bin -ContentType application/octet-stream
   ```
6. ⏳ **Wait 35 seconds.** The scheduler ignores objects uploaded in the last 30 s.

If you repeat the tests later, the buckets already exist (the create command then fails with
409). That's fine: skip it and just upload again.

---

## A + B – Heartbeat failure detection and erasure-coded repair

**What to write in the report**

> **Failure detection with heartbeats.** In Part 1, the repair worker only found problems by
> probing each copy. In Part 2, failures are detected from heartbeats, as the specification
> requires. Every storage node sends a heartbeat to the metadata service every 5 seconds. A node
> that sends no heartbeat for 15 seconds (three missed heartbeats) is marked as **failed**. The
> repair scheduler checks the node list every 10 seconds. When it sees a newly failed node, it
> creates a repair job and queues every chunk that had a copy or a shard on that node. The
> workers then restore the missing data on other live nodes, chosen with the consistent hashing
> ring. Each repair job is stored in PostgreSQL with its reason, its progress (chunks repaired out
> of the total) and its repair traffic, and the web UI shows it live. When the node sends
> heartbeats again, it is marked as **recovered**. To avoid false alarms, a request that fails
> because of a short network error is retried before a copy is treated as lost.

> **Repair of erasure-coded data.** In Part 1 only replicated chunks were repaired. Now
> erasure-coded chunks are repaired too. When shards are lost (failed node) or damaged (bit rot),
> a worker reads any 4 healthy shards, decodes the original chunk with zfec and checks its
> SHA-256 hash. It then encodes the chunk again and writes **only the missing shards**, each on a
> live node that does not already hold a shard of the same chunk, and updates the shard locations
> in the metadata. Because of deduplication, the same shard may already be stored on another
> healthy node; in that case the worker simply points the lost location to that copy, and nothing
> has to be transferred. A 7th storage node was added (and an 8th in Task 8) because each chunk
> needs 6 different nodes. With only 6 nodes, a lost shard would have no free node to be rebuilt
> on.

**Test**

1. Open http://localhost:3000, paste the API key, click **Connect** and open
   **Cluster Monitor**. 📸 **Screenshot A1:** all 8 nodes Online.
2. Show where the 6 shards of the first chunk of `t4.bin` are stored.
   📸 **Screenshot A2**
   ```powershell
   docker exec pg1 psql -U ds_user -d ds_storage -c "SELECT version, shard_index, node_id FROM erasure_shards WHERE bucket='t4-ec' AND object_key='t4.bin' AND position=0 ORDER BY version DESC, shard_index LIMIT 6;"
   ```
   Choose one node from this list, for example `node4`. Below, replace **4** with your number.
3. Stop that node:
   ```powershell
   docker stop storage-node-4
   ```
4. Watch the Cluster Monitor (it refreshes every 3 s). After about 15–25 s, node4 turns
   **Offline** and a job *"node failure (missed heartbeats): node4"* appears, with its progress bar
   filling up. 📸 **Screenshot A3** (job running, if you can catch it) and 📸 **Screenshot A4**
   (job completed).
5. The scheduler detected the failure from missed heartbeats and queued the job.
   📸 **Screenshot A5**
   ```powershell
   docker logs --since 3m midtermds-repair-scheduler-1 | Select-String "NODE FAILED|QUEUED"
   ```
6. The workers repaired the replicas (`REPAIRED replica`) and rebuilt the shards
   (`REBUILT shard`). 📸 **Screenshot A6**
   ```powershell
   docker compose -f compose.storage.yaml --env-file .env logs --since 3m repair-worker | Select-String "REPAIRED|REBUILT|COMPLETED" | Select-Object -Last 12
   ```
7. Run the query from step 2 again. The shard that was on node4 now shows **another node**.
   📸 **Screenshot A7** (put it next to A2 in the report)
8. Both files still download correctly while node4 is down. All 3 hashes are the same, and
   `degraded_objects` is 0. 📸 **Screenshot A8**
   ```powershell
   curl.exe -s -o out_rep.bin http://localhost:8020/objects/t4-rep/t4.bin -H "X-API-Key: $key"; curl.exe -s -o out_ec.bin http://localhost:8020/objects/t4-ec/t4.bin -H "X-API-Key: $key"; (Get-FileHash t4.bin).Hash; (Get-FileHash out_rep.bin).Hash; (Get-FileHash out_ec.bin).Hash
   ```
   ```powershell
   Invoke-RestMethod http://localhost:8020/cluster/status -Headers $h | Select-Object degraded_objects, inaccessible_objects
   ```

⚠️ **Keep node4 stopped** and continue with C.

## C – Erasure-coded upload while a node is down

**What to write in the report**

> **Erasure-coded writes use the hash ring and a write quorum.** In Part 1, erasure-coded writes
> always used node1 to node6, so an upload failed as soon as one of these nodes was down. Now the
> 6 nodes are chosen with the consistent hashing ring, like replicas, and offline nodes are
> skipped. A write succeeds when at least 5 of the 6 shards are stored (write quorum 5 of 6),
> so uploads keep working when a node fails.

**Test** (node4 is still stopped)

1. Upload the second file to the erasure-coded bucket. It succeeds. 📸 **Screenshot C1**
   ```powershell
   Invoke-RestMethod -Method Put http://localhost:8020/objects/t4-ec/while-down.bin -Headers $h -InFile t4b.bin -ContentType application/octet-stream
   ```
2. Its 6 shards are on 6 different nodes, and **none** is on the stopped node4.
   📸 **Screenshot C2**
   ```powershell
   docker exec pg1 psql -U ds_user -d ds_storage -c "SELECT version, shard_index, node_id FROM erasure_shards WHERE bucket='t4-ec' AND object_key='while-down.bin' AND position=0 ORDER BY version DESC, shard_index LIMIT 6;"
   ```
3. Restart the node. About 10 s later the scheduler logs `NODE RECOVERED`, and the node is
   Online again in the Cluster Monitor. 📸 **Screenshot C3**
   ```powershell
   docker start storage-node-4
   ```
   ```powershell
   docker logs --since 1m midtermds-repair-scheduler-1 | Select-String "RECOVERED"
   ```

## D – A corrupted copy (bit rot) is deleted and written again

**What to write in the report**

> **Bit-rot repair.** Every 30 seconds the scheduler scrubs the data: each storage node re-reads
> every copy and checks its SHA-256 hash. A copy whose content no longer matches its hash is
> reported as corrupted (HTTP 409). In Part 1 the corrupted copy stayed on the node. Now the
> worker deletes it and writes a clean copy, taken from a healthy replica (or rebuilt from 4
> healthy shards for erasure coding), so the chunk has 3 healthy copies again.

**Test**

1. Find a node holding the first chunk of `t4.bin` in the replicated bucket:
   ```powershell
   $hash = (Invoke-RestMethod http://localhost:8020/object-metadata/t4-rep/t4.bin -Headers $h).chunks[0].hash; $node = (Invoke-RestMethod http://localhost:8010/chunks/locations/$hash).nodes[0]; $n = $node -replace 'node',''; "$node port 800$n"
   ```
2. Corrupt one byte of that copy. The node now refuses it with **409**. 📸 **Screenshot D1**
   ```powershell
   curl.exe -s -o NUL -w "before: %{http_code}`n" http://localhost:800$n/chunks/$hash
   ```
   ```powershell
   docker exec storage-node-$n sh -c "printf X | dd of=/data/$hash bs=1 conv=notrunc 2>/dev/null"
   ```
   ```powershell
   curl.exe -s -o NUL -w "after corrupting: %{http_code}`n" http://localhost:800$n/chunks/$hash
   ```
3. Wait about 40 s (one scrub round), then show the logs. The scheduler reports `1 damaged`;
   the worker logs `corrupt=['nodeX']` and then `REPAIRED replica ... -> nodeX`.
   📸 **Screenshot D2**
   ```powershell
   docker logs --since 2m midtermds-repair-scheduler-1 | Select-String "damaged|scrub"
   ```
   ```powershell
   docker compose -f compose.storage.yaml --env-file .env logs --since 2m repair-worker | Select-String $hash.Substring(0,12)
   ```
4. The copy is healthy again (**200**). 📸 **Screenshot D3**
   ```powershell
   curl.exe -s -o NUL -w "after repair: %{http_code}`n" http://localhost:800$n/chunks/$hash
   ```

## E – Benchmark: replication vs erasure coding

**What to write in the report** (fill in the blanks from your result tables)

> **Comparison of replication and erasure coding.** A benchmark script
> (`scripts/workload_benchmark.py`) compares the two redundancy methods on the two workloads of
> the specification: (a) 5,000 small text and JSON files of 1–50 KB, and (b) 10 large binary
> files of 100 MB. For each workload, all files are uploaded to a replication bucket and to an
> erasure-coded bucket, downloaded again and checked with SHA-256. The script measures upload
> and download throughput and latency, and the storage overhead. To measure repair traffic, it
> stops one storage node, waits for the heartbeat detection and the repair job, and records the
> bytes read and written by the repair.
>
> Replication stored __× the original data and erasure coding __×, close to the theoretical 3×
> and 1.5×. For the small files, the overhead on disk is higher (__× and __×), because every copy
> or shard uses at least one 4 KB disk block, and erasure coding splits each small file into 6
> very small shards. After the node failure, repair traffic was __ MB for replication and __ MB
> for erasure coding. To rebuild one lost shard, erasure coding must read 4 shards, so it moves
> more data per lost byte, while replication only copies one replica. In short, erasure coding
> halves the storage cost, while replication is simpler and cheaper to repair, especially for
> small files.

Copy the result table into the report as well.

**Test**

1. The gateways allow 60 requests per minute, which is too few for 5,000 files. Raise the limit
   for the benchmark:
   ```powershell
   $env:GATEWAY_RATE_LIMIT="1000000"; docker compose -f compose.storage.yaml --env-file .env up -d gateway-1 gateway-2
   ```
2. **Workload (a), small files** (about 15–25 minutes). It stops storage-node-2 by itself for the
   repair test and starts it again. 📸 **Screenshot E1:** the table at the end.
   ```powershell
   python -m scripts.workload_benchmark --workload small --failure-test
   ```
3. **Workload (b), large files** (it first creates about 1 GB of test data, and uses about
   4.5 GB of disk in Docker). 📸 **Screenshot E2**
   ```powershell
   python -m scripts.workload_benchmark --workload large --failure-test
   ```
4. Put the normal rate limit back:
   ```powershell
   Remove-Item Env:GATEWAY_RATE_LIMIT; docker compose -f compose.storage.yaml --env-file .env up -d gateway-1 gateway-2
   ```

The tables are also saved in `benchmark_results/` (`.md` and `.json`). Include them in your
submission.

Each run generates **new random files** (the seed is printed at the start and saved in the
table). Runs therefore never share chunks with earlier runs through deduplication, and each run
measures its own storage overhead and repair traffic. To repeat a run with exactly the same
files, add `--seed <number>`.

## F – Problems found while testing (optional paragraph)

> **Problems found and fixed during testing.** Testing the benchmark several times revealed
> three problems, which were fixed. (1) When a file was uploaded again, deduplication avoided
> storing its metadata twice, but the gateway still wrote the chunk again to the nodes the hash
> ring chose at that moment. After a repair had moved a copy, this created a 4th copy (or a
> second set of shards). The gateway now reuses copies that already exist on live nodes and
> only writes the missing ones. (2) The scrub now also detects chunks with more than 3 healthy
> copies, and a rebalance job removes the extra copies, so every chunk has exactly 3 replicas.
> (3) Since the repair work became asynchronous (Task 7), the scheduler re-queued chunks that
> could not be repaired every 10 seconds. It now checks the result of the previous job before
> queuing new work.

---

## Flowchart for the report (failure detection and repair)

The flowchart changed in Part 2, so draw the new one:

```
Storage node stops sending heartbeats
        |
No heartbeat for 15 s  ->  node marked FAILED (metadata service)
        |
Scheduler (every 10 s) sees the failed node -> creates a repair job
        |
One message per affected chunk -> RabbitMQ queue storage.repair
        |
Worker takes a message
   |-- replicated chunk: read a verified replica -> write it to a new node (ring)
   |-- erasure-coded chunk: read 4 healthy shards -> decode -> check SHA-256
   |                         -> re-encode -> write only the missing shards
        |
Update chunk / shard locations in metadata, update job progress
        |
All chunks done -> job COMPLETED (shown in the web UI)
```

## Clean up

```powershell
Remove-Item t4.bin, t4b.bin, out_rep.bin, out_ec.bin
```

---

### Results from the developer test run (for reference)

| Test | Result |
|---|---|
| node5 stopped | marked failed after 15 s; repair job of 257 chunks completed in 17 s, split about half and half between the 2 workers |
| Erasure-coded shard on node5 | shard #1 rebuilt on node6; both files downloaded with the correct SHA-256; 0 degraded objects |
| Erasure-coded upload with node5 down | succeeded (HTTP 201) |
| One byte corrupted on node3 | node returned 409; the next scrub (36 s later) found it, and the copy was deleted and written again in under 1 s (200) |
| Benchmark, 200 small files (`--small-count 200 --failure-test`) | overhead 3.0× (replication) vs 1.5× (erasure); 3.23× vs 2.0× with 4 KB disk blocks; repair traffic 3.53 MB vs 4.41 MB; failure detected after 12.5 s, repair done after 34 s |
| Test suite | `python -m pytest tests -v`: 7 passed |
