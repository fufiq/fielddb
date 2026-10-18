# Final run with resource metrics (VPS, 2026-10-18)

2 vCPU Xeon E5-2680 v2, ~1.9 GB RAM, Postgres 17.11, Field = fielddb (fixed engine + MVCC layer v3). 8 clients, 60 s measurement after a 10 s warm-up.

> **Note.** Quickly assembled test build of fielddb, a production-style database layer; not a production-hardened release. See the main README.

CPU is in % of one core (maximum 200%). Memory is the peak of the engine's cgroup: heap (anon) / file cache (file; for PG this includes shared_buffers). For PG, CPU = server + pgbench client.

## In RAM: 200k rows, 600 MB limit

| UPDATE | engine | ops/s | UPDATE p50 / p99, µs | read p50 / p99, µs | CPU, % | CPU per op, µs | peak memory, MB (heap / files) | disk read, MB | disk write, MB | iowait, % | steal, % |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 0% | Field | 2 643 264 | — | 0 / 1 | 192 | 0.7 | 130 (130 / 0) | 0 | 1 | 0 | 2 |
| 0% | PG | 6 807 | — | 982 / 4873 | 137 + 53 | 279.1 | 215 (16 / 193) | 0 | 1 | 0 | 2 |
| | **Field / PG** | **×388.3** | | | | | | | | | |
| 20% | Field | 108 895 | 12 / 5877 | 1 / 435 | 98 | 9.0 | 341 (132 / 202) | 3 | 4 621 | 24 | 13 |
| 20% | PG | 3 360 | 2509 / 25195 | 1256 / 11511 | 147 + 34 | 541.1 | 242 (37 / 210) | 0 | 114 | 0 | 4 |
| | **Field / PG** | **×32.4** | | | | | | | | | |
| 80% | Field | 28 467 | 9 / 5897 | 2 / 1464 | 80 | 28.5 | 475 (131 / 336) | 147 | 2 896 | 32 | 11 |
| 80% | PG | 2 123 | 2757 / 18462 | 1893 / 12897 | 165 + 22 | 882.5 | 257 (38 / 211) | 0 | 141 | 0 | 3 |
| | **Field / PG** | **×13.4** | | | | | | | | | |
| 100% | Field | 18 905 | 17 / 7207 | — | 79 | 41.6 | 421 (131 / 281) | 68 | 6 331 | 28 | 12 |
| 100% | PG | 1 831 | 3185 / 18494 | — | 169 + 20 | 1031.9 | 256 (38 / 213) | 0 | 125 | 0 | 3 |
| | **Field / PG** | **×10.3** | | | | | | | | | |

## Out of RAM: 1.5M rows, 600 MB limit

| UPDATE | engine | ops/s | UPDATE p50 / p99, µs | read p50 / p99, µs | CPU, % | CPU per op, µs | peak memory, MB (heap / files) | disk read, MB | disk write, MB | iowait, % | steal, % |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 0% | Field | 1 222 452 | — | 1 / 2 | 186 | 1.5 | 589 (149 / 436) | 105 | 1 | 2 | 2 |
| 0% | PG | 4 348 | — | 1261 / 10060 | 132 + 40 | 396.2 | 600 (16 / 579) | 1 280 | 1 | 6 | 6 |
| | **Field / PG** | **×281.2** | | | | | | | | | |
| 20% | Field | 2 189 | 3855 / 36526 | 616 / 11884 | 51 | 234.1 | 600 (150 / 438) | 540 | 488 | 47 | 8 |
| 20% | PG | 1 357 | 12961 / 42417 | 2745 / 16663 | 137 + 17 | 1134.1 | 600 (22 / 566) | 1 705 | 1 571 | 13 | 6 |
| | **Field / PG** | **×1.6** | | | | | | | | | |
| 80% | Field | 1 617 | 3916 / 28491 | 947 / 10384 | 78 | 483.9 | 600 (150 / 436) | 903 | 1 064 | 39 | 9 |
| 80% | PG | 560 | 14620 / 63671 | 4913 / 25790 | 147 + 8 | 2796.1 | 600 (23 / 568) | 1 709 | 2 478 | 10 | 6 |
| | **Field / PG** | **×2.9** | | | | | | | | | |
| 100% | Field | 1 428 | 3745 / 27151 | — | 79 | 546.0 | 600 (150 / 436) | 859 | 1 032 | 39 | 9 |
| 100% | PG | 501 | 13418 / 43236 | — | 153 + 7 | 3210.6 | 600 (22 / 567) | 1 988 | 2 744 | 8 | 5 |
| | **Field / PG** | **×2.9** | | | | | | | | | |

## Out of RAM, reads: 256 MB limit

| UPDATE | engine | ops/s | UPDATE p50 / p99, µs | read p50 / p99, µs | CPU, % | CPU per op, µs | peak memory, MB (heap / files) | disk read, MB | disk write, MB | iowait, % | steal, % |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 0% | Field | 9 618 | — | 551 / 5490 | 98 | 102.3 | 256 (149 / 104) | 2 313 | 1 | 36 | 8 |
| 0% | PG | 2 890 | — | 2158 / 12962 | 123 + 32 | 534.0 | 256 (15 / 236) | 1 524 | 1 | 14 | 6 |
| | **Field / PG** | **×3.3** | | | | | | | | | |

