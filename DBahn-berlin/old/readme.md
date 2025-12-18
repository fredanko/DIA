# DBahn-Berlin Data — Weekly Archives

This repo contains the Deutsche Bahn (DB) information of the Berlin's S-Bahn road network.
The information is contained into weekly `.tar.gz` archives representing hourly timetables and 15-minutes sampled timetable_chages:
* `timetables/` → hourly snapshots (`YYMMDDHHMM`, e.g. `2509021200`)
* `timetable_changes/` → 15-minute snapshots (`YYMMDDHHMM`, minutes ∈ {00,15,30,45})

**Weekly containers:** The files are contained into fixed 7-day windows `.tar.gz` files anchored at the earliest snapshot date. Each **file name**: `YYMMDD_YYMMDD.tar.gz`, represents the start and end of the week with the second date exclusive. Each `.tar.gz` the information for the timetables that fall in that 7-day window.

### On-disk layout (archives)
```bash
.
├─ timetables/
│  ├─ 250902_250909.tar.gz
│  ├─ 250909_250916.tar.gz
│  └─ …
└─ timetable_changes/
   ├─ 250902_250909.tar.gz
   ├─ 250909_250916.tar.gz
   └─ …
```

### Inside an archive (example)
```bash
250902_250909.tar.gz
├─ 2509021200/   # 2025-09-02 12:00
├─ 2509021300/   # 2025-09-02 13:00
└─ …             # up to, but not including, 2025-09-09 00:00
```


For `timetable_changes/`, folders are every 15 minutes (e.g., `2509021215/`, `2509021230/`, `2509021245/`).