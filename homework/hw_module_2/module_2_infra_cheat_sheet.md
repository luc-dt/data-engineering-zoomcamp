# 📝 Module 2: Infrastructure & Workflow Cheat Sheet

This cheat sheet consolidates practical commands, configurations, and best practices for managing **WSL2, Docker, Kestra, and PostgreSQL** when working with large datasets in a local Data Engineering environment.

---

## 🛠️ WSL & Docker Storage Management

Essential for handling large datasets without exhausting the system drive.

- **Check WSL Distributions**
  ```bash
  wsl --list --verbose
  ```

- **Export / Backup Docker Data**
  ```bash
  wsl --export docker-desktop-data D:\backup.tar
  ```

- **Import to High-Capacity Drive**
  ```bash
  wsl --import docker-desktop-data D:\DockerWSL D:\backup.tar
  ```

- **Unregister (Delete) Existing Distribution**
  ```bash
  wsl --unregister docker-desktop-data
  ```

- **Enable Sparse VHD (Recommended)**
  Ensure the following is present in your `.wslconfig` file to reduce disk usage growth:
  ```ini
  [wsl2]
  sparseVhd=true
  ```

---

## 🧹 System Cleanup Commands

Run the following commands in **PowerShell (Administrator)** to reclaim disk space on **C:** and **D:**.

- **Docker System Prune (Containers, Images, Volumes)**
  ```bash
  docker system prune -a --volumes
  ```

- **Docker Builder Cache Cleanup**
  ```bash
  docker builder prune -a
  ```

- **Clear Windows Prefetch**
  ```powershell
  Remove-Item -Path "$env:WINDIR\Prefetch\*" -Force -Recurse -ErrorAction SilentlyContinue
  ```

- **Windows Update Component Cleanup**
  ```bash
  dism.exe /online /cleanup-image /startcomponentcleanup
  ```

---

## 🐘 PostgreSQL & SQL Verification

Use these queries in **pgAdmin** (or psql) to validate data completeness and integrity.

```sql
-- Count rows for a specific year
SELECT COUNT(*)
FROM public.yellow_tripdata
WHERE filename LIKE 'yellow_tripdata_2020%';

-- Sanity check for duplicates per file
SELECT filename, COUNT(*)
FROM public.green_tripdata
GROUP BY filename;
```

---

## 🏗️ Kestra Orchestration Snippets

Key patterns for stable and production-ready workflows.

### Purge Temporary Execution Files
Prevents local storage bloat by removing CSVs after pipeline execution.

```yaml
- id: purge_files
  type: io.kestra.plugin.core.storage.PurgeCurrentExecutionFiles
```

### Timezone-Aware Scheduling
Ensures deterministic scheduling aligned with business or regional requirements.

```yaml
timezone: America/New_York
```

---

## 💡 Junior Data Engineer Career Tips (2026)

- **Drive Isolation**: Reserve **C:** for OS and applications; dedicate **D:** / **H:** for Docker volumes and datasets.
- **Infrastructure as Code**: Document WSL migrations and Docker storage decisions—this signals operational maturity.
- **Preventive Maintenance**: Schedule weekly Docker pruning to keep your development environment performant.

---

## ✅ Summary

This cheat sheet serves as a reusable operational playbook for spinning up, maintaining, and validating local Data Engineering environments under real-world data volumes.

