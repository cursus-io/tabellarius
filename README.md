# Tabellarius
Change Data Capture Source

<br>

## Quick Start
1. Start MySQL and Inspect CDC State: `docker compose up mysql cdc-cli`
   - output when CDC metadata is not initialized: `[MISSING] cdc_log table`

2. Initialize metadata
   ```
   docker compose run --rm cdc-cli \
     cdc-cli \
     --mode=init \
     --apply \
     --config=/app/cdc-config.yaml \
     --dsn=root:root@tcp(mysql:3306)/mydb
    ```

3. Re-run Inspect (Safe to re-execute)

   ```
   docker compose run --rm cdc-cli \
     cdc-cli \
     --mode=inspect \
     --config=/app/cdc-config.yaml \
     --dsn=root:root@tcp(mysql:3306)/mydb
   ```

4. Start the Server: `docker compose up cdc-server`

## Docker Images

Docker images are published to GitHub Container Registry:

```bash
docker pull ghcr.io/cursus-io/tabellarius:latest
docker run --rm ghcr.io/cursus-io/tabellarius:latest cdc-server --help
```

Versioned release images are available with release tags:

```bash
docker pull ghcr.io/cursus-io/tabellarius:<version>
```
