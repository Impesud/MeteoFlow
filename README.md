# MeteoFlow

**Piattaforma ETL containerizzata per dati meteorologici orari**\
Estrae, carica e aggrega dati meteo orari in PostgreSQL/PostGIS, orchestrando tutto con Apache Airflow.

---

## 📂 Struttura del repository

```
.
├── db/
│   └── init.sql              # DDL tabelle + abilitazione PostGIS
├── dags/
│   ├── meteo_etl_dag.py      # Definizione DAG Airflow
│   └── sql/
│       └── post_load.sql     # SQL post‑elaborazione
├── data/                     # Dati di input/output
│   ├── meteo-jsonl/          # File grezzi raw: meteo (JSON/GZ)
│   ├── cities.csv            # città (CSV)
│   ├── provinces.jsonl       # province (JSONL)
│   ├── regions.jsonl         # regioni (JSONL)
│   └── output/          # File elaborati: aggregazioni (CSV/JSON)
├── src/
│   └── scripts/
│       ├── extract.py
│       ├── bulk_copy.py
│       ├── load_dimensions.py
│       ├── aggregate_sql.py
│       └── utils/data_utils.py
├── tests/
│   ├── conftest.py           # Setup DB per pytest
│   ├── test_extract.py
│   ├── test_data_utils.py
│   └── test_aggregate_sql.py
├── .env.example              # Template variabili ambiente
├── .gitignore
├── Dockerfile                # Custom Airflow image
├── docker-compose.yml        # Servizi: postgres, airflow
├── Makefile                  # Alias: docker‑up, lint, test…
├── pyproject.toml            # Configurazione Black, Flake8, Mypy, Bandit
├── .pre-commit-config.yaml   # Hook pre‑commit
└── README.md                 # (questo file)
```

---

## ⚙️ Prerequisiti

- **Docker** ≥ 20.10
- **Docker Compose** ≥ 1.29
- (facoltativo) `make` su host per alias comodi

---

## 🚀 Installazione & Avvio

1. **Clona** il repository:

   ```bash
   git clone git@github.com:Impesud/MeteoFlow.git
   cd MeteoFlow
   ```

2. **Rinomina** il file delle variabili d’ambiente:

   ```bash
   Rinomina .env.example a .env
   ```

3. **Imposta** i permessi per entrypoint.sh, logs/ e data/output:

   ```bash
   chmod +x entrypoint.sh
   chmod 777 logs
   chmod 777 data/output
   ```

4. **Avvia i container**:

   ```bash
   docker-compose up --build
   ```

   - **Postgres+PostGIS** sarà disponibile su `localhost:5432`
   - **Airflow Web UI** su `http://localhost:8080` (user/password: `meteoflow/meteoflow`)

5. **Verifica e Avvio DAG**

   - In Airflow UI il DAG meteo_etl_dag dovrebbe comparire.
   - Il database deve contenere lo schema creato da `db/init.sql`.
   - Lanciare manualmente l'intero DAG dal pulsante "Trigger DAG" presente nell'interfaccia di Airflow UI.
   - In alternativa, dal container si può lanciare il comando:
    ```bash
   airflow dags trigger meteo_etl_dag —conf
    ```

---

## Avvio manuale

### 1️⃣ Avvia i container (Postgres+PostGIS e Airflow)
docker-compose up -d --build

### 2️⃣ Entra nel container Airflow
docker-compose exec airflow bash

### 3️⃣ Carica lo schema iniziale con init.sql
psql -h postgres -U meteo_user -d meteo_db \
  -f /docker-entrypoint-initdb.d/init.sql

### 4️⃣ Estrai e prepara i CSV orari
python /opt/airflow/src/scripts/extract.py

### 5️⃣ Carica le dimensioni (cities, provinces, regions)
python /opt/airflow/src/scripts/load_dimensions.py

### 6️⃣ Applica post_load.sql (geom e region_istat)
psql -h postgres -U meteo_user -d meteo_db \
  -f /opt/airflow/dags/sql/post_load.sql

### 7️⃣ (Opzionale) Verifica staging table e vincolo UNIQUE
psql -h postgres -U meteo_user -d meteo_db -c "\d weather_city_hourly_tmp"
psql -h postgres -U meteo_user -d meteo_db -c "\d weather_city_hourly"

### 8️⃣ Bulk‐load + UPSERT in weather_city_hourly
python /opt/airflow/src/scripts/bulk_copy.py

### 9️⃣ Aggregazioni per scope
python /opt/airflow/src/scripts/aggregate_sql.py --scope city
python /opt/airflow/src/scripts/aggregate_sql.py --scope prov
python /opt/airflow/src/scripts/aggregate_sql.py --scope reg

### 🔟 Verifica aggregazioni giornaliere città (esempio)
psql -h postgres -U meteo_user -d meteo_db \
  -c "SELECT date, COUNT(*) FROM weather_city_daily GROUP BY date ORDER BY date LIMIT 5;"

### 1️⃣1️⃣ Esci dal container
exit

---

## 🛠️ Comandi utili (Makefile)

```bash
make docker-up       # docker-compose up -d --build
make docker-down     # docker-compose down
make lint            # black --check, isort --check, flake8
make types           # mypy src/ tests/
make security        # bandit -r src/
make test            # pytest
make all             # esegue tutti i comandi di controllo codice
```

> Per entrare nella shell del container Airflow:
>
> ```bash
> docker-compose exec airflow bash
> ```

> Per accedere al database Postgres dal container:
>
> ```bash
> docker-compose exec postgres psql -U meteo_user -d meteo_db
> ```

---

## 🔄 Flusso ETL

1. **db/init.sql**
   - Crea lo schema del database: tabelle, indici, vincoli.
   - Abilita l’estensione PostGIS (`CREATE EXTENSION IF NOT EXISTS postgis;`).
   - Definisce le tabelle di dimensione (`cities`, `provinces`, `regions`) e la staging table `weather_city_hourly_tmp`.
   - Aggiunge il vincolo UNIQUE `(istat_code, datetime)` su `weather_city_hourly` per supportare l’upsert.

2. **Airflow DAG** (`dags/meteo_etl_dag.py`)
   - Orchestrazione e scheduling dei task ETL.
   - **extract.py**  
     - Legge file JSONL / `.jsonl.gz`.
     - Usa `orjson.loads` per parsing veloce e gestisce malformazioni **row‑by‑row** con logging di errori.  
     - Normalizza i record (`clean_dataframe`, `pad_istat_code`) e scrive in batch molto grandi per minimizzare I/O.  
   - **load_dimensions.py**  
     - Popola le tabelle di lookup:  
       - `cities` da CSV (codice ISTAT, lat/lon, nome).  
       - `provinces` da JSON (codice ISTAT, confini).  
       - `regions` da JSON (codice ISTAT, confini).
   - **dags/sql/post_load.sql**  
     - Trasforma i confini testuali (`…_boundaries` WKT) in vere geometrie PostGIS (`geom MULTIPOLYGON`). 
     - Aggiunge la colonna `province_istat` in `cities` e la popola con uno spatial‑join.
     - Aggiunge la colonna `region_istat` in `cities` e la popola con uno spatial‑join.
   - **bulk_copy.py**  
     - Costruisce un CSV temporaneo con solo le colonne necessarie (`HOURLY_COLUMNS`).  
     - **Bulk‑loading** via `COPY … STDIN` in staging table `weather_city_hourly_tmp`.  
     - **Upsert** in `weather_city_hourly` con `INSERT … ON CONFLICT DO NOTHING`.  
     - Pulisce la staging table (`TRUNCATE`) per il run successivo.
   - **aggregate_sql.py**  
     - Genera dinamicamente query `INSERT … SELECT` basate su `AGG_CONFIG`.  
     - Esegue aggregazioni **daily**, **weekly**, **monthly** per **city**, **prov**, **reg** in parallelo.

---

## 🧪 Testing

Esegui i primi test dentro al container Airflow:

```bash
docker-compose exec airflow pytest --maxfail=1 --disable-warnings -q
```

---

## 🧪 Perché questo ETL e quali alternative per un take‑home test

Per un **take‑home test** ho scelto un’architettura che:

1. **“End‑to‑end” in un unico ambiente containerizzato**  
   - **Docker Compose** (Postgres+PostGIS + Airflow): zero setup esterno, riproducibilità immediata con `docker-compose up --build`.  
   - Tutto il codice, gli script e le configurazioni vivono nel repository, nessuna dipendenza nascosta.

2. **Semplicità operativa e scalabilità real‑world**  
   - **Python + Pandas + orjson**: parsing rapido di JSONL/GZ, batch size configurabile per processare decine di milioni di righe senza OOM.  
   - **COPY … STDIN + staging table + UPSERT**: evita migliaia di singole INSERT, garantisce idempotenza e performance native di Postgres.

3. **Basso attrito e massima visibilità**  
   - **PostgreSQL + PostGIS**: supporto out‑of‑the‑box per dati spaziali e constraint di unicità (`istat_code, datetime`).  
   - **Apache Airflow**: orchestrazione, retry automatici, logging centralizzato, task grouping, e monitoraggio via UI.

4. **Qualità del codice e operatività**  
   - **Logging strutturato** in ogni script (info, warning, error), report su record malformati e metriche di avanzamento (batch flush, totali).  
   - **Controlli statici e CI**:
     - **Black** & **isort** per formattazione;
     - **Flake8** per linting;
     - **Mypy** per static typing;
     - **Bandit** per security scan;
     - **pytest** con fixture DB docker‑based per unit/integration test.

---

### Alternative valutate

- **Spark + Parquet**  
  - **Pro**: Parquet è molto efficiente in lettura e compressione, ideale per dataset molto grandi.  
  - **Contro**: aggiunge complessità (cluster Spark, configurazione HDFS/S3), e per il volume di dati di questo test un file CSV singolo in locale è sufficiente e più semplice da gestire.  
  - **Quando**: ho adottato Parquet in progetti AI MLOps e AI FinOps in produzione, dove la scalabilità e l’I/O throughput giustificano l’overhead.

- **Kafka + streaming**  
  - **Pro**: consente ingestione real‑time e processing a bassa latenza.  
  - **Contro**: richiede broker, topic management, consumer groups — overkill per un home‑test batch.  
  - **Quando**: usato in un progetto di previsioni E‑commerce AI in produzione per pipeline streaming.

- **Materialized Views in Postgres**  
  - **Pro**: mantengono aggregazioni pre‑calcolate e indicizzate dentro il database.  
  - **Contro**: servono per scenari di produzione con carichi continui; per un proof‑of‑concept preferibile generare tabelle dedicate via Airflow.

- **CSV vs Parquet**  
  - Ho scelto CSV perché:
    - è nativamente supportato dal comando `COPY` di Postgres senza dipendenze aggiuntive;
    - è immediatamente leggibile e debuggabile con strumenti di linea di comando;
    - il test richiede una soluzione end‑to‑end semplice da avviare via Docker Compose;
  - Parquet rimane un’ottima scelta per dataset > 100 GB o in presenza di query analitiche intensi, ma qui il dataset orario compresso sta sotto soglia e CSV riduce il time‑to‑value.

---

### Possibili estensioni

- **Partitioning** delle tabelle orarie (range su `datetime`) per scansioni più veloci.
- **Parametrizzazione DAG**: aggiungere sensori sui file, alert su fallimenti e SLA.
- **Data quality** con Great Expectations per validare record in ingresso. Applicato su progetti AI MLOps e AI FinOps in staging e produzione.
- **Caching/API layer** (FastAPI + Redis) per servire i dati aggregati in <100 ms. Applicato su progetti AI MLOps e AI FinOps in staging e produzione.

Questa architettura bilancia rapidità di sviluppo, facilità di review e solidità per dataset medio‑grandi: ideale per un take‑home test. 

---

# Validazione End-to-End SQL

## 1. Esecuzione manuale

1. Entra nel container Airflow:

   ```bash
   docker-compose exec airflow bash
   ```

2. Da lì lancia psql sul service Postgres:

   ```bash
   psql -h postgres -U meteo_user -d meteo_db
   ```
   Pass: meteo_pass

3. Ora puoi caricare i tuoi SQL, ad esempio:

   ```bash
   \i /opt/airflow/tests/sql/count_city_daily.sql
   ```

   - Se la query termina senza errori e restituisce il numero di righe atteso, significa che l’aggregazione giornaliera per città esiste.

## 2. Panoramica delle query di validazione

| File                                   | Scopo                                                                  |
|----------------------------------------|------------------------------------------------------------------------|
| **count_city_daily.sql**               | Verifica aggregazioni giornaliere per città                            |
| **check_null_keys.sql**                | Controlla che non ci siano chiavi ISTAT nulle                          |
| **top_precipitation_cities.sql**       | Elenca le 5 città con più precipitazione nell’ultimo giorno            |
| **spatial_join_cities_regions.sql**    | Assicura che ogni città abbia una regione assegnata                    |
| **check_province_geometry.sql**        | Verifica che tutte le province abbiano la geometria `geom`            |
| **check_region_geometry.sql**          | Verifica che tutte le regioni abbiano la geometria `geom`              |
| **rolling_avg_temp_city.sql**          | Calcola media mobile a 7 giorni di temperatura per una città           |
| **weekly_precip_change_province.sql**  | Confronta precipitazione tra ultime due settimane e calcola % change   |
| **check_hourly_counts.sql**            | Verifica che ogni giorno ci siano esattamente 24 record orari          |
| **temp_trend_correlation_city.sql**    | Calcola coefficiente di correlazione tra data (epoch) e temperatura    |
| **temp_anomaly_detection_city.sql**    | Rileva anomalie termiche con Z‑score > 2 su finestra mobile di 30 giorni |    


## 📜 Licenza

MIT © Erick Jara
