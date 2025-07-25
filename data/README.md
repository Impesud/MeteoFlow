# Documentazione dei Dati

Questo documento descrive i formati e i contenuti di quattro tipologie di dati forniti: **dati meteo**, **regioni**,
**province** e **città**. Ogni tipo di dati è strutturato in un formato specifico e contiene informazioni dettagliate
come descritto di seguito.

---

## 1. Dati Meteo

### Formato

- **Formato file**: JSON (o JSONL) compresso in formato GZ.
- **Descrizione**: I dati contengono informazioni geografiche e condizioni meteorologiche orarie. Ogni file rappresenta
  un set di dati meteo con struttura JSON. Nel caso di file in formato JSONL, la struttura si applica alle singole righe.

### Struttura del File

| Nome          | Tipo    | Descrizione                                        | Esempio       |
|---------------|---------|----------------------------------------------------|---------------|
| `latitude`    | float   | Latitudine del luogo osservato.                    | 45.3593736    |
| `longitude`   | float   | Longitudine del luogo osservato.                   | 11.7886941    |
| `timezone`    | string  | Fuso orario della località, in formato IANA.       | "Europe/Rome" |
| `offset`      | float   | Differenza oraria rispetto all'orario UTC, in ore. | 2.0           |
| `elevation`   | integer | Altitudine del luogo osservato, in metri.          | 0             |
| `hourly.data` | array   | Lista di dati orari (vedere dettagli sotto).       |               |

#### Dettagli per `hourly.data`

| Nome              | Tipo      | Descrizione                                                            | Esempio       |
|-------------------|-----------|------------------------------------------------------------------------|---------------|
| `time`            | timestamp | Data e ora in formato Unix timestamp (secondi dal 1 gennaio 1970 UTC). | 1717192800    |
| `icon`            | string    | Icona che rappresenta le condizioni atmosferiche.                      | "clear-night" |
| `summary`         | string    | Descrizione testuale delle condizioni atmosferiche.                    | "Clear"       |
| `precipIntensity` | float     | Intensità della precipitazione, in mm/ora.                             | 0.077         |
| `temperature`     | float     | Temperatura in gradi Celsius.                                          | 11.85         |

---

## 2. Dati Regioni

### Formato

- **Formato file**: JSONL (JSON Lines).
- **Descrizione**: Ogni riga del file rappresenta un'istanza di dati relativi a una regione, comprensiva di nome, codice
- ISTAT e confini geografici.

### Struttura del File

| Nome                | Tipo   | Descrizione                                                                                    | Esempio                                                         |
|---------------------|--------|------------------------------------------------------------------------------------------------|-----------------------------------------------------------------|
| `region_name`       | string | Nome della regione.                                                                            | "Abruzzo"                                                       |
| `region_istat`      | string | Codice ISTAT identificativo della regione.                                                     | "13"                                                            |
| `region_boundaries` | string | Confini geografici della regione rappresentati come poligono in formato WKT (Well-Known Text). | "POLYGON((13.1901198 42.4011934, ... , 13.1901198 42.4011934))" |

---

## 3. Dati Province

### Formato

- **Formato file**: JSONL (JSON Lines).
- **Descrizione**: Ogni riga rappresenta un'istanza di dati relativi a una provincia, comprensiva di nome, codice ISTAT
- e confini geografici.

### Struttura del File

| Nome                 | Tipo   | Descrizione                                                                         | Esempio                                                                        |
|----------------------|--------|-------------------------------------------------------------------------------------|--------------------------------------------------------------------------------|
| `province_name`      | string | Nome ufficiale della provincia.                                                     | "Agrigento"                                                                    |
| `province_istat_code`| string | Codice ISTAT numerico identificativo della provincia.                               | "084"                                                                          |
| `province_boundaries`| string | Confini geografici della provincia rappresentati come multipoligono in formato WKT. | "MULTIPOLYGON(((12.5282991 35.5290215, ...)), ((12.8802587 35.8723229, ...)))" |

---

## 4. Elenco delle Città

### Formato

- **Formato file**: CSV.
- **Descrizione**: Contiene informazioni sui comuni italiani, comprese denominazioni, codici ISTAT, coordinate
- geografiche e superfici.

### Struttura del File

| Nome Colonna              | Tipo       | Descrizione                                           | Esempio          |
|---------------------------|------------|-------------------------------------------------------|------------------|
| `sigla_provincia`         | string     | Sigla della provincia (es. codice di due lettere).    | "TO"             |
| `codice_istat`            | string     | Codice ISTAT univoco per il comune.                   | "001001"         |
| `denominazione_ita`       | string     | Denominazione ufficiale del comune in italiano.       | "Agliè"          |
| `lat`                     | float      | Latitudine del centro geografico del comune.          | 45.3634669       |
| `lon`                     | float      | Longitudine del centro geografico del comune.         | 7.7686057        |
| `superficie_kmq`          | float      | Superficie totale del comune, in chilometri quadrati. | 13.2851          |

### Dettagli Tecnici

- **Separatore**: Punto e virgola (`;`).
- **Decimali**: Virgola (`,`) per i valori numerici.
- **Quote**: I valori contenenti spazi o caratteri speciali sono racchiusi tra virgolette doppie (`"`).
