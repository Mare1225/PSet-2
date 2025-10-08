# PSet-2
## 1. Resumen Ejecutivo y Arquitectura del Pipeline

Este proyecto implementa una solución de *Data Lakehouse* en **Snowflake** para ingestar, transformar y modelar el *dataset* histórico de viajes de taxis **Yellow y Green** (2015-2025). La arquitectura sigue el patrón **Medallion (Bronze/Silver/Gold)**, orquestada por **Mage** y transformada con **dbt**.



### 1.1. Capas del Modelo (Arquitectura de Medallas) 

| Capa | Propósito | Contenido Principal | Modelo dbt |
| :--- | :--- | :--- | :--- |
| **Bronze (Raw)** | Reflejo fiel del origen. | Tablas `ny_taxi_yellow`, `ny_taxi_green` (Parquet) + Metadatos de ingesta.  | Fuente (`source`) |
| **Silver (Staging/Core)** | Limpieza, estandarización y unificación. | Tablas unificadas (`taxi_unification`) y *staging* de dimensiones (`stg_*`).| Modelo (`staging`, `core`) |
| **Gold (Marts)** | Modelo en estrella optimizado para negocio. | **Hecho:** `fct_trips`. **Dimensiones:** `dim_date`, `dim_zones`, `dim_time`, etc. | Modelo (`marts/gold`) |

erDiagram
    dim_date {
      int date_sk PK
      date date_key
      int year
      int month
      string year_month_key
      int day_of_month
      string day_of_week_name
      int day_of_week_iso
    }

    dim_time {
      int time_sk PK
      int hour_of_day
      string time_slot
    }

    dim_zones {
      int zone_sk PK
      string borough
      int zone_id
    }

    dim_vendor {
      int vendor_sk PK
      string vendor_description
    }

    dim_rate_code {
      int rate_code_sk PK
      string rate_code_description
    }

    dim_payment_type {
      int payment_type_sk PK
      string payment_type_description
    }

    dim_trip_type {
      int trip_type_sk PK
      string trip_type_description
    }

    dim_service_type {
      string service_type_sk PK
    }

    fact_trips {
      int pickup_date_sk FK
      int dropoff_date_sk FK
      int pickup_time_sk FK
      int dropoff_time_sk FK
      int pu_zone_sk FK
      int do_zone_sk FK
      int vendor_sk FK
      int rate_code_sk FK
      int payment_type_sk FK
      int trip_type_sk FK
      string service_type_sk FK
      int passenger_count
      float trip_distance
      numeric fare_amount
      numeric extra_charge
      numeric mta_tax
      numeric tip_amount
      numeric tolls_amount
      numeric improvement_surcharge
      numeric congestion_surcharge
      numeric total_amount
      int trip_duration_seconds
    }

    fact_trips }o--|| dim_date : pickup_date_sk
    fact_trips }o--|| dim_date : dropoff_date_sk
    fact_trips }o--|| dim_time : pickup_time_sk
    fact_trips }o--|| dim_time : dropoff_time_sk
    fact_trips }o--|| dim_zones : pu_zone_sk
    fact_trips }o--|| dim_zones : do_zone_sk
    fact_trips }o--|| dim_vendor : vendor_sk
    fact_trips }o--|| dim_rate_code : rate_code_sk
    fact_trips }o--|| dim_payment_type : payment_type_sk
    fact_trips }o--|| dim_trip_type : trip_type_sk
    fact_trips }o--|| dim_service_type : service_type_sk

---

## 2. Ingesta y Cobertura Histórica (Mage) 

### 2.1. Estrategia de Pipeline y Control

* **Orquestación:** **Mage** ejecuta un *pipeline* de *backfill* para la ingesta masiva de archivos Parquet. 
* **Chunking:** La ingesta se realiza mediante **lotes mensuales** (`chunking` por año/mes) para controlar el volumen, gestionar fallos y facilitar los reintentos. 
* **Idempotencia:** La lógica de ingesta garantiza la **idempotencia** al reejecutar un mes, evitando la duplicación de datos en la capa Bronze mediante la gestión de metadatos o la estrategia `MERGE/UPSERT` basada en la ventana temporal del lote. 
* ]**Metadatos:** Se almacenan metadatos por lote, incluyendo `run_id`, `ingest_ts`, `año/mes`, y conteos de filas. 

### 2.2. Matriz de Cobertura (2015-2025 Parquet) 

| Servicio | Rango de Meses | Meses Totales | Meses Faltantes (Brechas) |
| :--- | :--- | :--- | :--- |
| **Yellow Cab** | Ene 2015 - Ago 2025 | 116 | NINGUNO |
| **Green Cab** | Ene 2015 - Ago 2025 | 115 | Mayo 2024 |


---

## 3. Seguridad y Operación de Credenciales 

### 3.1. Gestión de Secrets (Mage) 

Todas las credenciales de conexión a Snowflake se almacenan de forma segura en **Mage Secrets**. El repositorio de código no contiene valores sensibles. Las evidencias se encuentran en la carpeta de evidencias 

| Nombre del Secret | Propósito |
| :--- | :--- |
| `SNOWFLAKE_ACCOUNT` | Identificador de la cuenta de Snowflake. |
| `SNOWFLAKE_USER` | Usuario técnico de menor privilegio. |
| `SNOWFLAKE_PASSWORD` | Contraseña del usuario técnico. |
| `SNOWFLAKE_WAREHOUSE` | Warehouse de cómputo para cargas/transformaciones. |
| `SNOWFLAKE_ROLE` | Rol dedicado con permisos mínimos. |

### 3.2. Cuenta de Servicio con Privilegios Mínimos 

Una captura con los permisos otorgados esta en la carpeta de evidencias.

---

## 4. Clustering en Snowflake (fct_trips) 
Se realizó un experimento en la tabla de hechos más grande, `fct_trips` (capa Gold), para medir el impacto del *clustering* en el rendimiento de las consultas.

### 4.1. Llaves Elegidas y Justificación [cite: 83]

* **Llave de Clustering:** `CLUSTER BY (pickup_date_sk, pu_zone_sk)`
* **Justificación:** Esta combinación optimiza los patrones de consulta más comunes: filtros de rango de fecha y filtros geográficos puntuales/agrupados. Esto promueve la **poda (*pruning*)** de micro-partitions, reduciendo la cantidad de datos escaneados. 

### 4.2. Resultados y Conclusión (Métricas Antes/Después)

| Métrica (Consulta Representativa) | Base (Antes) | Clustering (Después) | Observación Clave |
| :--- | :--- | :--- | :--- |
| **Tiempo de Ejecución** | Compilation: 649ms, Execution: 625ms | Compilation: 127ms, Execution: 250ms | Reducción drástica del tiempo de ejecución. |
| **Particiones Escaneadas** | 32 | 15 | Éxito en el *pruning* de micro-partitions.  |

**Conclusión:** El *clustering* en `(pickup_date_sk, pu_zone_sk)` aporta un valor significativo, mejorando la eficiencia del cómputo. Se eligió esta combinación a largo plazo y se habilitó el *Auto-Clustering* para mantener el orden. Se evitó el **sobreclusterizar** para controlar los costos de mantenimiento. Y el Query que se uso de prueba esta en las evidencias tambien.

---

## 5. Checklist de Aceptación (DM202501-PSet-2)

| Requisito | Estado |
| :--- | :--- |
| [cite_start]Cargados todos los meses 2015-2025 (Parquet) y matriz de cobertura.  | ✅ |
| [cite_start]Mage orquesta *backfill* mensual con idempotencia y metadatos por lote.  | ✅ |
| [cite_start]Arquitectura Medallion: Bronze fiel, Silver unifica/escaliza, Gold en estrella.  | ✅ |
| [cite_start]Clustering aplicado a `fct_trips` con evidencia antes/después (*Query Profile*).  | ✅ |
| [cite_start]Secrets y cuenta de servicio con permisos mínimos.  | ✅ |
| [cite_start]Tests dbt (`not_null`, `unique`, etc.) pasan; docs y *lineage* generados.  | ✅ |
| [cite_start]*Notebook* con respuestas a las 5 preguntas de negocio desde Gold.  | ✅ |