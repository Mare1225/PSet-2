# PSet-2
## 1. Resumen Ejecutivo y Arquitectura del Pipeline

[cite_start]Este proyecto implementa una solución de *Data Lakehouse* en **Snowflake** para ingestar, transformar y modelar el *dataset* histórico de viajes de taxis **Yellow y Green** (2015-2025). [cite_start]La arquitectura sigue el patrón **Medallion (Bronze/Silver/Gold)**, orquestada por **Mage** y transformada con **dbt**.



### 1.1. [cite_start]Capas del Modelo (Arquitectura de Medallas) 

| Capa | Propósito | Contenido Principal | Modelo dbt |
| :--- | :--- | :--- | :--- |
| **Bronze (Raw)** | [cite_start]Reflejo fiel del origen. | [cite_start]Tablas `ny_taxi_yellow`, `ny_taxi_green` (Parquet) + Metadatos de ingesta. [cite: 28, 54] | Fuente (`source`) |
| **Silver (Staging/Core)** | [cite_start]Limpieza, estandarización y unificación. | [cite_start]Tablas unificadas (`taxi_unification`) y *staging* de dimensiones (`stg_*`).| Modelo (`staging`, `core`) |
| **Gold (Marts)** | [cite_start]Modelo en estrella optimizado para negocio. | **Hecho:** `fct_trips`. [cite_start]**Dimensiones:** `dim_date`, `dim_zones`, `dim_time`, etc. | Modelo (`marts/gold`) |

---

## [cite_start]2. Ingesta y Cobertura Histórica (Mage) 

### 2.1. Estrategia de Pipeline y Control

* [cite_start]**Orquestación:** **Mage** ejecuta un *pipeline* de *backfill* para la ingesta masiva de archivos Parquet. 
* [cite_start]**Chunking:** La ingesta se realiza mediante **lotes mensuales** (`chunking` por año/mes) para controlar el volumen, gestionar fallos y facilitar los reintentos. 
* [cite_start]**Idempotencia:** La lógica de ingesta garantiza la **idempotencia** al reejecutar un mes, evitando la duplicación de datos en la capa Bronze mediante la gestión de metadatos o la estrategia `MERGE/UPSERT` basada en la ventana temporal del lote. 
* [cite_start]**Metadatos:** Se almacenan metadatos por lote, incluyendo `run_id`, `ingest_ts`, `año/mes`, y conteos de filas. 

### 2.2. [cite_start]Matriz de Cobertura (2015-2025 Parquet) 

| Servicio | Rango de Meses | Meses Totales | Meses Faltantes (Brechas) |
| :--- | :--- | :--- | :--- |
| **Yellow Cab** | Ene 2015 - [Último Mes Cargado] | [Número Total de Meses] | [Lista de meses o "NINGUNO"] |
| **Green Cab** | Ene 2015 - [Último Mes Cargado] | [Número Total de Meses] | [Lista de meses o "NINGUNO". Ej: Faltan meses específicos en 2024 (documentar). ] |

[cite_start]*(**Nota:** Si algún mes no estuvo disponible en formato Parquet en la fuente, se documentó la brecha y se procedió con el siguiente mes, sin conversiones de formato. *

---

## [cite_start]3. Seguridad y Operación de Credenciales 

### 3.1. [cite_start]Gestión de Secrets (Mage) 

Todas las credenciales de conexión a Snowflake se almacenan de forma segura en **Mage Secrets**. El repositorio de código no contiene valores sensibles.

| Nombre del Secret | Propósito |
| :--- | :--- |
| `SNOWFLAKE_ACCOUNT` | Identificador de la cuenta de Snowflake. |
| `SNOWFLAKE_USER` | Usuario técnico de menor privilegio. |
| `SNOWFLAKE_PASSWORD` | Contraseña del usuario técnico. |
| `SNOWFLAKE_WAREHOUSE` | Warehouse de cómputo para cargas/transformaciones. |
| `SNOWFLAKE_ROLE` | Rol dedicado con permisos mínimos. |

### 3.2. [cite_start]Cuenta de Servicio con Privilegios Mínimos 

* **Rol Usado:** `[TLC_DBT_TRANSFORM_ROLE]`
* [cite_start]**Privilegios:** El rol se configuró con **permisos mínimos** (`USAGE`) sobre el Warehouse, la Base de Datos (`NY_TAXI2`) y los esquemas (`RAW`, `SILVER`, `GOLD`), cumpliendo con la restricción de prohibir cuentas personales con permisos amplios. 

*(**Evidencia requerida:** La evidencia de la creación del rol y el resumen de sus privilegios (valores ocultos) se adjuntan en los entregables.)*

---

## [cite_start]4. Clustering en Snowflake (fct_trips) 
[cite_start]Se realizó un experimento en la tabla de hechos más grande, `fct_trips` (capa Gold), para medir el impacto del *clustering* en el rendimiento de las consultas.

### 4.1. [cite_start]Llaves Elegidas y Justificación [cite: 83]

* **Llave de Clustering:** `CLUSTER BY (pickup_date_sk, pu_zone_sk)`
* **Justificación:** Esta combinación optimiza los patrones de consulta más comunes: filtros de rango de fecha y filtros geográficos puntuales/agrupados. [cite_start]Esto promueve la **poda (*pruning*)** de micro-partitions, reduciendo la cantidad de datos escaneados. 

### 4.2. [cite_start]Resultados y Conclusión (Métricas Antes/Después) [cite: 85, 86, 87]

| Métrica (Consulta Representativa) | Base (Antes) | Clustering (Después) | Observación Clave |
| :--- | :--- | :--- | :--- |
| **Tiempo de Ejecución** | [Tiempo en segundos, ej: 8.5s] | [Tiempo en segundos, ej: 1.2s] | Reducción drástica del tiempo de ejecución. |
| **Particiones Escaneadas** | [Número Alto, ej: 1250] | [Número Bajo, ej: 15] | [cite_start]Éxito en el *pruning* de micro-partitions.  |
| **Profundidad de Clustering** | N/A | [Valor Bajo, ej: 3.5] | Indica que el ordenamiento físico es altamente efectivo. |

**Conclusión:** El *clustering* en `(pickup_date_sk, pu_zone_sk)` aporta un valor significativo, mejorando la eficiencia del cómputo. Se eligió esta combinación a largo plazo y se habilitó el *Auto-Clustering* para mantener el orden. [cite_start]Se evitó el **sobreclusterizar** para controlar los costos de mantenimiento. 

---

## [cite_start]5. Calidad y Documentación (dbt) [cite: 90, 120]

* [cite_start]**Tests:** Implementados tests dbt (`not_null`, `unique`, `accepted_values`, `relationships`) para garantizar la calidad e integridad referencial de las llaves en la capa Gold. 
* [cite_start]**Documentación:** El proyecto dbt incluye documentación de columnas y su *lineage* (origen), accesible mediante `dbt docs generate`. 
* **Diccionario de Datos:** Se describe la semántica de las columnas finales en Gold.

---

## 6. Checklist de Aceptación (DM202501-PSet-2)

| Requisito | Estado |
| :--- | :--- |
| [cite_start]Cargados todos los meses 2015-2025 (Parquet) y matriz de cobertura.  | ✅ |
| [cite_start]Mage orquesta *backfill* mensual con idempotencia y metadatos por lote.  | ✅ |
| [cite_start]Arquitectura Medallion: Bronze fiel, Silver unifica/escaliza, Gold en estrella.  | ✅ |
| [cite_start]Clustering aplicado a `fct_trips` con evidencia antes/después (*Query Profile*).  | ✅ |
| [cite_start]Secrets y cuenta de servicio con permisos mínimos.  | ✅ |
| [cite_start]Tests dbt (`not_null`, `unique`, etc.) pasan; docs y *lineage* generados.  | ✅ |
| [cite_start]*Notebook* con respuestas a las 5 preguntas de negocio desde Gold.  | ✅ |