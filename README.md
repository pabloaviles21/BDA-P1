# BDA-P1: Large-Scale Data Engineering for AI - Data Science Pipeline

## 📋 Descripción del Proyecto

Este proyecto implementa un **end-to-end Data Science pipeline** siguiendo la arquitectura DataOps con múltiples zonas de datos (Landing Zone, Formatted Zone, Trusted Zone y Exploitation Zone). El objetivo es procesar datos de múltiples fuentes, normalizarlos, validar su calidad e integrarlos para análisis posterior.

## 🏗️ Arquitectura del Proyecto

El proyecto sigue la arquitectura en capas descrita en el enunciado:

```
Data Sources (Kaggle APIs)
    ↓
Landing Zone (datos crudos)
    ↓
Data Formatting Pipeline
    ↓
Formatted Zone (datos normalizados)
    ↓
Data Quality Pipeline
    ↓
Trusted Zone (datos limpios)
    ↓
Exploitation Zone (datos integrados)
    ↓
Data Analysis Pipelines
```


## 📊 Archivos Principales

### 1. **data_collector.py** - Data Collector (Landing Zone)
**Propósito**: Ingesta de datos desde fuentes externas (Kaggle)

**Funcionalidad**:
- Descarga 3 datasets desde Kaggle API
- Organiza datos en `landing_zone/<dataset>/<fecha>/`
- Convierte a formato Parquet (eficiente para procesamiento distribuido)
- Permite ejecuciones periódicas con soporte para fechas específicas

**Datasets**:
- **uber_trips**: Datos de viajes de Uber en NYC 2016
- **weather_nyc**: Datos meteorológicos de NYC 2016
- **accidents_nyc**: Datos de accidentes vehiculares en NYC

**Ejecución**:
```bash
python data_collector.py --date 2026-04-02  # Descarga de una fecha específica
python data_collector.py                     # Usa la fecha actual
```

**Cumplimiento de requisitos**:
- ✅ Data Collector implementado para cada fuente
- ✅ Soporte para ejecuciones periódicas
- ✅ Almacenamiento en Data Lake con estructura de carpetas por fecha
- ✅ Formatos: Parquet (escalable y eficiente)

---

### 2. **The_Formatted_Zone.py** - Data Formatting Pipeline (Formatted Zone)
**Propósito**: Normalización sintáctica de datos y almacenamiento en base de datos relacional

**Funcionalidad**:
- Lee datos Parquet de la Landing Zone
- Aplica transformaciones con Apache Spark:
  - Normalización de nombres de columnas (minúsculas, sin caracteres especiales)
  - Conversión de tipos: fechas, horas, números
  - Estandarización de valores
- Almacena datos en DuckDB mediante tablas relacionales
- Crea una DB común para todos los datasets

**Tecnologías**:
- **Apache Spark**: Procesamiento distribuido (DataFrame API)
- **DuckDB**: Base de datos OLAP embebida para queries analíticos
- **Python**: Orquestación y lógica de negocio

**Transformaciones aplicadas**:
- `uber_data`: tabla con campos de tipos correctos (fecha, enteros)
- `weather_data`: tabla con temperatura y precipitación (tipo double)
- `accidents_data`: tabla con información de accidentes y víctimas

**Ejecución**:
```bash
# Requiere JAVA_HOME configurado
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot"
python The_Formatted_Zone.py --date 2026-04-02  # Fecha específica
python The_Formatted_Zone.py                     # Última fecha disponible
```

**Cumplimiento de requisitos**:
- ✅ Data Formatting Pipeline con Spark/SparkSQL
- ✅ Homogeneización de datos en modelo relacional
- ✅ Una tabla por dataset (uber_data, weather_data, accidents_data)
- ✅ Almacenamiento en base de datos relacional (DuckDB)

---

## 🔧 Configuración y Dependencias

### Variables de Entorno Requeridas
```powershell
# Windows - Necesario para que Spark comunique con Java
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot"
```

### Dependencias Python (`requirements.txt`)
- **pyspark**: Procesamiento distribuido de datos
- **duckdb**: Base de datos SQL embebida
- **pandas**: Manipulación de datos
- **pyarrow**: Serialización de datos (Parquet)
- **kaggle**: API para descargar datasets

### Instalación
```bash
pip install -r requirements.txt
```

---

## 🎯 Cómo el Proyecto Cumple los Requisitos del Enunciado

### ✅ Landing Zone
- **Ubicación**: `landing_zone/<dataset_name>/<date>/`
- **Formato**: Parquet (SequenceFile alternativo, Avro, etc.)
- **Contenido**: Datos crudos descargados sin transformaciones
- **Implementación**: `data_collector.py`

### ✅ Formatted Zone (en desarrollo)
- **Ubicación**: `formatted_zone.db` (tablas DuckDB)
- **Contenido**: Datos homogeneizados según modelo canónico
- **Transformaciones**: 
  - Normalización sintáctica de columnas
  - Conversiones de tipo (fechas, números)
- **Tecnología**: Spark DataFrame + DuckDB
- **Implementación**: `The_Formatted_Zone.py`

### ⏳ Trusted Zone (próximamente)
- **Propósito**: Validar calidad de datos & limpieza
- **Denial Constraints**: Reglas de validación (p.ej., fechas válidas, sin nulos críticos)
- **Implementación**: `Data_Quality_Pipeline.py` (por crear)

### ⏳ Exploitation Zone (próximamente)
- **Propósito**: Integración y reconciliación de datos
- **Datos reconciliados**: Resolución de entidades duplicadas entre datasets
- **Vistas preparadas**: Para análisis específicos

### ⏳ Data Analysis Pipelines (próximamente)
- **Mínimo 2 pipelines**:
  1. Pipeline 1: [Por definir - análisis predictivo/descriptivo]
  2. Pipeline 2: [Por definir - visualización/clustering]
- **Tecnologías**: Scikit-learn, PyTorch, o Spark MLlib

---

## 🚀 Flujo de Ejecución Completo

```bash
# 1. Colectar datos desde Kaggle
python data_collector.py

# 2. Formatear y normalizar en base de datos
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot"
python The_Formatted_Zone.py

# 3. Validar y limpiar datos (próximo)
# python Data_Quality_Pipeline.py

# 4. Integrar datos (próximo)
# python Exploitation_Zone.py

# 5. Análisis de datos (próximo)
# python Analysis_Pipeline_1.py
# python Analysis_Pipeline_2.py
```

---

## 📝 Ejemplos de Uso

```bash
# Descargar datos de una fecha específica
python data_collector.py --date 2026-04-04

# Procesar Formatted Zone con última fecha disponible
python The_Formatted_Zone.py

# Procesar Formatted Zone con fecha específica
python The_Formatted_Zone.py --date 2026-04-04
```

---

## 🔍 Próximos Pasos (Roadmap)

- [ ] **Trusted Zone**: Data Quality Pipeline (validación & limpieza)
- [ ] **Exploitation Zone**: Integración y reconciliación de datos
- [ ] **Data Analysis Pipeline 1**: Modelo predictivo
- [ ] **Data Analysis Pipeline 2**: Análisis descriptivo/visualización
- [ ] Documentación de Denial Constraints
- [ ] Notebooks Jupyter de demostración

---