# Benchmark q3

## q3_memory

Running q3-memory...
python src/q3_memory.py
2024-07-05 11:59:52,670 - INFO - Starting: q3_memory
2024-07-05 11:59:52,670 - INFO - Ejecutando query
2024-07-05 11:59:54,996 - INFO - Tiempo de ejecución de la query (lado del cliente, python, hasta recibir la respuesta): 2.33 segundos
2024-07-05 11:59:54,997 - INFO - Se uso query cache?: False
2024-07-05 11:59:54,997 - INFO - Bigquery Job Detail:
2024-07-05 11:59:54,997 - INFO - Job ID: 31eefceb-4f36-4175-ba3f-3e9d326d167a
2024-07-05 11:59:54,997 - INFO - Job Status: DONE
2024-07-05 11:59:54,997 - INFO - Tiempo inicio en máquina GCP: 2024-07-05 15:59:53.456000+00:00
2024-07-05 11:59:54,997 - INFO - Tiempo fin en máquina GCP: 2024-07-05 15:59:53.936000+00:00
2024-07-05 11:59:54,998 - INFO - Tiempo de ejecución total en servidor GCP: 0.48 segundos
2024-07-05 11:59:54,998 - INFO - Tiempo de ejecución total en cliente (python): 2.33 segundos
2024-07-05 11:59:54,998 - INFO - Delta de tiempo (costo de red, serialización, SO, etc.): 1.85 segundos
2024-07-05 11:59:54,998 - INFO - Bytes procesados: 7504281
2024-07-05 11:59:54,998 - INFO - Bytes facturados: 10485760
2024-07-05 11:59:54,998 - INFO - Slot machine miliseconds: 616
2024-07-05 11:59:54,998 - INFO - Uso estimado de memoria en base a total bytes procesados: 7.16 MB
2024-07-05 11:59:54,998 - INFO - Query Plan:
2024-07-05 11:59:54,998 - INFO -   Step S00: Input:
2024-07-05 11:59:54,998 - INFO -     - Records read: 117407
2024-07-05 11:59:54,998 - INFO -     - Records written: 31260
2024-07-05 11:59:54,998 - INFO -     - Status: COMPLETE
2024-07-05 11:59:54,998 - INFO -   Step S01: Sort+:
2024-07-05 11:59:54,998 - INFO -     - Records read: 31260
2024-07-05 11:59:54,998 - INFO -     - Records written: 10
2024-07-05 11:59:54,998 - INFO -     - Status: COMPLETE
2024-07-05 11:59:54,998 - INFO -   Step S02: Output:
2024-07-05 11:59:54,998 - INFO -     - Records read: 10
2024-07-05 11:59:54,998 - INFO -     - Records written: 10
2024-07-05 11:59:54,998 - INFO -     - Status: COMPLETE
2024-07-05 11:59:55,405 - INFO - Sucessful finish: q3_memory
[('narendramodi', 2265), ('Kisanektamorcha', 1840), ('RakeshTikaitBKU', 1644), ('PMOIndia', 1427), ('RahulGandhi', 1146), ('GretaThunberg', 1048), ('RaviSinghKA', 1019), ('rihanna', 986), ('UNHumanRights', 962), ('meenaharris', 926)]

## q3_time

Running q3-time...
python src/q3_time.py
2024-07-05 12:00:48,100 - INFO - Starting: q3_time
2024-07-05 12:00:48,100 - INFO - Ejecutando query
2024-07-05 12:00:49,583 - INFO - Tiempo de ejecución de la query (lado del cliente, python, hasta recibir la respuesta): 1.48 segundos
2024-07-05 12:00:49,583 - INFO - Se uso query cache?: False
2024-07-05 12:00:49,583 - INFO - Bigquery Job Detail:
2024-07-05 12:00:49,583 - INFO - Job ID: 3eaaaa58-0eb9-48e1-a176-6bb05e1d0a5e
2024-07-05 12:00:49,583 - INFO - Job Status: DONE
2024-07-05 12:00:49,583 - INFO - Tiempo inicio en máquina GCP: 2024-07-05 16:00:48.597000+00:00
2024-07-05 12:00:49,583 - INFO - Tiempo fin en máquina GCP: 2024-07-05 16:00:49.112000+00:00
2024-07-05 12:00:49,584 - INFO - Tiempo de ejecución total en servidor GCP: 0.515 segundos
2024-07-05 12:00:49,584 - INFO - Tiempo de ejecución total en cliente (python): 1.48 segundos
2024-07-05 12:00:49,584 - INFO - Delta de tiempo (costo de red, serialización, SO, etc.): 0.97 segundos
2024-07-05 12:00:49,584 - INFO - Bytes procesados: 1328457
2024-07-05 12:00:49,584 - INFO - Bytes facturados: 10485760
2024-07-05 12:00:49,584 - INFO - Slot machine miliseconds: 681
2024-07-05 12:00:49,584 - INFO - Uso estimado de memoria en base a total bytes procesados: 1.27 MB
2024-07-05 12:00:49,584 - INFO - Query Plan:
2024-07-05 12:00:49,584 - INFO -   Step S00: Input:
2024-07-05 12:00:49,584 - INFO -     - Records read: 117407
2024-07-05 12:00:49,584 - INFO -     - Records written: 31260
2024-07-05 12:00:49,584 - INFO -     - Status: COMPLETE
2024-07-05 12:00:49,584 - INFO -   Step S01: Sort+:
2024-07-05 12:00:49,584 - INFO -     - Records read: 31260
2024-07-05 12:00:49,584 - INFO -     - Records written: 10
2024-07-05 12:00:49,584 - INFO -     - Status: COMPLETE
2024-07-05 12:00:49,584 - INFO -   Step S02: Output:
2024-07-05 12:00:49,584 - INFO -     - Records read: 10
2024-07-05 12:00:49,584 - INFO -     - Records written: 10
2024-07-05 12:00:49,584 - INFO -     - Status: COMPLETE
2024-07-05 12:00:50,084 - INFO - Successful finish: q3_time
[('narendramodi', 2265), ('Kisanektamorcha', 1840), ('RakeshTikaitBKU', 1644), ('PMOIndia', 1427), ('RahulGandhi', 1146), ('GretaThunberg', 1048), ('RaviSinghKA', 1019), ('rihanna', 986), ('UNHumanRights', 962), ('meenaharris', 926)]
