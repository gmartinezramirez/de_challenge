# Benchmark_q2

## q2_memory

Running q2-memory...
python src/q2_memory.py
2024-07-05 12:18:31,576 - INFO - Starting: q2_memory
2024-07-05 12:18:31,576 - INFO - Ejecutando query
2024-07-05 12:18:34,660 - INFO - Tiempo de ejecución de la query (lado del cliente, python, hasta recibir la respuesta): 3.08 segundos
2024-07-05 12:18:34,661 - INFO - Se uso query cache?: False
2024-07-05 12:18:34,661 - INFO - Bigquery Job Detail:
2024-07-05 12:18:34,661 - INFO - Job ID: 15935c9f-b8b7-4449-8ec6-fb4c62974b08
2024-07-05 12:18:34,661 - INFO - Job Status: DONE
2024-07-05 12:18:34,661 - INFO - Tiempo inicio en máquina GCP: 2024-07-05 16:18:32.665000+00:00
2024-07-05 12:18:34,661 - INFO - Tiempo fin en máquina GCP: 2024-07-05 16:18:33.826000+00:00
2024-07-05 12:18:34,661 - INFO - Tiempo de ejecución total en servidor GCP: 1.161 segundos
2024-07-05 12:18:34,661 - INFO - Tiempo de ejecución total en cliente (python): 3.08 segundos
2024-07-05 12:18:34,662 - INFO - Delta de tiempo (costo de red, serialización, SO, etc.): 1.92 segundos
2024-07-05 12:18:34,662 - INFO - Bytes procesados: 21869484
2024-07-05 12:18:34,662 - INFO - Bytes facturados: 22020096
2024-07-05 12:18:34,663 - INFO - Slot machine miliseconds: 55207
2024-07-05 12:18:34,663 - INFO - Uso estimado de memoria en base a total bytes procesados: 20.86 MB
2024-07-05 12:18:34,663 - INFO - Query Plan:
2024-07-05 12:18:34,663 - INFO -   Step S00: Input:
2024-07-05 12:18:34,663 - INFO -     - Records read: 117407
2024-07-05 12:18:34,663 - INFO -     - Records written: 419
2024-07-05 12:18:34,663 - INFO -     - Status: COMPLETE
2024-07-05 12:18:34,663 - INFO -   Step S01: Aggregate:
2024-07-05 12:18:34,663 - INFO -     - Records read: 419
2024-07-05 12:18:34,663 - INFO -     - Records written: 85
2024-07-05 12:18:34,663 - INFO -     - Status: COMPLETE
2024-07-05 12:18:34,663 - INFO -   Step S02: Aggregate+:
2024-07-05 12:18:34,663 - INFO -     - Records read: 85
2024-07-05 12:18:34,663 - INFO -     - Records written: 1
2024-07-05 12:18:34,663 - INFO -     - Status: COMPLETE
2024-07-05 12:18:34,663 - INFO -   Step S03: Aggregate:
2024-07-05 12:18:34,663 - INFO -     - Records read: 1
2024-07-05 12:18:34,663 - INFO -     - Records written: 1
2024-07-05 12:18:34,664 - INFO -     - Status: COMPLETE
2024-07-05 12:18:34,664 - INFO -   Step S04: Sort+:
2024-07-05 12:18:34,664 - INFO -     - Records read: 86
2024-07-05 12:18:34,664 - INFO -     - Records written: 85
2024-07-05 12:18:34,664 - INFO -     - Status: COMPLETE
2024-07-05 12:18:34,664 - INFO -   Step S05: Aggregate:
2024-07-05 12:18:34,664 - INFO -     - Records read: 85
2024-07-05 12:18:34,664 - INFO -     - Records written: 82
2024-07-05 12:18:34,664 - INFO -     - Status: COMPLETE
2024-07-05 12:18:34,664 - INFO -   Step S06: Aggregate+:
2024-07-05 12:18:34,664 - INFO -     - Records read: 82
2024-07-05 12:18:34,664 - INFO -     - Records written: 35
2024-07-05 12:18:34,664 - INFO -     - Status: COMPLETE
2024-07-05 12:18:34,664 - INFO -   Step S08: Coalesce:
2024-07-05 12:18:34,664 - INFO -     - Records read: 35
2024-07-05 12:18:34,664 - INFO -     - Records written: 35
2024-07-05 12:18:34,664 - INFO -     - Status: COMPLETE
2024-07-05 12:18:34,664 - INFO -   Step S09: Join+:
2024-07-05 12:18:34,664 - INFO -     - Records read: 2115
2024-07-05 12:18:34,664 - INFO -     - Records written: 10
2024-07-05 12:18:34,665 - INFO -     - Status: COMPLETE
2024-07-05 12:18:34,665 - INFO -   Step S0A: Output:
2024-07-05 12:18:34,665 - INFO -     - Records read: 10
2024-07-05 12:18:34,665 - INFO -     - Records written: 10
2024-07-05 12:18:34,665 - INFO -     - Status: COMPLETE
2024-07-05 12:18:34,982 - INFO - Successful finish: q2_memory
[('✊', 1724), ('❤', 1471), ('\u200d', 418), ('✌', 202), ('☮', 170), ('♂', 139), ('♀', 113), ('✍', 91), ('♥', 56), ('⚔', 48)]

## q2_time

Running q2-time...
python src/q2_time.py
2024-07-05 12:19:03,472 - INFO - Starting: q2_time
2024-07-05 12:19:03,472 - INFO - Ejecutando query
2024-07-05 12:19:05,063 - INFO - Tiempo de ejecución de la query (lado del cliente, python, hasta recibir la respuesta): 1.59 segundos
2024-07-05 12:19:05,063 - INFO - Se uso query cache?: False
2024-07-05 12:19:05,063 - INFO - Bigquery Job Detail:
2024-07-05 12:19:05,063 - INFO - Job ID: 831e049d-586d-4db2-aa12-ca32bf2d234e
2024-07-05 12:19:05,063 - INFO - Job Status: DONE
2024-07-05 12:19:05,063 - INFO - Tiempo inicio en máquina GCP: 2024-07-05 16:19:03.940000+00:00
2024-07-05 12:19:05,063 - INFO - Tiempo fin en máquina GCP: 2024-07-05 16:19:04.410000+00:00
2024-07-05 12:19:05,064 - INFO - Tiempo de ejecución total en servidor GCP: 0.47 segundos
2024-07-05 12:19:05,064 - INFO - Tiempo de ejecución total en cliente (python): 1.59 segundos
2024-07-05 12:19:05,064 - INFO - Delta de tiempo (costo de red, serialización, SO, etc.): 1.12 segundos
2024-07-05 12:19:05,064 - INFO - Bytes procesados: 21869484
2024-07-05 12:19:05,064 - INFO - Bytes facturados: 22020096
2024-07-05 12:19:05,064 - INFO - Slot machine miliseconds: 921
2024-07-05 12:19:05,064 - INFO - Uso estimado de memoria en base a total bytes procesados: 20.86 MB
2024-07-05 12:19:05,064 - INFO - Query Plan:
2024-07-05 12:19:05,064 - INFO -   Step S00: Input:
2024-07-05 12:19:05,064 - INFO -     - Records read: 117407
2024-07-05 12:19:05,064 - INFO -     - Records written: 419
2024-07-05 12:19:05,064 - INFO -     - Status: COMPLETE
2024-07-05 12:19:05,064 - INFO -   Step S01: Sort+:
2024-07-05 12:19:05,064 - INFO -     - Records read: 419
2024-07-05 12:19:05,064 - INFO -     - Records written: 10
2024-07-05 12:19:05,064 - INFO -     - Status: COMPLETE
2024-07-05 12:19:05,064 - INFO -   Step S02: Output:
2024-07-05 12:19:05,064 - INFO -     - Records read: 10
2024-07-05 12:19:05,064 - INFO -     - Records written: 10
2024-07-05 12:19:05,064 - INFO -     - Status: COMPLETE
2024-07-05 12:19:05,448 - INFO - Successful finish: q2_time
[('✊', 1724), ('❤', 1471), ('\u200d', 418), ('✌', 202), ('☮', 170), ('♂', 139), ('♀', 113), ('✍', 91), ('♥', 56), ('⚔', 48)]
