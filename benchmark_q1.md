# Benchmarks

## Q1

### Q1 - Memory

Running q1-memory...
python src/q1_memory.py
2024-07-05 11:40:54,811 - INFO - Starting: q1_memory
2024-07-05 11:40:54,811 - INFO - Ejecutando query
2024-07-05 11:40:56,861 - INFO - Tiempo de ejecución de la query (lado del cliente, python, hasta recibir la respuesta): 2.05 segundos
2024-07-05 11:40:56,862 - INFO - Se uso query cache?: False
2024-07-05 11:40:56,862 - INFO - Bigquery Job Detail:
2024-07-05 11:40:56,862 - INFO - Job ID: f441d545-1683-4d95-a1ba-8d69c28c9977
2024-07-05 11:40:56,862 - INFO - Job Status: DONE
2024-07-05 11:40:56,862 - INFO - Tiempo inicio en máquina GCP: 2024-07-05 15:40:55.301000+00:00
2024-07-05 11:40:56,862 - INFO - Tiempo fin en máquina GCP: 2024-07-05 15:40:56.129000+00:00
2024-07-05 11:40:56,862 - INFO - Tiempo de ejecución total en servidor GCP: 0.828 segundos
2024-07-05 11:40:56,862 - INFO - Tiempo de ejecución total en cliente (python): 2.05 segundos
2024-07-05 11:40:56,862 - INFO - Delta de tiempo (costo de red, serialización, SO, etc.): 1.22 segundos
2024-07-05 11:40:56,862 - INFO - Bytes procesados: 2569405
2024-07-05 11:40:56,863 - INFO - Bytes facturados: 10485760
2024-07-05 11:40:56,863 - INFO - Slot machine miliseconds: 21054
2024-07-05 11:40:56,863 - INFO - Uso estimado de memoria en base a total bytes procesados: 2.45 MB
2024-07-05 11:40:56,863 - INFO - Query Plan:
2024-07-05 11:40:56,863 - INFO -   Step S00: Input:
2024-07-05 11:40:56,863 - INFO -     - Records read: 117407
2024-07-05 11:40:56,863 - INFO -     - Records written: 13
2024-07-05 11:40:56,863 - INFO -     - Status: COMPLETE
2024-07-05 11:40:56,863 - INFO -   Step S01: Sort+:
2024-07-05 11:40:56,863 - INFO -     - Records read: 13
2024-07-05 11:40:56,863 - INFO -     - Records written: 10
2024-07-05 11:40:56,863 - INFO -     - Status: COMPLETE
2024-07-05 11:40:56,863 - INFO -   Step S02: Sort:
2024-07-05 11:40:56,863 - INFO -     - Records read: 10
2024-07-05 11:40:56,863 - INFO -     - Records written: 10
2024-07-05 11:40:56,863 - INFO -     - Status: COMPLETE
2024-07-05 11:40:56,864 - INFO -   Step S03: Coalesce:
2024-07-05 11:40:56,864 - INFO -     - Records read: 10
2024-07-05 11:40:56,864 - INFO -     - Records written: 10
2024-07-05 11:40:56,864 - INFO -     - Status: COMPLETE
2024-07-05 11:40:56,864 - INFO -   Step S04: Join+:
2024-07-05 11:40:56,864 - INFO -     - Records read: 117537
2024-07-05 11:40:56,864 - INFO -     - Records written: 44159
2024-07-05 11:40:56,864 - INFO -     - Status: COMPLETE
2024-07-05 11:40:56,864 - INFO -   Step S06: Sort:
2024-07-05 11:40:56,864 - INFO -     - Records read: 10
2024-07-05 11:40:56,864 - INFO -     - Records written: 10
2024-07-05 11:40:56,864 - INFO -     - Status: COMPLETE
2024-07-05 11:40:56,864 - INFO -   Step S07: Coalesce:
2024-07-05 11:40:56,864 - INFO -     - Records read: 10
2024-07-05 11:40:56,864 - INFO -     - Records written: 10
2024-07-05 11:40:56,864 - INFO -     - Status: COMPLETE
2024-07-05 11:40:56,864 - INFO -   Step S08: Join+:
2024-07-05 11:40:56,864 - INFO -     - Records read: 44169
2024-07-05 11:40:56,864 - INFO -     - Records written: 10
2024-07-05 11:40:56,864 - INFO -     - Status: COMPLETE
2024-07-05 11:40:56,864 - INFO -   Step S09: Aggregate+:
2024-07-05 11:40:56,864 - INFO -     - Records read: 10
2024-07-05 11:40:56,864 - INFO -     - Records written: 10
2024-07-05 11:40:56,864 - INFO -     - Status: COMPLETE
2024-07-05 11:40:56,864 - INFO -   Step S0A: Output:
2024-07-05 11:40:56,865 - INFO -     - Records read: 10
2024-07-05 11:40:56,865 - INFO -     - Records written: 10
2024-07-05 11:40:56,865 - INFO -     - Status: COMPLETE
2024-07-05 11:40:57,245 - INFO - Successful finish: q1_memory
[(datetime.date(2021, 2, 12), 'RanbirS00614606'), (datetime.date(2021, 2, 13), 'MaanDee08215437'), (datetime.date(2021, 2, 17), 'RaaJVinderkaur'), (datetime.date(2021, 2, 16), 'jot__b'), (datetime.date(2021, 2, 14), 'rebelpacifist'), (datetime.date(2021, 2, 18), 'neetuanjle_nitu'), (datetime.date(2021, 2, 15), 'jot__b'), (datetime.date(2021, 2, 20), 'MangalJ23056160'), (datetime.date(2021, 2, 23), 'Surrypuria'), (datetime.date(2021, 2, 19), 'Preetm91')]

### Q1 - Time

Running q1-time...
python src/q1_time.py
2024-07-05 11:41:43,479 - INFO - Starting: q1_time
2024-07-05 11:41:43,479 - INFO - Ejecutando query
2024-07-05 11:41:45,175 - INFO - Tiempo de ejecución de la query (lado del cliente, python, hasta recibir la respuesta): 1.70 segundos
2024-07-05 11:41:45,175 - INFO - Se uso query cache?: False
2024-07-05 11:41:45,175 - INFO - Bigquery Job Detail:
2024-07-05 11:41:45,175 - INFO - Job ID: 6835e765-ecac-4ede-b47a-3237d45cc1e6
2024-07-05 11:41:45,175 - INFO - Job Status: DONE
2024-07-05 11:41:45,175 - INFO - Tiempo inicio en máquina GCP: 2024-07-05 15:41:43.993000+00:00
2024-07-05 11:41:45,175 - INFO - Tiempo fin en máquina GCP: 2024-07-05 15:41:44.583000+00:00
2024-07-05 11:41:45,175 - INFO - Tiempo de ejecución total en servidor GCP: 0.59 segundos
2024-07-05 11:41:45,175 - INFO - Tiempo de ejecución total en cliente (python): 1.70 segundos
2024-07-05 11:41:45,175 - INFO - Delta de tiempo (costo de red, serialización, SO, etc.): 1.11 segundos
2024-07-05 11:41:45,175 - INFO - Bytes procesados: 2569405
2024-07-05 11:41:45,175 - INFO - Bytes facturados: 10485760
2024-07-05 11:41:45,175 - INFO - Slot machine miliseconds: 1472
2024-07-05 11:41:45,175 - INFO - Uso estimado de memoria en base a total bytes procesados: 2.45 MB
2024-07-05 11:41:45,175 - INFO - Query Plan:
2024-07-05 11:41:45,175 - INFO -   Step S00: Input:
2024-07-05 11:41:45,175 - INFO -     - Records read: 117407
2024-07-05 11:41:45,175 - INFO -     - Records written: 51646
2024-07-05 11:41:45,175 - INFO -     - Status: COMPLETE
2024-07-05 11:41:45,175 - INFO -   Step S01: Aggregate+:
2024-07-05 11:41:45,175 - INFO -     - Records read: 51646
2024-07-05 11:41:45,175 - INFO -     - Records written: 13
2024-07-05 11:41:45,175 - INFO -     - Status: COMPLETE
2024-07-05 11:41:45,175 - INFO -   Step S02: Sort+:
2024-07-05 11:41:45,175 - INFO -     - Records read: 13
2024-07-05 11:41:45,175 - INFO -     - Records written: 10
2024-07-05 11:41:45,176 - INFO -     - Status: COMPLETE
2024-07-05 11:41:45,176 - INFO -   Step S03: Sort+:
2024-07-05 11:41:45,176 - INFO -     - Records read: 10
2024-07-05 11:41:45,176 - INFO -     - Records written: 10
2024-07-05 11:41:45,176 - INFO -     - Status: COMPLETE
2024-07-05 11:41:45,176 - INFO -   Step S04: Output:
2024-07-05 11:41:45,176 - INFO -     - Records read: 10
2024-07-05 11:41:45,176 - INFO -     - Records written: 10
2024-07-05 11:41:45,176 - INFO -     - Status: COMPLETE
2024-07-05 11:41:45,655 - INFO - Successful finish: q1_time
[(datetime.date(2021, 2, 12), 'RanbirS00614606'), (datetime.date(2021, 2, 13), 'MaanDee08215437'), (datetime.date(2021, 2, 17), 'RaaJVinderkaur'), (datetime.date(2021, 2, 16), 'jot__b'), (datetime.date(2021, 2, 14), 'rebelpacifist'), (datetime.date(2021, 2, 18), 'neetuanjle_nitu'), (datetime.date(2021, 2, 15), 'jot__b'), (datetime.date(2021, 2, 20), 'MangalJ23056160'), (datetime.date(2021, 2, 23), 'Surrypuria'), (datetime.date(2021, 2, 19), 'Preetm91')]