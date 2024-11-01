# Simple-Kafka-Project

| Nama | NRP |
| ---------------------- | ---------- |
| Azzahra Sekar Rahmadina | 5027221035 |
| Stephanie Hebrina Mabunbun Simatupang  | 5027221069 |

Proyek ini adalah contoh sederhana untuk menjalankan Apache Kafka. Proyek ini mencakup konfigurasi Kafka dan Zookeeper untuk menjalankan instance Kafka yang dapat diakses lokal untuk kebutuhan pengembangan.

## Menjalankan Kafka

## Kode Python untuk Producer dan Consumer

Pastikan telah menginstal kafka-python sebelum melanjutkan.
- Menginstal Kafka Python

  ```bash
  pip install kafka-python
  ```

- Membuat topik sensor-suhu
  ```bash
  .\kafka-topics.bat --create --topic sensor-suhu --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
  ```
  
### Menggunakan kode yang sudah disiapkan 
- Jalankan producer di terminal baru:
  ```bash
  python producer.py
  ```
- Jalankan consumer di terminal baru:
  ```bash
  python consumer.py
   ```

## Hasil
