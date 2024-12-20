from pyspark.sql import SparkSession

# Inisialisasi SparkSession
spark = SparkSession.builder \
    .appName("Join Ketiga Tabel dengan id_reservasi") \
    .getOrCreate()

# Detail koneksi PostgreSQL
jdbc_url = "jdbc:postgresql://172.21.57.199:5432/pentaho_db"
jdbc_properties = {
    "user": "postgres",
    "password": "fazars666",
    "driver": "org.postgresql.Driver"
}

# Load tabel dari PostgreSQL
reservasi_wisata = spark.read.format("jdbc").options(
    url=jdbc_url,
    dbtable="reservasi_wisata_final",
    **jdbc_properties
).load()

aktivitas_wisata = spark.read.format("jdbc").options(
    url=jdbc_url,
    dbtable="aktivitas_wisata_final",
    **jdbc_properties
).load()

ulasan_preferensi = spark.read.format("jdbc").options(
    url=jdbc_url,
    dbtable="ulasan_preferensi",
    **jdbc_properties
).load()

# Rename kolom untuk menghindari konflik
reservasi_wisata = reservasi_wisata.withColumnRenamed("deskripsi", "deskripsi_wisatawan")
ulasan_preferensi = ulasan_preferensi.withColumnRenamed("deskripsi", "deskripsi_ulasan")

# Lakukan Join menggunakan kolom id_reservasi
join_reservasi_aktivitas = reservasi_wisata.join(aktivitas_wisata, on="id_reservasi", how="inner")
final_join = join_reservasi_aktivitas.join(ulasan_preferensi, on="id_reservasi", how="inner")

# Tampilkan hasil join untuk validasi
final_join.show(10)

# Simpan hasil join ke PostgreSQL sebagai tabel baru
final_join.write.format("jdbc").options(
    url=jdbc_url,
    dbtable="data_final",
    **jdbc_properties
).mode("overwrite").save()

print("Hasil join ketiga tabel telah disimpan ke tabel 'data_final' di PostgreSQL.")

# Menghentikan SparkSession
spark.stop()
