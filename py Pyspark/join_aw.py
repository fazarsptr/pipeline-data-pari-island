from pyspark.sql import SparkSession

# Inisialisasi SparkSession
spark = SparkSession.builder \
    .appName("Join PostgreSQL with Master CSV") \
    .getOrCreate()

# Detail koneksi PostgreSQL
jdbc_url = "jdbc:postgresql://172.21.57.199:5432/pentaho_db"
jdbc_properties = {
    "user": "postgres",
    "password": "fazars666",
    "driver": "org.postgresql.Driver"
}

# Load main table dari PostgreSQL
main_table = spark.read.format("jdbc").options(
    url=jdbc_url,
    dbtable="aktivitas_wisata",
    **jdbc_properties
).load()

# Load master table dari file CSV
master_table = spark.read.csv(
    "/home/hadoop/data/master_aktivitas_wisata.csv",
    header=True,
    inferSchema=True
)

# Tampilkan schema untuk validasi
print("Schema Main Table:")
main_table.printSchema()

print("Schema Master Table:")
master_table.printSchema()

# Join kedua tabel menggunakan kolom kunci
joined_table = main_table.join(master_table, on="id_aktivitas", how="inner")

# Tampilkan hasil join untuk validasi
print("Hasil Join:")
joined_table.show(10)

# Simpan hasil join ke PostgreSQL
joined_table.write.format("jdbc").options(
    url=jdbc_url,
    dbtable="aktivitas_wisata_final",
    **jdbc_properties
).mode("overwrite").save()

print("Hasil join telah disimpan ke tabel 'aktivitas_wisata_final' di PostgreSQL.")

# Menghentikan SparkSession
spark.stop()
