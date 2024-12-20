from pyspark.sql import SparkSession

# Inisialisasi SparkSession
spark = SparkSession.builder.appName("backup_data_postgres").getOrCreate()

# Detail koneksi PostgreSQL
jdbc_url = "jdbc:postgresql://172.21.57.199:5432/pentaho_db"
jdbc_properties = {
    "user": "postgres",
    "password": "fazars666",
    "driver": "org.postgresql.Driver"
}

# Load data dari tabel PostgreSQL
main_table = spark.read.format("jdbc").options(
    url=jdbc_url,
    dbtable="data_final",
    **jdbc_properties
).load()

# Path lokal untuk menyimpan file backup
output_path = "/home/hadoop/data/backup_data_final.csv"

# Simpan data ke path lokal sebagai CSV
main_table.write.csv(output_path, header=True, mode="overwrite")

print(f"Backup data berhasil disimpan di {output_path}.")