from pyspark.sql import SparkSession

# Inisialisasi SparkSession
spark = SparkSession.builder.appName("validasi_total_biaya").getOrCreate()

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
    dbtable="data_final",
    **jdbc_properties
).load()

# Validasi nilai negatif atau NULL di kolom total_biaya
invalid_data = main_table.filter((main_table["total_biaya"] < 0) | (main_table["total_biaya"].isNull()))

# Hitung jumlah data invalid
invalid_count = invalid_data.count()
if invalid_count > 0:
    print(f"Data dengan nilai negatif atau NULL di kolom 'total_biaya' ditemukan sebanyak {invalid_count} baris.")
else:
    print("Tidak ada data dengan nilai negatif atau NULL di kolom 'total_biaya'.")