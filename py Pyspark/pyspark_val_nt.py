from pyspark.sql import SparkSession

# Inisialisasi SparkSession
spark = SparkSession.builder.appName("validasi_nomor_telepon").getOrCreate()

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

# Validasi nilai NULL di kolom nomor_telepon
invalid_data = main_table.filter(main_table["nomor_telepon"].isNull())

# Hitung jumlah data invalid
invalid_count = invalid_data.count()

if invalid_count > 0:
    print(f"Data dengan nilai NULL di kolom 'nomor_telepon' ditemukan sebanyak {invalid_count} baris.")
    
    # # Simpan hasil validasi ke HDFS
    # output_path = "hdfs://127.0.0.1:9866/user/hadoop/invalid_nomor_telepon.csv"
    # invalid_data.write.csv(output_path, header=True)
    # print(f"Data invalid telah disimpan di {output_path}.")
else:
    print("Tidak ada data dengan nilai NULL di kolom 'nomor_telepon'.")
