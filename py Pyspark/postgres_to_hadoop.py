from pyspark.sql import SparkSession

# Inisialisasi SparkSession
spark = SparkSession.builder \
    .appName("tohadoop") \
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
    dbtable="data_final",
    **jdbc_properties
).load()

# Path HDFS file (gunakan URI NameNode)
hdfs_path = "hdfs://localhost:9000/user/hadoop/data_final.csv"

# Pastikan file di HDFS
main_table.write.csv(hdfs_path, mode="overwrite", header=True)

# Membaca file CSV kembali dari HDFS untuk validasi
df = spark.read.csv(hdfs_path, header=True, inferSchema=True)

# Tampilkan isi DataFrame
df.show()
