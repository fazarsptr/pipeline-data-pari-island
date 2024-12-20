from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from airflow.operators.bash import BashOperator

# Default arguments untuk DAG
default_args = {
    'owner': 'Fazar',
    'depends_on_past': False,
    'email': ['fazarsptr13@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

# Definisi DAG
dag = DAG(
    'masking_and_load_data_final',
    default_args=default_args,
    description='Mask nomor telepon, buat tabel, dan insert ke PostgreSQL',
    schedule_interval=None,
    start_date=datetime(2024, 11, 20),
    catchup=False
)

# Fungsi untuk masking nomor telepon
def mask_phone_number(data):
    """
    Mask 4 angka tengah dari nomor telepon.
    Contoh: 081234567890 -> 08****67890
    """
    data["nomor_telepon"] = data["nomor_telepon"].apply(
        lambda x: x[:2] + "*" * 4 + x[-4:] if len(x) > 6 else x
    )
    return data

# Fungsi untuk membaca, memproses, dan menyimpan CSV dengan masking nomor telepon
def load_and_mask_csv(**kwargs):
    input_csv_path = "/home/hadoop/data/data_final.csv"
    output_csv_path = "/home/hadoop/data/data_final_masked.csv"

    # Baca file CSV
    data = pd.read_csv(input_csv_path, dtype={"nomor_telepon": str})

    # Masking nomor telepon
    data = mask_phone_number(data)

    # Simpan hasil ke file baru
    data.to_csv(output_csv_path, index=False)
    print(f"File dengan masking berhasil disimpan di: {output_csv_path}")

    # Simpan data ke XCom untuk digunakan di tugas berikutnya
    kwargs['ti'].xcom_push(key='masked_data', value=data.to_dict(orient='records'))

# Fungsi untuk menyimpan data ke PostgreSQL
def save_to_postgres(**kwargs):
    # Ambil data dari XCom
    masked_data = kwargs['ti'].xcom_pull(key='masked_data', task_ids='mask_csv_task')

    if masked_data:
        # Koneksi ke PostgreSQL
        hook = PostgresHook(postgres_conn_id="postgres_conn")
        connection = hook.get_conn()
        cursor = connection.cursor()

        # Simpan data ke tabel
        for row in masked_data:
            query = """
            INSERT INTO data_final_masked (
                id_reservasi, id_pembayaran, id_wisatawan, nama, nomor_telepon,
                tanggal_reservasi, jumlah_orang, total_biaya, kategori, deskripsi_wisatawan,
                jenis_pembayaran, deskripsi_pembayaran, id_aktivitas, tanggal_wisata,
                nama_aktivitas, deskripsi, durasi, tanggal_ulasan, rating, komentar, preferensi
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(query, (
                row['id_reservasi'], row['id_pembayaran'], row['id_wisatawan'], row['nama'], row['nomor_telepon'],
                row['tanggal_reservasi'], row['jumlah_orang'], row['total_biaya'], row['kategori'], row['deskripsi_wisatawan'],
                row['jenis_pembayaran'], row['deskripsi_pembayaran'], row['id_aktivitas'], row['tanggal_wisata'],
                row['nama_aktivitas'], row['deskripsi'], row['durasi'], row['tanggal_ulasan'], row['rating'], row['komentar'], row['preferensi']
            ))

        # Commit perubahan
        connection.commit()
        cursor.close()
        connection.close()
        print("Data dengan masking berhasil disimpan ke PostgreSQL.")

# Untuk membuat tabel di PostgreSQL
create_table_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_conn',
    sql="""
    CREATE TABLE IF NOT EXISTS data_final_masked (
        id_reservasi VARCHAR(50),
        id_pembayaran VARCHAR(50),
        id_wisatawan VARCHAR(50),
        nama VARCHAR(100),
        nomor_telepon VARCHAR(15),
        tanggal_reservasi VARCHAR(10),
        jumlah_orang INT,
        total_biaya INT,
        kategori VARCHAR(50),
        deskripsi_wisatawan VARCHAR(255),
        jenis_pembayaran VARCHAR(50),
        deskripsi_pembayaran VARCHAR(255),
        id_aktivitas VARCHAR(50),
        tanggal_wisata VARCHAR(10),
        nama_aktivitas VARCHAR(100),
        deskripsi VARCHAR(255),
        durasi VARCHAR(50),
        tanggal_ulasan VARCHAR(10),
        rating INT,
        komentar VARCHAR(255),
        preferensi VARCHAR(100)
    );
    """,
    dag=dag
)

# Untuk melakukan masking data
mask_csv_task = PythonOperator(
    task_id='mask_csv_task',
    python_callable=load_and_mask_csv,
    provide_context=True,
    dag=dag
)

# Untuk menyimpan hasil ke PostgreSQL
save_to_postgres_task = PythonOperator(
    task_id='save_to_postgres_task',
    python_callable=save_to_postgres,
    provide_context=True,
    dag=dag
)

# PYSPARK validasi nomor_telepon
validasi_nomor_telepon = BashOperator(
    task_id = "validasi_nomor_telepon",
    bash_command = "spark-submit --jars /home/hadoop/postgresql-42.2.26.jar /home/hadoop/spark-script/pyspark_val_nt.py",
    dag = dag
)

# PYSPARK validasi total_biaya
validasi_total_biaya = BashOperator(
    task_id = "validasi_total_biaya",
    bash_command = "spark-submit --jars /home/hadoop/postgresql-42.2.26.jar /home/hadoop/spark-script/pyspark_val_tb.py",
    dag = dag
)

# PYSPARK backup data
backup_data = BashOperator(
    task_id = "backup_data",
    bash_command = "spark-submit --jars /home/hadoop/postgresql-42.2.26.jar /home/hadoop/spark-script/pyspark_val_bd.py",
    dag = dag
)

# Urutan eksekusi
create_table_task >> mask_csv_task >> save_to_postgres_task >> validasi_nomor_telepon >> validasi_total_biaya >> backup_data
