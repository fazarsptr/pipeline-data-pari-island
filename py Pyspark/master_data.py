import pandas as pd

# Creating Master Data for Profil Wisatawan
master_profil_wisatawan = [
    {"id_wisatawan": "IDW1", "kategori": "Keluarga", "deskripsi": "Wisatawan bersama keluarga"},
    {"id_wisatawan": "IDW2", "kategori": "Grup Pelajar", "deskripsi": "Wisatawan dalam rombongan pelajar"},
    {"id_wisatawan": "IDW3", "kategori": "Corporate Group", "deskripsi": "Wisatawan dalam rombongan perusahaan"},
    {"id_wisatawan": "IDW4", "kategori": "Community Group", "deskripsi": "Wisatawan dalam rombongan komunitas"},
    {"id_wisatawan": "IDW5", "kategori": "Solo Traveler", "deskripsi": "Wisatawan yang bepergian sendiri"}
]

# Creating Master Data for Aktivitas Wisata
master_aktivitas_wisata = [
    {"id_aktivitas": "IDA1", "nama_aktivitas": "Snorkeling", "deskripsi": "Menyelam di perairan jernih Pulau Pari", "durasi": "2 Jam"},
    {"id_aktivitas": "IDA2", "nama_aktivitas": "Camping", "deskripsi": "Berkemah di kawasan alam Pulau Pari", "durasi": "12 Jam"},
    {"id_aktivitas": "IDA3", "nama_aktivitas": "Bersepeda", "deskripsi": "Bersepeda keliling Pulau Pari", "durasi": "3 Jam"},
    {"id_aktivitas": "IDA4", "nama_aktivitas": "BBQ", "deskripsi": "Menikmati makan malam BBQ di pantai", "durasi": "6 Jam"},
    {"id_aktivitas": "IDA5", "nama_aktivitas": "Tur Mangrove", "deskripsi": "Menjelajahi hutan mangrove Pulau Pari", "durasi": "1 Jam"}
]

# Creating Master Data for Jenis Pembayaran
master_jenis_pembayaran = [
    {"id_pembayaran": "IDP1", "jenis_pembayaran": "Transfer Bank", "deskripsi": "Pembayaran via bank transfer"},
    {"id_pembayaran": "IDP2", "jenis_pembayaran": "E-Wallet", "deskripsi": "Pembayaran menggunakan e-wallet"},
    {"id_pembayaran": "IDP3", "jenis_pembayaran": "Tunai", "deskripsi": "Pembayaran tunai di lokasi"}
]

# Convert to DataFrames
profil_wisatawan_df = pd.DataFrame(master_profil_wisatawan)
aktivitas_wisata_df = pd.DataFrame(master_aktivitas_wisata)
jenis_pembayaran_df = pd.DataFrame(master_jenis_pembayaran)

# Save Master Data to CSV
profil_wisatawan_file = "/home/hadoop/spark-script/master_profil_wisatawan.csv"
aktivitas_wisata_file = "/home/hadoop/spark-script/master_aktivitas_wisata.csv"
jenis_pembayaran_file = "/home/hadoop/spark-script/master_jenis_pembayaran.csv"

profil_wisatawan_df.to_csv(profil_wisatawan_file, index=False)
aktivitas_wisata_df.to_csv(aktivitas_wisata_file, index=False)
jenis_pembayaran_df.to_csv(jenis_pembayaran_file, index=False)

{
    "Master Profil Wisatawan": profil_wisatawan_file,
    "Master Aktivitas Wisata": aktivitas_wisata_file,
    "Master Jenis Pembayaran": jenis_pembayaran_file
}

