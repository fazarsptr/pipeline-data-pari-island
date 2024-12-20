import pandas as pd
import random
from faker import Faker
from datetime import timedelta

# Inisialisasi Faker
fake = Faker()

# Fungsi untuk menghasilkan nomor telepon
def generate_phone_number():
    return "08" + "".join([str(random.randint(0, 9)) for _ in range(9)])

# Fungsi untuk menghasilkan tanggal yang konsisten
def generate_consistent_dates():
    start_date = fake.date_this_year()
    end_date = start_date + timedelta(days=random.randint(1, 5))
    review_date = end_date + timedelta(days=random.randint(1, 3))
    return {
        "Tanggal_Reservasi": start_date.strftime("%d/%m/%y"),
        "Tanggal_Aktivitas": end_date.strftime("%d/%m/%y"),
        "Tanggal_Ulasan": review_date.strftime("%d/%m/%y")
    }

# Fungsi untuk komentar berdasarkan rating
def generate_rating_comment(rating):
    if rating == 5:
        return random.choice(["Pengalaman tak terlupakan", "Luar biasa", "Sangat memuaskan"])
    elif rating == 4:
        return random.choice(["Sangat baik", "Nyaris sempurna", "Pengalaman menyenangkan"])
    elif rating == 3:
        return random.choice(["Memuaskan", "Lumayan", "Cukup baik"])
    elif rating == 2:
        return random.choice(["Tidak seperti yang diharapkan", "Biasa saja", "Kurang memuaskan"])
    elif rating == 1:
        return random.choice(["Tidak memuaskan", "Sangat buruk", "Mengecewakan"])
    else:
        return "Rating tidak valid"

# Data Master
master_wisatawan = ["IDW1", "IDW2", "IDW3", "IDW4", "IDW5"]
master_pembayaran = ["IDP1", "IDP2", "IDP3"]
master_aktivitas = ["IDA1", "IDA2", "IDA3", "IDA4", "IDA5"]
activities = ["Snorkeling", "Camping", "Bersepeda", "BBQ", "Tur Mangrove"]
preferences = ["Snorkeling", "Camping", "Bersepeda", "BBQ", "Tur Mangrove"]

# Generate Data
rows = 1010
reservasi_data = []
aktivitas_data = []
ulasan_data = []

for i in range(1, rows + 1):
    # Generate dates
    dates = generate_consistent_dates()

    # Reservasi Data
    reservasi_data.append({
        "ID_Reservasi": f"R{i:04}",
        "ID_Wisatawan": random.choice(master_wisatawan),
        "ID_Pembayaran": random.choice(master_pembayaran),
        "Nama": " ".join(fake.first_name() for _ in range(random.randint(2, 3))),
        "Nomor_Telepon": generate_phone_number(),
        "Tanggal_Reservasi": dates["Tanggal_Reservasi"],
        "Jumlah_Orang": random.randint(1, 10),
        "Total_Biaya": random.randint(1, 10) * 200000
    })

    # Aktivitas Data
    aktivitas_data.append({
        "ID_Reservasi": f"R{i:04}",
        "ID_Aktivitas": random.choice(master_aktivitas),
        "Tanggal_Wisata": dates["Tanggal_Aktivitas"]
    })

    # Ulasan Data
    rating = random.randint(1, 5)
    ulasan_data.append({
        "ID_Reservasi": f"R{i:04}",
        "Tanggal_Ulasan": dates["Tanggal_Ulasan"],
        "Rating": rating,
        "Komentar": generate_rating_comment(rating),
        "Preferensi": random.choice(preferences)
    })

# Konversi ke DataFrames
reservasi_df = pd.DataFrame(reservasi_data)
aktivitas_df = pd.DataFrame(aktivitas_data)
ulasan_df = pd.DataFrame(ulasan_data)

# Revisi 3: Adjust jumlah_orang
def adjust_jumlah_orang(id_wisatawan):
    if id_wisatawan == "IDW1":
        return random.randint(2, 15)
    elif id_wisatawan == "IDW2":
        return random.randint(5, 30)
    elif id_wisatawan in ["IDW3", "IDW4"]:
        return random.randint(10, 30)
    elif id_wisatawan == "IDW5":
        return 1
    return random.randint(1, 10)

reservasi_df["Jumlah_Orang"] = reservasi_df["ID_Wisatawan"].apply(adjust_jumlah_orang)

# Simpan File
reservasi_df.to_csv("/home/hadoop/spark-script/reservasi_wisata.csv", index=False)
aktivitas_df.to_csv("/home/hadoop/spark-script/aktivitas_wisata.csv", index=False)
ulasan_df.to_csv("/home/hadoop/spark-script/ulasan_preferensi.csv", index=False)

# Output paths
print({
    "Reservasi Wisata": "/home/hadoop/spark-script/reservasi_wisata.csv",
    "Aktivitas Wisata": "/home/hadoop/spark-script/aktivitas_wisata.csv",
    "Ulasan Preferensi": "/home/hadoop/spark-script/ulasan_preferensi.csv"
})
