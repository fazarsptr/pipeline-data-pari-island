----------------CREATE TABLE DATA UTAMA------------------
 CREATE TABLE reservasi_wisata (
    id_reservasi VARCHAR(20) PRIMARY KEY,
    id_wisatawan VARCHAR(20),
    id_pembayaran VARCHAR(20),
    nama VARCHAR(30),
    nomor_telepon VARCHAR(15),
    tanggal_reservasi VARCHAR(10),
    jumlah_orang BIGINT,
    total_biaya BIGINT
);