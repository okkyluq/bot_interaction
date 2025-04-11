import pandas as pd
import datetime

# =========================
# Step 1: Load file txt
# =========================
input_file = 'interaction_base_crmbe_interaction_20250406_222509.txt'
df = pd.read_csv(input_file, delimiter='~', low_memory=False)

# =========================
# Step 2: Mapping Topic ke SERVICE_DETAIL
# =========================
# Buat dictionary mapping
topic_to_service_detail = {
    # P1
    "P11-Permintaan Pasang Baru Telkomsel Halo": "P1",
    "P11-Permintaan Penambahan Kontrak Pasang Baru Telkomsel Halo": "P1",
    "P11-Permintaan Pasang Baru Telkomsel Halo Nomor Cantik": "P1",
    "P15-Permintaan reestablish": "P1",
    "P32-Permintaan Pre2Post Halo+": "P1",
    "P32-Permintaan Post2Pre karena migrasi Halo+": "P1",
    "P52-Permintaan perubahan kepemilikan": "P1",
    "P52-Permintaan perubahan customer type": "P1",
    "P52-Permintaan Perubahan Data Pelanggan": "P1",
    "P58-Permintaan Migrasi Seamless P2P": "P1",
    "P58-Permintaan Migrasi Pre to Post": "P1",
    "P59-Permintaan Migrasi Post to Pre": "P1",
    "P61-Permintaan aktivasi produk campaign": "P1",
    "P71-Permintaan berhenti berlangganan": "P1",

    # P2
    "P13-Permintaan registrasi Prabayar": "P2",
    "P13-Permintaan registrasi Prabayar WNA": "P2",
    "P14-Permintaan Reaktivasi": "P2",
    "P51-Ganti Kartu Karena Hilang": "P2",
    "P51-Ganti Kartu Karena Hilang, Terdapat Fitur Banking": "P2",
    "P51-Ganti Kartu Karena Rusak Akibat Penggunaan": "P2",
    "P51-Ganti Kartu Karena Rusak Akibat Penggunaan, Terdapat Fitur Banking": "P2",
    "P51-Ganti Kartu Karena Rusak Fabrikasi": "P2",
    "P51-Ganti Kartu Untuk Reaktivasi": "P2",
    "P51-Ganti Kartu Untuk Upgrade Kartu": "P2",
    "P51-Ganti Kartu Online Untuk Upgrade 4G": "P2",
    "P51-Ganti Kartu Online Karena Rusak/Hilang Online": "P2",
    "P51-Ganti Kartu Online": "P2",
    "P51-Permintaan Registrasi IMEI Roamer": "P2",

    # P44
    "P44-Permintaan pembayaran cicilan tagihan": "P44",
    "P44-Permintaan pembayaran deposit": "P44",
    "P44-Permintaan pembayaran tagihan": "P44"
}

# =========================
# Step 3: Tambah kolom SERVICE_DETAIL
# =========================
df['SERVICE_DETAIL'] = df['topic_result'].map(topic_to_service_detail)
df['SERVICE_DETAIL'] = df['SERVICE_DETAIL'].fillna(df['service']).astype(str)

# =========================
# Step 4: Buat skor prioritas berdasarkan SERVICE_DETAIL
# =========================
priority_order = {
    'P1': 1, 'P44': 2, 'P2': 3, 'P': 4, 'O': 5, 'K': 6, 'I': 7, 'C': 8
}

# Ekstrak awalan huruf dari SERVICE_DETAIL untuk menentukan prioritas
df['PRIORITY'] = df['SERVICE_DETAIL'].str.extract(r'^([A-Z]+\d*)')[0].map(
    lambda x: priority_order.get(str(x) if str(x) in priority_order else str(x)[0] if pd.notna(x) and str(x) else None, 99)
)

# =========================
# Step 5: Ambil data unik berdasarkan msisdn dan prioritas tertinggi
# =========================
df_sorted = df.sort_values(by='PRIORITY')
df_unique = df_sorted.drop_duplicates(subset='msisdn', keep='first')

# =========================
# Step 6: Simpan ke file output
# =========================
output_file = 'output_unique_msisdn.txt'
df_unique.to_csv(output_file, sep='~', index=False)

print(f"✅ File berhasil disimpan sebagai: {output_file}")

# =========================
# Langkah Tambahan: Transformasi Unique dan Insert ke Tabel Unik
# =========================

# Mapping topik ke SERVICE_DETAIL
topic_to_service_detail = {
    # P1
    "P11-Permintaan Pasang Baru Telkomsel Halo": "P1",
    "P11-Permintaan Penambahan Kontrak Pasang Baru Telkomsel Halo": "P1",
    "P11-Permintaan Pasang Baru Telkomsel Halo Nomor Cantik": "P1",
    "P15-Permintaan reestablish": "P1",
    "P32-Permintaan Pre2Post Halo+": "P1",
    "P32-Permintaan Post2Pre karena migrasi Halo+": "P1",
    "P52-Permintaan perubahan kepemilikan": "P1",
    "P52-Permintaan perubahan customer type": "P1",
    "P52-Permintaan Perubahan Data Pelanggan": "P1",
    "P58-Permintaan Migrasi Seamless P2P": "P1",
    "P58-Permintaan Migrasi Pre to Post": "P1",
    "P59-Permintaan Migrasi Post to Pre": "P1",
    "P61-Permintaan aktivasi produk campaign": "P1",
    "P71-Permintaan berhenti berlangganan": "P1",

    # P2
    "P13-Permintaan registrasi Prabayar": "P2",
    "P13-Permintaan registrasi Prabayar WNA": "P2",
    "P14-Permintaan Reaktivasi": "P2",
    "P51-Ganti Kartu Karena Hilang": "P2",
    "P51-Ganti Kartu Karena Hilang, Terdapat Fitur Banking": "P2",
    "P51-Ganti Kartu Karena Rusak Akibat Penggunaan": "P2",
    "P51-Ganti Kartu Karena Rusak Akibat Penggunaan, Terdapat Fitur Banking": "P2",
    "P51-Ganti Kartu Karena Rusak Fabrikasi": "P2",
    "P51-Ganti Kartu Untuk Reaktivasi": "P2",
    "P51-Ganti Kartu Untuk Upgrade Kartu": "P2",
    "P51-Ganti Kartu Online Untuk Upgrade 4G": "P2",
    "P51-Ganti Kartu Online Karena Rusak/Hilang Online": "P2",
    "P51-Ganti Kartu Online": "P2",
    "P51-Permintaan Registrasi IMEI Roamer": "P2",

    # P44
    "P44-Permintaan pembayaran cicilan tagihan": "P44",
    "P44-Permintaan pembayaran deposit": "P44",
    "P44-Permintaan pembayaran tagihan": "P44"
}

priority_order = {
    'P1': 1, 'P44': 2, 'P2': 3, 'P': 4, 'K': 5, 'O': 6, 'I': 7, 'C': 8
}

# Ambil data dari tabel yang baru saja diinsert
df = pd.read_sql(f"SELECT * FROM {tabel_tujuan}", conn)

# Mapping SERVICE_DETAIL
df['SERVICE_DETAIL'] = df['topic_result'].map(topic_to_service_detail)
df['SERVICE_DETAIL'] = df['SERVICE_DETAIL'].fillna(df['service']).astype(str)

# Hitung PRIORITY
df['PRIORITY'] = df['SERVICE_DETAIL'].str.extract(r'^([A-Z]+\d*)')[0].map(
    lambda x: priority_order.get(str(x) if str(x) in priority_order else str(x)[0] if pd.notna(x) and str(x) else None, 99)
)

# Deduplikasi berdasarkan msisdn dan prioritas
df_sorted = df.sort_values(by='PRIORITY')
df_unique = df_sorted.drop_duplicates(subset='msisdn', keep='first')

# Tentukan nama tabel unik berdasarkan waktu
now = datetime.datetime.now()
unique_table_name = f"ccap_t_interaction_unique_{now.strftime('%Y%m')}"

# Insert ke tabel unik
df_unique.to_sql(unique_table_name, con=conn, if_exists='append', index=False)
print(f"✅ Data unik berhasil disimpan ke tabel {unique_table_name}")