import os
from dotenv import load_dotenv
from datetime import datetime
import pandas as pd
import mysql.connector
import sys

load_dotenv()

mysql_config = {
    'host': os.getenv('MYSQL_HOST'),
    'user': os.getenv('MYSQL_USER'),
    'password': os.getenv('MYSQL_PASSWORD'),
    'database': os.getenv('MYSQL_DATABASE'),
}

location_project = os.getcwd()
location_folder_txt = os.path.join(location_project, "file_txt")
expected_columns = [
    "update_stamp", "msisdn", "brand", "unit_type", "unit_name",
    "area_name", "reg_name", "topic_reason_1", "topic_reason_2",
    "topic_result", "service", "app_id", "user_id",
    "employee_code", "employee_name", "notes"
]

def replace_nan(value, default_value='EMPTY'):
    return default_value if pd.isna(value) else value

def read_file_txt(txt_location, file_name, truncate=False):
    if len(sys.argv) < 3:
        raise ValueError("Usage: python auto_import_excel_interaction.py <bulan> <tahun> [truncate]")

    bulan = int(sys.argv[1])
    tahun = int(sys.argv[2])
    result_date = datetime(tahun, bulan, 1).strftime("%Y-%m-%d")

    try:
        df = pd.read_csv(txt_location, delimiter='~', dtype=str)
    except pd.errors.EmptyDataError:
        print(f"Error: Empty DataFrame in file {txt_location}")
        return
    except Exception as e:
        print(f"Error reading file {txt_location}: {e}")
        return

    missing_columns = set(expected_columns) - set(df.columns)
    if missing_columns:
        print(f"File: [{file_name}] Gagal Import Txt. Kolom tidak ditemukan: {missing_columns}")
        return

    try:
        df['update_stamp'] = pd.to_datetime(df['update_stamp'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
    except Exception as e:
        print(f"Error parsing update_stamp: {e}")
        return

    try:
        conn = mysql.connector.connect(**mysql_config)
        cursor = conn.cursor()
        table_interaction = f"ccap_t_interaction_{tahun}{str(bulan).zfill(2)}"

        if truncate:
            cursor.execute(f"TRUNCATE TABLE {table_interaction}")
            conn.commit()

        for index, row in df.iterrows():
            cursor.execute("SELECT grapari_fix FROM ccap_m_mapping_grapari WHERE grapari_source = %s", (row['unit_name'],))
            result = cursor.fetchone()
            unit_name_final = result[0] if result else 'NOT FOUND'
            cursor.nextset()

            msisdn = str(row['msisdn']) if not pd.isna(row['msisdn']) else ''
            type_service = 'Telkomsel' if msisdn.startswith('628') else 'Indihome' if msisdn else None

            insert_query = f"""
            INSERT INTO {table_interaction} (
                update_stamp, msisdn, brand, unit_type, unit_name, unit_name_final,
                area_name, reg_name, topic_reason_1, topic_reason_2, topic_result,
                service, app_id, user_id, employee_code, employee_name, notes, type_service
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, (
                replace_nan(row['update_stamp']), replace_nan(row['msisdn']), replace_nan(row['brand']),
                replace_nan(row['unit_type']), replace_nan(row['unit_name']), replace_nan(unit_name_final),
                replace_nan(row['area_name']), replace_nan(row['reg_name']), replace_nan(row['topic_reason_1']),
                replace_nan(row['topic_reason_2']), replace_nan(row['topic_result']), replace_nan(row['service']),
                replace_nan(row['app_id']), replace_nan(row['user_id']), replace_nan(row['employee_code']),
                replace_nan(row['employee_name']), replace_nan(row['notes']), replace_nan(type_service)
            ))
            conn.commit()
            print(f"Success Insert: {row['msisdn']} | {unit_name_final} | {row['service']} | {row['topic_result']}")

        print(f"File: [{file_name}] Sukses Import Txt.")

        # ========== Proses Interaksi Unique ==========
        # Proses ini mengambil semua data dari tabel bulan berjalan,
        # lalu menentukan baris interaksi unik berdasarkan kombinasi msisdn + tanggal (update_date),
        # dengan urutan prioritas topik yang sudah dikelompokkan dalam SERVICE_DETAIL dan PRIORITY.
        
        cursor = conn.cursor(dictionary=True)
        cursor.execute(f"SELECT * FROM {table_interaction}")
        rows = cursor.fetchall()
        if rows:
            df_all = pd.DataFrame(rows)
            print("✅ Jumlah total baris awal:", len(df_all))
            # Hitung jumlah baris unik berdasarkan kombinasi msisdn dan tanggal (berarti bisa lebih dari 1 per bulan)
            print("✅ Jumlah nilai msisdn unik (berdasarkan tanggal):", df_all['msisdn'].nunique())
            # Ambil hanya bagian tanggal dari update_stamp (tanpa waktu) untuk digunakan sebagai dasar pengelompokan harian
            df_all['update_date'] = pd.to_datetime(df_all['update_stamp'], errors='coerce').dt.date


            topic_to_service_detail = {
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
                "P44-Permintaan pembayaran cicilan tagihan": "P44",
                "P44-Permintaan pembayaran deposit": "P44",
                "P44-Permintaan pembayaran tagihan": "P44"
            }
            priority_order = {'P1': 1, 'P44': 2, 'P2': 3, 'P': 4, 'K': 5, 'O': 6, 'I': 7, 'C': 8}
            # Mapping topic_result ke SERVICE_DETAIL berdasarkan kamus topic_to_service_detail
            # Jika tidak ditemukan, fallback ke nilai 'service' dari kolom data
            # Kemudian dari SERVICE_DETAIL diekstrak prefix huruf (misal: P1, P44, P) lalu dipetakan ke PRIORITY
            # Tujuannya untuk mengurutkan interaksi agar yang prioritas tinggi diproses lebih dulu saat pengambilan unique
            df_all['SERVICE_DETAIL'] = df_all['topic_result'].map(topic_to_service_detail)
            df_all['SERVICE_DETAIL'] = df_all['SERVICE_DETAIL'].fillna(df_all['service']).astype(str)
            df_all['PRIORITY'] = df_all['SERVICE_DETAIL'].str.extract(r'^([A-Z]+\d*)')[0].map(
                lambda x: priority_order.get(str(x) if str(x) in priority_order else str(x)[0] if pd.notna(x) and str(x) else None, 99)
            )

            df_sorted = df_all.sort_values(by='PRIORITY')
            df_sorted['update_date'] = df_sorted['update_stamp'].dt.date
            # Ambil satu interaksi unik per msisdn per hari berdasarkan update_date
            # Jika ingin hanya satu interaksi per msisdn per bulan, cukup gunakan subset=['msisdn']
            df_unique = df_sorted.drop_duplicates(subset=['msisdn', 'update_date'], keep='first')

            unique_table = f"ccap_t_interaction_unique_{tahun}{str(bulan).zfill(2)}"
            cursor.execute(f"TRUNCATE TABLE {unique_table}")
            conn.commit()
            cols = [col for col in df_unique.columns if col not in ['id', 'SERVICE_DETAIL', 'PRIORITY', 'update_date']]
            placeholders = ", ".join(["%s"] * len(cols))
            insert_stmt = f"INSERT INTO {unique_table} ({', '.join(cols)}) VALUES ({placeholders})"

            for _, row in df_unique.iterrows():
                values = tuple(row[col] for col in cols)
                cursor.execute(insert_stmt, values)
            conn.commit()
            print(f"✅ Sukses insert ke {unique_table}")

    except Exception as e:
        print(f"Error processing rows: {e}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

if __name__ == "__main__":
    truncate_flag = len(sys.argv) > 3 and sys.argv[3].lower() == "truncate"

    if not os.path.exists(location_folder_txt):
        print(f"Directory {location_folder_txt} does not exist.")
    else:
        with os.scandir(location_folder_txt) as entries:
            for entry in entries:
                if entry.is_file() and entry.name.endswith('.txt') and not entry.name.startswith('~$'):
                    read_file_txt(entry.path, entry.name, truncate=truncate_flag)