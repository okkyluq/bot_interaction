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
expected_columns = ["update_stamp", "msisdn", "brand", "unit_type", "unit_name", "area_name", "reg_name", "topic_reason_1", "topic_reason_2", "topic_result", "service", "app_id", "user_id", "employee_code", "employee_name", "notes"]

def replace_nan(value, default_value='EMPTY'):
    return default_value if pd.isna(value) else value

def read_file_txt(txt_location, file_name, truncate=False):
    if len(sys.argv) < 3:
        raise ValueError("Usage: python auto_import_excel_interaction.py <parameter1 [Bulan]> <parameter2 [Tahun]> [truncate]")

    bulan = int(sys.argv[1])
    tahun = int(sys.argv[2])
    result_date = datetime(tahun, bulan, 1).strftime("%Y-%m-%d")

    if not bulan:
        raise ValueError("Parameter1 untuk bulan tidak boleh kosong")
    
    if not tahun:
        raise ValueError("Parameter2 untuk Tahun tidak boleh kosong")

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
        print(f"File: [{file_name}] Gagal Import Txt (Format Salah). Kolom yang diharapkan tidak ditemukan: {missing_columns}")
        return
    
    if 'update_stamp' in df.columns:
        for idx, value in enumerate(df['update_stamp']):
            update_stamp = datetime.strptime(str(value), '%Y-%m-%d %H:%M:%S')
            if update_stamp.year != tahun or update_stamp.month != bulan:
                print(f"File: [{file_name}] Gagal Import Txt. Terdapat Bulan dan Tahun yang tidak sesuai pada Parameter")
                return

    try:
        df['update_stamp'] = pd.to_datetime(df['update_stamp'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
    except Exception as e:
        print(f"Error converting 'update_stamp' to datetime: {e}")
        return

    try:
        conn = mysql.connector.connect(**mysql_config)
        cursor = conn.cursor()
        table_interaction = f"ccap_t_interaction_{tahun}{str(bulan).zfill(2)}"

        if truncate:
            try:
                print(f"Truncating table: {table_interaction}")
                cursor.execute(f"TRUNCATE TABLE {table_interaction}")
                conn.commit()
                print(f"Table {table_interaction} truncated successfully.")
            except Exception as e:
                print(f"Error truncating table {table_interaction}: {e}")
                return

        for index, row in df.iterrows():
            try:
                cursor.execute("SELECT grapari_fix FROM ccap_m_mapping_grapari WHERE grapari_source = %s", (row['unit_name'],))
                result = cursor.fetchone()
                unit_name_final = result[0] if result else 'NOT FOUND'
                cursor.nextset()
            except Exception as e:
                print(f"Error fetching unit_name_final for row {index}: {e}")
                unit_name_final = 'NOT FOUND'
                
            msisdn = str(row['msisdn']) if not pd.isna(row['msisdn']) else ''
            if not msisdn or msisdn == '':
                type_service = None
            elif msisdn.startswith('628'):
                type_service = 'Telkomsel'
            else:
                type_service = 'Indihome'
            
            insert_query = f"""
            INSERT INTO {table_interaction} (update_stamp, msisdn, brand, unit_type, unit_name, unit_name_final, area_name, reg_name, topic_reason_1, topic_reason_2, topic_result, service, app_id, user_id, employee_code, employee_name, notes, type_service)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, (
                replace_nan(row['update_stamp']),
                replace_nan(row['msisdn']),
                replace_nan(row['brand']),
                replace_nan(row['unit_type']),
                replace_nan(row['unit_name']),
                replace_nan(unit_name_final),
                replace_nan(row['area_name']),
                replace_nan(row['reg_name']),
                replace_nan(row['topic_reason_1']),
                replace_nan(row['topic_reason_2']),
                replace_nan(row['topic_result']),
                replace_nan(row['service']),
                replace_nan(row['app_id']),
                replace_nan(row['user_id']),
                replace_nan(row['employee_code']),
                replace_nan(row['employee_name']),
                replace_nan(row['notes']),
                replace_nan(type_service)
            ))
            conn.commit()
            print(f"Success Insert : {row['msisdn']}|{unit_name_final}|{row['service']}|{row['topic_result']}")
            
    except Exception as e:
        print(f"Error processing rows: {e}")
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

    print(f"File: [{file_name}] Sukses Import Txt.")

if __name__ == "__main__":
    truncate_flag = len(sys.argv) > 3 and sys.argv[3].lower() == "truncate"

    if not os.path.exists(location_folder_txt):
        print(f"Directory {location_folder_txt} does not exist.")
    else:
        with os.scandir(location_folder_txt) as entries:
            for entry in entries:
                if entry.is_file() and entry.name.endswith('.txt') and not entry.name.startswith('~$'):
                    read_file_txt(entry.path, entry.name, truncate=truncate_flag)
