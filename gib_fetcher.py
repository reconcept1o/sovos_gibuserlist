import os
import sys
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import traceback
from zeep import Transport, Client
from requests import Session
from requests.auth import HTTPBasicAuth

print("🚀 GIB Kullanıcı Listesi - Parça Parça İndirici")
print("="*60)

env = os.getenv("NODE_ENV", "production")
dotenv_path = f'.env.{env}'
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
    print(f"✅ {dotenv_path} yüklendi")
else:
    print(f"❌ {dotenv_path} bulunamadı!")
    sys.exit(1)

db_params = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}

print(f"📊 Veritabanı: {db_params['host']}:{db_params['port']}/{db_params['dbname']}")

local_wsdl_path = os.path.join(os.path.dirname(__file__), "wsdl", "ClientEInvoiceServices-2.2.wsdl")
if not os.path.exists(local_wsdl_path):
    print(f"❌ WSDL dosyası bulunamadı: {local_wsdl_path}")
    sys.exit(1)

print(f"📄 WSDL dosyası: ✅")

def create_client():
    username = os.getenv("SOVOS_API_USERNAME")
    password = os.getenv("SOVOS_API_PASSWORD")
    
    if not username or not password:
        raise ValueError("❌ SOVOS API kimlik bilgileri .env dosyasında bulunamadı.")
    
    print(f"🔐 API Kullanıcı: {username[:4]}****")
    
    session = Session()
    session.auth = HTTPBasicAuth(username, password)
    transport = Transport(session=session, timeout=600)
    
    client = Client(local_wsdl_path, transport=transport)
    client.service._binding_options['address'] = "https://efaturaws.fitbulut.com/ClientEInvoiceServices/ClientEInvoiceServicesPort.svc"
    
    return client

def test_db_connection():
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM sovos_gib_user_list;")
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        print(f"✅ Veritabanı bağlantısı başarılı - Mevcut kayıt: {count}")
        return True
    except Exception as e:
        print(f"❌ Veritabanı bağlantı hatası: {e}")
        return False

def save_batch_to_db(batch_data):
    if not batch_data:
        return True
        
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        sql = """
            INSERT INTO sovos_gib_user_list 
            (identifier, alias, title, type, document_type, first_creation_time, is_active, created_at, last_synced_at) 
            VALUES %s 
            ON CONFLICT (identifier, alias, document_type) 
            DO UPDATE SET 
                title = EXCLUDED.title, 
                is_active = EXCLUDED.is_active, 
                last_synced_at = EXCLUDED.last_synced_at;
        """
        
        execute_values(cursor, sql, batch_data, page_size=1000)
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"  💾 {len(batch_data)} kayıt veritabanına yazıldı")
        return True
        
    except Exception as e:
        print(f"  ❌ Veritabanı kaydetme hatası: {e}")
        traceback.print_exc()
        return False

def process_xml_part(xml_content, part_number):
    print(f"    🔍 Part {part_number} XML işleniyor...")
    
    records = []
    
    try:
        it = ET.iterparse(xml_content)
        for _, el in it:
            if '}' in el.tag:
                el.tag = el.tag.split('}', 1)[1]
        root = it.root
        
        user_blocks = root.findall(".//User")
        print(f"      👥 {len(user_blocks)} kullanıcı bloğu bulundu")
        
        processed_users = 0
        for user_block in user_blocks:
            identifier_node = user_block.find("Identifier")
            if identifier_node is None or not identifier_node.text:
                continue
            
            vkn_tckn = identifier_node.text
            title = user_block.findtext("Title")
            
            document_nodes = user_block.findall(".//Document")
            for doc_node in document_nodes:
                for alias_node in doc_node.findall("Alias"):
                    if alias_node.find("DeletionTime") is not None:
                        continue
                    
                    name_node = alias_node.find("Name")
                    creation_time_node = alias_node.find("CreationTime")
                    
                    if name_node is None or creation_time_node is None or not name_node.text:
                        continue
                    
                    try:
                        creation_time = datetime.strptime(creation_time_node.text, "%Y-%m-%dT%H:%M:%S")
                    except ValueError as e:
                        print(f"      ⚠️ Tarih parse hatası: {creation_time_node.text}")
                        continue
                    
                    doc_type = "DespatchAdvice" if doc_node.get("type") == "DespatchAdvice" else "Invoice"
                    now = datetime.now()
                    
                    record = (
                        vkn_tckn,
                        name_node.text,
                        title,
                        "PK",
                        doc_type,
                        creation_time,
                        True,
                        now,
                        now
                    )
                    records.append(record)
                    processed_users += 1
        
        print(f"      ✅ Part {part_number}'den {processed_users} kayıt işlendi")
        return records
        
    except Exception as e:
        print(f"      ❌ Part {part_number} XML işleme hatası: {e}")
        traceback.print_exc()
        return []

def main():
    print("\n" + "="*60)
    print("İŞLEM BAŞLATILIYOR")
    print("="*60)
    
    if not test_db_connection():
        print("❌ Veritabanı bağlantısı başarısız!")
        return
    
    print("🔄 Mevcut kayıtlar pasif yapılıyor...")
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        cursor.execute("UPDATE sovos_gib_user_list SET is_active = false;")
        affected = cursor.rowcount
        conn.commit()
        cursor.close()
        conn.close()
        print(f"  ✅ {affected} kayıt pasif yapıldı")
    except Exception as e:
        print(f"  ❌ Pasif yapma hatası: {e}")
    
    try:
        client = create_client()
        print("✅ SOAP Client oluşturuldu")
    except Exception as e:
        print(f"❌ Client oluşturma hatası: {e}")
        return
    
    print("\n📡 API'den kullanıcı listesi çekiliyor...")
    try:
        response = client.service.getPartialUserList(
            Identifier=os.getenv("SOVOS_IDENTIFIER"),
            VKN_TCKN=os.getenv("SOVOS_VKNTCKN"),
            Role="PK",
            IncludeBinary=True
        )
        
        if not response or not response.userListPart:
            print("❌ API'den veri gelmedi!")
            return
        
        total_parts = len(response.userListPart)
        print(f"✅ {total_parts} adet part alındı")
        
    except Exception as e:
        print(f"❌ API çağrı hatası: {e}")
        traceback.print_exc()
        return
    
    print(f"\n📦 {total_parts} part tek tek işlenecek...")
    
    batch_data = []
    BATCH_SIZE = 5000
    total_processed = 0
    successful_parts = 0
    
    for i, part_data in enumerate(response.userListPart, 1):
        print(f"\n📋 Part {i}/{total_parts} işleniyor...")
        
        binary_data = part_data.binaryData
        if not binary_data:
            print(f"  ⚠️ Part {i} boş, atlanıyor")
            continue
        
        print(f"  📏 Boyut: {len(binary_data):,} bytes")
        
        zip_path = f"temp_part_{i}.zip"
        
        try:
            with open(zip_path, "wb") as f:
                f.write(binary_data)
            
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                xml_filename = zip_ref.namelist()[0]
                print(f"  📄 XML dosyası: {xml_filename}")
                
                with zip_ref.open(xml_filename) as xml_file:
                    part_records = process_xml_part(xml_file, i)
                    
                    if part_records:
                        batch_data.extend(part_records)
                        total_processed += len(part_records)
                        successful_parts += 1
                        
                        if len(batch_data) >= BATCH_SIZE:
                            print(f"  💾 {len(batch_data)} kayıt veritabanına yazılıyor...")
                            if save_batch_to_db(batch_data):
                                batch_data = []
                            else:
                                print(f"  ❌ Part {i} kaydetme başarısız!")
                                break
            
        except Exception as e:
            print(f"  ❌ Part {i} işleme hatası: {e}")
            traceback.print_exc()
            
        finally:
            if os.path.exists(zip_path):
                os.remove(zip_path)
                print(f"  🗑️ Geçici dosya silindi")
        
        progress = (i / total_parts) * 100
        print(f"  📊 İlerleme: {progress:.1f}% ({i}/{total_parts})")
    
    if batch_data:
        print(f"\n💾 Kalan {len(batch_data)} kayıt veritabanına yazılıyor...")
        save_batch_to_db(batch_data)
    
    print(f"\n" + "="*60)
    print("İŞLEM TAMAMLANDI!")
    print("="*60)
    print(f"📊 Toplam part sayısı: {total_parts}")
    print(f"✅ Başarılı part sayısı: {successful_parts}")
    print(f"📈 Toplam işlenen kayıt: {total_processed:,}")
    
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM sovos_gib_user_list WHERE is_active = true;")
        active_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM sovos_gib_user_list;")
        total_count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        
        print(f"💾 Veritabanı aktif kayıt: {active_count:,}")
        print(f"💾 Veritabanı toplam kayıt: {total_count:,}")
        
    except Exception as e:
        print(f"⚠️ Final rapor hatası: {e}")
    
    print(f"🎉 İşlem {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} tarihinde tamamlandı!")

if __name__ == "__main__":
    main()