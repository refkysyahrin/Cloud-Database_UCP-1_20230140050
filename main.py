import time
import schedule
from datetime import datetime
from bs4 import BeautifulSoup
from pymongo import MongoClient

# Library Selenium
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# ==========================================================
# 1. SETUP KONEKSI MONGODB
# ==========================================================
MONGO_URI = 'mongodb+srv://refkyremote_db_user:mongoEky@testingp2.sqpzbvo.mongodb.net/'

try:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    client.server_info()
    db = client['scheduler_auto']
    collection = db['CNBC_Sustainability']
    print("✅ Koneksi MongoDB Atlas Berhasil!")
except Exception as e:
    print(f"❌ ERROR Database: {e}")
    exit()

def get_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")
    
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=chrome_options)

def crawl_cnbc_hybrid():
    print(f"\n" + "="*60)
    print(f"🚀 MEMULAI HYBRID CRAWLING: {datetime.now().strftime('%H:%M:%S')}")
    print(f"="*60)
    
    target_url = "https://www.cnbcindonesia.com/news" 
    driver = get_driver()
    
    try:
        print(f"🌐 Selenium mengakses: {target_url}")
        driver.get(target_url)
        
        try:
            wait = WebDriverWait(driver, 15)
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "article")))
            driver.execute_script("window.scrollTo(0, 800);")
            time.sleep(3) 
        except:
            pass

        soup = BeautifulSoup(driver.page_source, 'lxml')
        articles = soup.select('article') or soup.select('.list__item')
        
        print(f"📊 Ditemukan {len(articles)} artikel potensial.")

        for index, art in enumerate(articles[:10], 1): 
            try:
                link_tag = art.find('a')
                if not link_tag: continue
                link = link_tag['href']
                if link.startswith('/'): link = "https://www.cnbcindonesia.com" + link

                judul = art.find('h2').get_text(strip=True) if art.find('h2') else "No Title"

                if collection.find_one({'url': link}):
                    print(f"⏩ [{index}] Skip: Sudah ada.")
                    continue

                # --- MENUJU HALAMAN DETAIL ---
                driver.get(link)
                time.sleep(3) 
                detail_soup = BeautifulSoup(driver.page_source, 'lxml')

                # 1. TANGGAL & AUTHOR (Meta Tag tetap yang paling stabil)
                date_meta = detail_soup.find('meta', attrs={'name': 'dtk:publishdate'})
                tanggal = date_meta['content'] if date_meta else "N/A"

                author_meta = detail_soup.find('meta', attrs={'name': 'dtk:author'})
                author = author_meta['content'] if author_meta else "N/A"

                # 2. PERBAIKAN TAG KATEGORI (Mencoba berbagai selector)
                # Mencari di box tag bawah artikel, tag di atas judul, dan tag metadata
                raw_tags = detail_soup.select('.child_tag a') + \
                           detail_soup.select('.detail__body-tag a') + \
                           detail_soup.select('.tag a')
                
                # Gunakan set() untuk menghindari duplikasi tag, lalu balikkan ke list
                tags = list(set([t.get_text(strip=True).replace("#", "") for t in raw_tags if t.get_text(strip=True)]))

                # 3. ISI BERITA
                detail_body = detail_soup.find('div', class_='detail_text') or \
                              detail_soup.find('div', class_='detail__body-text')
                
                isi_berita = "Isi tidak ditemukan"
                if detail_body:
                    for s in detail_body(['script', 'style', 'div', 'table', 'iframe', 'canvas']): 
                        s.decompose()
                    isi_berita = detail_body.get_text(" ", strip=True)
                else:
                    # Fallback ke meta description jika konten utama tidak ketemu
                    meta_desc = detail_soup.find('meta', attrs={'name': 'description'})
                    if meta_desc:
                        isi_berita = meta_desc['content']

                # 4. THUMBNAIL
                img_meta = detail_soup.find('meta', property='og:image')
                thumbnail = img_meta['content'] if img_meta else ""

                # SIMPAN KE DATABASE
                payload = {
                    "url": link,
                    "judul": judul,
                    "tanggal_publish": tanggal,
                    "author": author,
                    "tag_kategori": tags, # Sekarang harusnya terisi array ['tag1', 'tag2']
                    "isi_berita": isi_berita,
                    "thumbnail": thumbnail,
                    "scraped_at": datetime.now()
                }
                
                collection.insert_one(payload)
                print(f"✅ [{index}] Berhasil: {judul[:40]}... (Tags: {len(tags)})")

            except Exception as e:
                print(f"❌ [{index}] Gagal: {e}")
                continue

    except Exception as e:
        print(f"❌ Error Utama: {e}")
    finally:
        driver.quit() 
        print(f"\n🏁 SELESAI.")

# Jalankan langsung
crawl_cnbc_hybrid()