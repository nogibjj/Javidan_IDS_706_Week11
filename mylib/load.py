from lib import load_to_db, extract_csv

if __name__ == "__main__":
  df = extract_csv(
    url = 'https://raw.githubusercontent.com/nogibjj/Javidan_IDS_706_Week11/refs/heads/main/data/olympic_summer.csv'
  )
  
  load_to_db(df)