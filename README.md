```

# Folder (recommended for real use)
python .\mpp_tm1_extractor_smart.py `
  --base-dir "C:\Users\sas1924s\Downloads\Sasi_KickPV\Data_Extraction_Tools" `
  --timestamps-csv "C:\Users\sas1924s\Downloads\Sasi_KickPV\Data_Extraction_Tools\sma_all_files_extended.csv" `
  --ts-column "timestamp" `
  --out-csv "C:\Users\sas1924s\Downloads\Sasi_KickPV\Data_Extraction_Tools\out_tm1.csv" `
  --out-pkl "C:\Users\sas1924s\Downloads\Sasi_KickPV\Data_Extraction_Tools\out_tm1.pkl" `
  --inverter-id 4 `
  --debug

```


```

python .\mpp_tm1_extractor_fast.py `
  --base-dir "C:\Users\sas1924s\Downloads\Sasi_KickPV\Data_Extraction_Tools\mpp" `
  --timestamps-csv "C:\Users\sas1924s\Downloads\Sasi_KickPV\Data_Extraction_Tools\sma_all_files_extended.csv" `
  --ts-column "timestamp" `
  --out-csv "C:\Users\sas1924s\Downloads\Sasi_KickPV\Data_Extraction_Tools\out_tm1.csv" `
  --out-pkl "C:\Users\sas1924s\Downloads\Sasi_KickPV\Data_Extraction_Tools\out_tm1.pkl" `
  --inverter-id 4 `
  --debug

```

How to run it (PowerShell)

Stop at May and match by minute (default):

python .\mpp_tm1_extractor_fast.py `
  --base-dir "C:\Users\sas1924s\Downloads\Sasi_KickPV\Data_Extraction_Tools\mpp" `
  --timestamps-csv "C:\Users\sas1924s\Downloads\Sasi_KickPV\Data_Extraction_Tools\sma_all_files_extended.csv" `
  --ts-column "timestamp" `
  --date-max "2025-05" `
  --out-csv "C:\Users\sas1924s\Downloads\Sasi_KickPV\Data_Extraction_Tools\out_tm1.csv" `
  --out-pkl "C:\Users\sas1924s\Downloads\Sasi_KickPV\Data_Extraction_Tools\out_tm1.pkl" `
  --inverter-id 4 `
  --debug

If you prefer a tolerance (±59s around your CSV time)
python .\mpp_tm1_extractor_fast.py `
  --base-dir "C:\Users\sas1924s\Downloads\Sasi_KickPV\Data_Extraction_Tools\mpp" `
  --timestamps-csv "..." `
  --ts-column "timestamp" `
  --date-max "2025-05" `
  --match tolerance `
  --second-tolerance 59 `
  --out-csv "...\out_tm1.csv" `
  --out-pkl "...\out_tm1.pkl" `
  --inverter-id 4 `
  --debug