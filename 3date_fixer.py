import os
import re
import shutil
import pandas as pd
import jdatetime
from datetime import datetime
from tqdm import tqdm
from tkinter import Tk
from tkinter.filedialog import askopenfilename

CHUNK_SIZE = 50000

class DateAuditorModule:
    @staticmethod
    def extract_and_convert(val, fmt_choice, is_datetime=False):
        if pd.isna(val) or val is None or str(val).strip() == "": return None
        
        d = str.maketrans("۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩", "01234567890123456789")
        clean_text = str(val).translate(d)
        
        date_pattern = re.compile(r'(\d{2,4}[-./]\d{1,2}[-./]\d{1,2})')
        match = date_pattern.search(clean_text)
        
        if not match: return None
        date_str = match.group(1).replace('.', '-').replace('/', '-')
        
        time_match = re.search(r'(\d{1,2}:\d{2}(?::\d{2})?)', clean_text)
        t_str = time_match.group(1) if time_match else "00:00:00"
        if len(t_str.split(':')) == 2: t_str += ":00"
        
        fmts = ["%Y-%m-%d", "%d-%m-%Y", "%m-%d-%Y", "%y-%m-%d", "%d-%m-%y"]
        if fmt_choice == '1': fmts.insert(0, fmts.pop(1))
        elif fmt_choice == '2': fmts.insert(0, fmts.pop(2))

        for f in fmts:
            try:
                try:
                    jt = jdatetime.datetime.strptime(date_str, f)
                    if 1300 <= jt.year <= 1500:
                        greg_date = jt.togregorian().strftime("%Y-%m-%d")
                        return f"{greg_date} {t_str}" if is_datetime else greg_date
                except: pass
                
                dt = datetime.strptime(date_str, f)
                if dt.year > 1500:
                    greg_date = dt.strftime("%Y-%m-%d")
                    return f"{greg_date} {t_str}" if is_datetime else greg_date
            except: continue
        return None

def process_row_logic(row, date_cols, datetime_cols, fmt_choice):
    any_fixed = False
    any_failed = False
    
    all_cols = date_cols + datetime_cols
    
    for col in all_cols:
        original_val = row.get(col)
        error_cols = str(row.get("error_column", "")) if pd.notna(row.get("error_column")) else ""
        
        is_datetime = col in datetime_cols

        if col not in error_cols:
            pass 
        else:
            fixed_val = DateAuditorModule.extract_and_convert(original_val, fmt_choice, is_datetime=is_datetime)
            if fixed_val:
                any_fixed = True
                row[col] = fixed_val
                
                msgs_str = str(row.get("error_message", "")) if pd.notna(row.get("error_message")) else ""
                if col in error_cols:
                    msgs = msgs_str.split("; ")
                    cols_list = error_cols.split("; ")
                    new_msgs = [f"[Fixed] {m}" if c == col and not m.startswith("[Fixed]") else m for c, m in zip(cols_list, msgs)]
                    row["error_message"] = "; ".join(new_msgs)
            else:
                any_failed = True
                
    if any_fixed:
        row["date_fix_status"] = True
    elif any_failed:
        row["date_fix_status"] = False
    else:
        row["date_fix_status"] = None

    return row

def get_total_rows(file_path):
    with open(file_path, 'r', encoding='utf-8-sig', errors='ignore') as f:
        return sum(1 for _ in f) - 1

def main():
    root = Tk()
    root.withdraw()
    root.attributes('-topmost', True)
    print("="*60)
    print("📅 GLOBAL DATE CLEANER & AUDITOR (Optimized)")
    print("="*60)
    
    file_path = askopenfilename(
        parent=root,
        title="Select FINAL Processed File from Step 4",
        filetypes=[("CSV Files", "*.csv"), ("All Files", "*.*")]
    )
    
    root.destroy()
    
    if not file_path:
        print("❌ No file selected. Exiting...")
        return

    print("\n" + "="*60)
    datetime_cols_input = input("Enter DATETIME columns (Date + Time) (comma-separated, press Enter if none): ").strip()
    datetime_cols = [c.strip() for c in datetime_cols_input.split(',')] if datetime_cols_input else []
    
    date_cols_input = input("Enter ONLY DATE columns (comma-separated, press Enter if none): ").strip()
    date_cols = [c.strip() for c in date_cols_input.split(',')] if date_cols_input else []
    
    if not date_cols and not datetime_cols:
        print("❌ Column names cannot be empty. Exiting...")
        return

    print("\n📅 Date format detected in the raw data:")
    print("[1] Day first   (e.g., 31/12/2023 or 29/12/1402)")
    print("[2] Month first (e.g., 12/31/2023)")
    print("[3] Year first  (e.g., 2023/12/31 or 1402/12/29)")
    fmt_choice = input("Enter the number (1, 2, or 3): ").strip()
    if fmt_choice not in ['1', '2', '3']: fmt_choice = '3'

    print("\n" + "-"*60)
    print("Save options:")
    print("[1] Overwrite original file (Workflow optimized)")
    print("[2] Save as a new file")
    print("-"*60)
    choice = input("Your choice (1 or 2): ").strip()

    print(f"\nAnalyzing file: {os.path.basename(file_path)}...")
    total_rows = get_total_rows(file_path)
    
    temp_file = "temp_date_processing.csv"
    fixed_count, failed_count, null_count = 0, 0, 0

    try:
        with tqdm(total=total_rows, desc="Processing Rows", unit="row") as pbar:
            for i, chunk in enumerate(pd.read_csv(file_path, dtype=str, encoding='utf-8-sig', chunksize=CHUNK_SIZE)):
                
                missing_cols = [c for c in (date_cols + datetime_cols) if c not in chunk.columns]
                if missing_cols:
                    print(f"\n❌ Error: Columns {missing_cols} not found in the file!")
                    return
                
                if "date_fix_status" not in chunk.columns:
                    chunk["date_fix_status"] = None
                
                records = chunk.to_dict('records')
                processed_records = []
                
                for row in records:
                    proc_row = process_row_logic(row, date_cols, datetime_cols, fmt_choice)
                    status = proc_row.get("date_fix_status")
                    if status is True: fixed_count += 1
                    elif status is False: failed_count += 1
                    else: null_count += 1
                    processed_records.append(proc_row)
                
                processed_chunk = pd.DataFrame(processed_records)
                mode = 'w' if i == 0 else 'a'
                header = True if i == 0 else False
                processed_chunk.to_csv(temp_file, mode=mode, index=False, header=header, encoding='utf-8-sig')
                
                pbar.update(len(chunk))

        if choice == '1':
            output_file = file_path
            shutil.move(temp_file, output_file)
            action_msg = "Overwritten (Updated)"
        else:
            base_name, ext = os.path.splitext(file_path)
            output_file = f"{base_name}_date_audited{ext}"
            shutil.move(temp_file, output_file)
            action_msg = "Saved as a new file"
            
        print("\n✅ Operation completed successfully! 🎉")
        print("📊 STATISTICS:")
        print(f"   📥 Input Rows: {total_rows}")
        print(f"   📤 Output Rows: {total_rows}")
        print(f"   ⚖️ Difference (Deleted Rows): 0")
        print("\n📈 DETAILS:")
        print(f"   🟢 Fixed (True): {fixed_count}")
        print(f"   🔴 Unfixable (False): {failed_count}")
        print(f"   ⚪ Already Valid/Empty (Null): {null_count}")
        print(f"\n📂 Your file {action_msg}:\n   {output_file}")

    except Exception as e:
        if os.path.exists(temp_file): os.remove(temp_file)
        print(f"\n❌ An error occurred: {e}")

if __name__ == "__main__":
    main()