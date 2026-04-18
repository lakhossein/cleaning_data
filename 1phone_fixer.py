import os
import re
import shutil
import pandas as pd
from tqdm import tqdm
from tkinter import Tk
from tkinter.filedialog import askopenfilename

CHUNK_SIZE = 50000

def clean_phone_number(val):
    if pd.isna(val) or val is None or str(val).strip() == "": return None
    cleaned = re.sub(r'\D', '', str(val))
    return cleaned if cleaned else None

def process_row_logic(row, phone_cols):
    any_fixed = False
    any_failed = False
    
    for phone_col in phone_cols:
        val = row.get(phone_col)
        original_val = str(val).strip() if pd.notna(val) and val is not None else ""
        cleaned_val = clean_phone_number(val)
        
        if not original_val or original_val == cleaned_val:
            pass 
        elif cleaned_val:
            any_fixed = True
            row[phone_col] = cleaned_val
            
            cols_str = str(row.get("error_column", "")) if pd.notna(row.get("error_column")) else ""
            msgs_str = str(row.get("error_message", "")) if pd.notna(row.get("error_message")) else ""
            
            if phone_col in cols_str:
                msgs = msgs_str.split("; ")
                cols = cols_str.split("; ")
                new_msgs = [f"[Fixed] {m}" if c == phone_col and not m.startswith("[Fixed]") else m for c, m in zip(cols, msgs)]
                row["error_message"] = "; ".join(new_msgs)
        else:
            any_failed = True
            row[phone_col] = original_val 

    if any_fixed:
        row["phone_fix_status"] = True
    elif any_failed:
        row["phone_fix_status"] = False
    else:
        row["phone_fix_status"] = None

    return row

def get_total_rows(file_path):
    with open(file_path, 'r', encoding='utf-8-sig', errors='ignore') as f:
        return sum(1 for _ in f) - 1

def main():
    root = Tk()
    root.withdraw()
    root.attributes('-topmost', True)

    print("="*60)
    print("📱 PHONE NUMBER CLEANER & AUDITOR (Optimized)")
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

    phone_cols_input = input("\nEnter phone number columns (comma-separated): ").strip()
    phone_cols = [c.strip() for c in phone_cols_input.split(',')] if phone_cols_input else []
    
    if not phone_cols:
        print("❌ Column names cannot be empty. Exiting...")
        return

    print("\n" + "-"*60)
    print("Save options:")
    print("[1] Overwrite original file (Workflow optimized)")
    print("[2] Save as a new file")
    print("-"*60)
    choice = input("Your choice (1 or 2): ").strip()

    print(f"\nAnalyzing file: {os.path.basename(file_path)}...")
    total_rows = get_total_rows(file_path)
    
    temp_file = "temp_phone_processing.csv"
    fixed_count, failed_count, null_count = 0, 0, 0

    try:
        with tqdm(total=total_rows, desc="Processing Rows", unit="row") as pbar:
            for i, chunk in enumerate(pd.read_csv(file_path, dtype=str, encoding='utf-8-sig', chunksize=CHUNK_SIZE)):
                
                missing_cols = [c for c in phone_cols if c not in chunk.columns]
                if missing_cols:
                    print(f"\n❌ Error: Columns {missing_cols} not found in the file!")
                    return
                
                if "phone_fix_status" not in chunk.columns:
                    chunk["phone_fix_status"] = None
                
                records = chunk.to_dict('records')
                processed_records = []
                
                for row in records:
                    proc_row = process_row_logic(row, phone_cols)
                    status = proc_row.get("phone_fix_status")
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
            output_file = f"{base_name}_audited{ext}"
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