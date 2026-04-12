import os
import re
import difflib
import shutil
import pandas as pd
from tqdm import tqdm
from tkinter import Tk
from tkinter.filedialog import askopenfilename

CHUNK_SIZE = 50000
EMAIL_REGEX = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]{2,}$", re.IGNORECASE)
MAJOR_PROVIDERS = ["gmail", "hotmail", "outlook", "yahoo", "icloud", "yandex", "aol", "protonmail", "live", "msn", "zoho", "gmx", "mail"]
COMMON_TLDS = ["com", "net", "org", "edu", "gov", "ir", "tr", "co.uk", "info"]

def is_valid_email(email):
    if not email or pd.isna(email): return False
    return bool(EMAIL_REGEX.match(str(email).strip()))

def clean_and_fix_email(val):
    if not val or pd.isna(val) or str(val).strip() == "": return None, False
    email = str(val).lower().strip()
    email = email.replace(" ", "").replace(",", ".").replace("çcom", ".com").replace("mcom", ".com")
    email = re.sub(r'\.{2,}', '.', email)
    email = email.replace(".@", "@")
    
    if "@" not in email:
        for provider in MAJOR_PROVIDERS:
            idx = email.rfind(provider)
            if idx != -1:
                email = email[:idx-1] + '@' + email[idx:] if idx > 0 and email[idx-1] == 'q' else email[:idx] + '@' + email[idx:]
                break
                
    parts = email.split('@')
    if len(parts) == 2:
        username, domain = parts
        for tld in COMMON_TLDS:
            if domain.endswith(tld) and not domain.endswith("." + tld):
                domain = domain[:-len(tld)] + "." + tld
                break
        domain_parts = domain.split('.', 1)
        if len(domain_parts) == 2:
            provider, tld = domain_parts
            # Fix provider (gmail, yahoo, ...)
            provider_matches = difflib.get_close_matches(provider, MAJOR_PROVIDERS, n=1, cutoff=0.7)
            if provider_matches:
                provider = provider_matches[0]
            # Fix TLD (com, net, org, ...)
            tld_matches = difflib.get_close_matches(tld, COMMON_TLDS, n=1, cutoff=0.6)
            if tld_matches:
                tld = tld_matches[0]
            domain = f"{provider}.{tld}"
        email = f"{username}@{domain}"
        
    is_fixed_valid = bool(EMAIL_REGEX.match(email))
    return email if is_fixed_valid else val, is_fixed_valid

def process_row_logic(row, email_cols):
    any_fixed = False
    any_failed = False
    
    for email_col in email_cols:
        val = row.get(email_col)
        original_val = str(val).strip() if pd.notna(val) and val is not None else ""
        
        if not original_val or is_valid_email(original_val):
            pass
        else:
            cleaned_val, is_fixed_valid = clean_and_fix_email(original_val)
            if is_fixed_valid:
                any_fixed = True
                row[email_col] = cleaned_val
                
                cols_str = str(row.get("error_column", "")) if pd.notna(row.get("error_column")) else ""
                msgs_str = str(row.get("error_message", "")) if pd.notna(row.get("error_message")) else ""
                
                if email_col in cols_str:
                    msgs = msgs_str.split("; ")
                    cols = cols_str.split("; ")
                    new_msgs = [f"[Fixed] {m}" if c == email_col and not m.startswith("[Fixed]") else m for c, m in zip(cols, msgs)]
                    row["error_message"] = "; ".join(new_msgs)
            else:
                any_failed = True
                row[email_col] = original_val 

    if any_fixed:
        row["email_fix_status"] = True
    elif any_failed:
        row["email_fix_status"] = False
    else:
        row["email_fix_status"] = None

    return row

def get_total_rows(file_path):
    with open(file_path, 'r', encoding='utf-8-sig', errors='ignore') as f:
        return sum(1 for _ in f) - 1

def main():
    root = Tk()
    root.withdraw()
    root.attributes('-topmost', True)
    print("="*60)
    print("📧 GLOBAL EMAIL CLEANER & AUDITOR (Optimized)")
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

    email_cols_input = input("\nEnter EMAIL columns (comma-separated): ").strip()
    email_cols = [c.strip() for c in email_cols_input.split(',')] if email_cols_input else []
    
    if not email_cols:
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
    
    temp_file = "temp_email_processing.csv"
    fixed_count, failed_count, null_count = 0, 0, 0

    try:
        with tqdm(total=total_rows, desc="Processing Rows", unit="row") as pbar:
            for i, chunk in enumerate(pd.read_csv(file_path, dtype=str, encoding='utf-8-sig', chunksize=CHUNK_SIZE)):
                
                missing_cols = [c for c in email_cols if c not in chunk.columns]
                if missing_cols:
                    print(f"\n❌ Error: Columns {missing_cols} not found in the file!")
                    return
                
                if "email_fix_status" not in chunk.columns:
                    chunk["email_fix_status"] = None
                
                records = chunk.to_dict('records')
                processed_records = []
                
                for row in records:
                    proc_row = process_row_logic(row, email_cols)
                    status = proc_row.get("email_fix_status")
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
            output_file = f"{base_name}_email_audited{ext}"
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