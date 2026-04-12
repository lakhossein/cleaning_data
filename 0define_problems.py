import os
import re
import csv
import logging
from io import StringIO
from datetime import datetime
from tkinter import Tk
from tkinter.filedialog import askopenfilename

import pandas as pd
from tqdm import tqdm

# ==========================================
# CONFIG
# ==========================================
CONFIG = {
    "CHUNK_SIZE": 5000,
    "ENABLE_LOG": True,
}

# ==========================================
# LOGGING SETUP
# ==========================================
if CONFIG["ENABLE_LOG"]:
    logging.basicConfig(
        filename="errors.log",
        level=logging.INFO, 
        format="%(asctime)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

def write_log(msg):
    if CONFIG["ENABLE_LOG"]:
        logging.info(msg)

# ==========================================
# REGEX PATTERNS
# ==========================================
EMAIL_REGEX = re.compile(r"^[\w\.-]+@[\w\.-]+\.\w+$")

# ==========================================
# FILE READER
# ==========================================
def read_file_safe(path):
    encodings = ["utf-8", "latin-1", "cp1252", "utf-8-sig"]
    for enc in encodings:
        try:
            with open(path, "r", encoding=enc) as f:
                return f.read()
        except UnicodeDecodeError:
            continue
    raise Exception("Could not read the file with the provided encodings.")

# ==========================================
# TEXT CLEANER 
# ==========================================
def clean_text(val):
    if val is None:
        return None
    val = str(val)
    val = re.sub(r'\s+', ' ', val)
    return val.strip()

# ==========================================
# SMART ROW EXTRACTOR (FOR SQL)
# ==========================================
def extract_rows_from_values(values_block):
    rows = []
    in_string = False
    escape_next = False
    start_idx = -1

    for i, char in enumerate(values_block):
        if escape_next:
            escape_next = False
            continue
        if char == '\\':
            escape_next = True
            continue
        if char == "'":
            in_string = not in_string
            continue

        if not in_string:
            if char == '(':
                if start_idx == -1:
                    start_idx = i + 1
            elif char == ')':
                if start_idx != -1:
                    row_str = values_block[start_idx:i]
                    rows.append(row_str)
                    start_idx = -1
                    
    return rows

# ==========================================
# SQL STREAM PARSER
# ==========================================
def parse_sql_stream(content):
    pattern = r"INSERT INTO\s+`?(\w+)`?\s*\((.*?)\)\s*VALUES\s*(.*?);"
    inserts = re.finditer(pattern, content, re.S | re.I)

    for match in inserts:
        table_name = match.group(1)
        columns = [c.strip().replace("`", "") for c in match.group(2).split(",")]
        values_block = match.group(3)

        rows = extract_rows_from_values(values_block)
        
        row_number = 0
        for row_str in rows:
            row_number += 1
            reader = csv.reader(StringIO(row_str), quotechar="'", escapechar='\\', skipinitialspace=True)
            try:
                parsed_row = next(reader)
            except StopIteration:
                continue

            cleaned = []
            for v in parsed_row:
                v_clean = clean_text(v)
                if v_clean.upper() == "NULL":
                    cleaned.append(None)
                else:
                    cleaned.append(v_clean)

            yield table_name, columns, cleaned, row_number

# ==========================================
# UNIVERSAL DATA ROUTER
# ==========================================
def get_data_stream(file_path, file_type, delimiter=','):
    base_name = os.path.splitext(os.path.basename(file_path))[0]
    safe_table_name = re.sub(r'\W+', '_', base_name)

    # تابعی برای ذخیره ردیف‌های متلاشی‌شده توسط پانداز
    def bad_line_handler(bad_line):
        with open("skipped_lines_log.txt", "a", encoding="utf-8-sig") as f:
            f.write(delimiter.join(bad_line) + "\n")
        # برگرداندن None باعث می‌شود پانداز این ردیف را نادیده بگیرد تا برنامه متوقف نشود
        return None

    if file_type == 'sql':
        content = read_file_safe(file_path)
        yield from parse_sql_stream(content)

    elif file_type in ['csv', 'txt']:
        try:
            df = pd.read_csv(
                file_path, 
                sep=delimiter, 
                dtype=str, 
                keep_default_na=False, 
                engine='python',
                on_bad_lines=bad_line_handler
            )
        except UnicodeDecodeError:
            df = pd.read_csv(
                file_path, 
                sep=delimiter, 
                dtype=str, 
                encoding='latin-1', 
                keep_default_na=False, 
                engine='python',
                on_bad_lines=bad_line_handler
            )
            
        columns = list(df.columns)
        for i, row in enumerate(df.values):
            cleaned = [clean_text(v) if str(v).strip() != "" else None for v in row]
            yield safe_table_name, columns, cleaned, i + 1

    elif file_type == 'xlsx':
        df = pd.read_excel(file_path, dtype=str)
        df = df.fillna("") 
        columns = list(df.columns)
        for i, row in enumerate(df.values):
            cleaned = [clean_text(v) if str(v).strip() != "" else None for v in row]
            yield safe_table_name, columns, cleaned, i + 1

    elif file_type == 'json':
        df = pd.read_json(file_path, dtype=str)
        df = df.fillna("")
        columns = list(df.columns)
        for i, row in enumerate(df.values):
            cleaned = [clean_text(v) if str(v).strip() != "" else None for v in row]
            yield safe_table_name, columns, cleaned, i + 1

# ==========================================
# ALIGN ROW & SCHEMA
# ==========================================
def update_schema_ordered(existing_cols, new_cols):
    for col in new_cols:
        if col not in existing_cols:
            existing_cols.append(col)

def align_row(columns, row, unified_columns):
    row_dict = dict(zip(columns, row))
    return [row_dict.get(col, None) for col in unified_columns]

# ==========================================
# VALIDATION & TRANSFORMATION
# ==========================================
def validate_and_transform_row(row, columns, seen_hashes, rules, date_format):
    errors = []
    null_logs = []
    fake_nulls = ["N/A", "NA", "-", "NULL", "NONE", ""]

    for idx, col_name in enumerate(columns):
        val = row[idx]
        
        # 1. Null Handling
        if val is None or str(val).upper() in fake_nulls:
            null_logs.append(f"Column '{col_name}' is Empty.")
            row[idx] = None 
            continue 
            
        str_val = str(val).strip()

        # 2. Date and Datetime Validation
        is_date = col_name in rules.get("date", set())
        is_datetime = col_name in rules.get("datetime", set())
        
        if is_date or is_datetime:
            raw_val = str(val).strip()
            
            has_letters = any(c.isalpha() for c in raw_val)
            parts = raw_val.split()
            has_time = ":" in raw_val or len(parts) > 1
            has_persian_digits = any(c in raw_val for c in "۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩")

            test_val = re.sub(r'[/.]', '-', parts[0] if parts else "").strip()
            
            formats = []
            if date_format == '1':   formats = ["%d-%m-%Y", "%d-%m-%y"]
            elif date_format == '2': formats = ["%m-%d-%Y", "%m-%d-%y"]
            elif date_format == '3': formats = ["%Y-%m-%d", "%y-%m-%d"]
            else: formats = ["%Y-%m-%d", "%d-%m-%Y", "%m-%d-%Y"]

            is_perfect_gregorian = False
            if not has_letters and not has_persian_digits:
                for fmt in formats:
                    try:
                        dt = datetime.strptime(test_val, fmt)
                        if dt.year > 1500:
                            is_perfect_gregorian = True
                            if is_datetime:
                                time_match = re.search(r'(\d{1,2}:\d{2}(?::\d{2})?)', raw_val)
                                t_str = time_match.group(1) if time_match else "00:00:00"
                                if len(t_str.split(':')) == 2: t_str += ":00"
                                row[idx] = dt.strftime(f"%Y-%m-%d {t_str}")
                            else:
                                row[idx] = dt.strftime("%Y-%m-%d")
                            break
                    except: continue

            if not is_perfect_gregorian:
                issue_tags = []
                if has_time and is_date: issue_tags.append("TimeDetected")
                if not has_time and is_datetime: issue_tags.append("MissingTime")
                if has_letters: issue_tags.append("MixedContent")
                if has_persian_digits or (re.search(r'(13\d{2}|14\d{2})', raw_val)): issue_tags.append("Shamsi/Persian")
                
                error_msg = f"Structure Issue ({', '.join(issue_tags)}): {raw_val}"
                errors.append(("DATE", col_name, error_msg))
            
            continue

        # 3. Numeric Validation
        if col_name in rules.get("numeric", set()):
            cleaned_num = str_val.replace(" ", "").replace("-", "")
            check_val = cleaned_num.replace("+", "") 
            
            if not check_val.isdigit():
                errors.append(("NUMBER", col_name, f"expected numbers but got: {val}"))
            else:
                row[idx] = cleaned_num

        # 4. Alphabetic Validation
        if col_name in rules.get("alpha", set()):
            clean_alpha = str_val.replace(" ", "").replace("-", "").replace(".", "")
            if not clean_alpha.isalpha():
                errors.append(("type_error", col_name, f"expected only letters but got: {val}"))

        # 5. Email Validation
        if col_name in rules.get("email", set()):
            if not EMAIL_REGEX.match(str_val):
                errors.append(("EMAIL", col_name, f"invalid email format: {val}"))

    new_row_tuple = tuple(str(x) for x in row)
    new_row_hash = hash(new_row_tuple)

    if new_row_hash in seen_hashes:
        errors.append(("duplicate_row", "ALL", "This row is an exact duplicate of a previous row."))

    return errors, null_logs, new_row_hash

# ==========================================
# TERMINAL INTERACTION HELPERS
# ==========================================
def ask_user_for_columns(prompt_text, all_cols):
    print(f"\n💡 {prompt_text}")
    user_input = input("Enter NUMBERS (comma-separated), or press Enter to skip: ")
    selected = set()
    
    if user_input.strip():
        parts = [p.strip() for p in user_input.split(",")]
        for p in parts:
            if p.isdigit():
                idx = int(p) - 1
                if 0 <= idx < len(all_cols):
                    selected.add(all_cols[idx])
            else:
                if p in all_cols:
                    selected.add(p)
                    
    if selected:
        print(f"✅ Selected: {', '.join(selected)}")
    else:
        print("⏭️ Skipped.")
    return selected

def ask_date_format():
    print("\n📅 Date format detected in the selected columns:")
    print("[1] Day first   (e.g., 31/12/2023 or 31-12-2023)")
    print("[2] Month first (e.g., 12/31/2023 or 12-31-2023)")
    print("[3] Year first  (e.g., 2023/12/31 or 2023-12-31)")
    choice = input("Enter the number (1, 2, or 3) indicating the raw data format: ").strip()
    if choice not in ['1', '2', '3']:
        print("Invalid choice, defaulting to Year first [3].")
        return '3'
    return choice

# ==========================================
# MAIN PROCESS
# ==========================================
def main(input_file, file_type, delimiter=','):
    # پاک کردن فایل لاگ ردیف‌های خراب از اجراهای قبلی (اگر وجود داشته باشد)
    if os.path.exists("skipped_lines_log.txt"):
        os.remove("skipped_lines_log.txt")

    print(f"\nReading {file_type.upper()} file...")
    
    stream = get_data_stream(input_file, file_type, delimiter)
    
    tables = {}
    all_unique_cols = set()

    print("Building table structure...")
    for table_name, columns, row, row_number in stream:
        if table_name not in tables:
            tables[table_name] = {"columns": [], "rows": [], "seen_hashes": set()}
            
        update_schema_ordered(tables[table_name]["columns"], columns)
        tables[table_name]["rows"].append((columns, row, row_number))
        all_unique_cols.update(columns)

    all_unique_cols = sorted(list(all_unique_cols))
    
    if not all_unique_cols:
        print("No columns detected! Check the input file format or delimiter.")
        return

    # ---------------------------------------------------------
    # بخش تعاملی
    # ---------------------------------------------------------
    print("\n" + "="*50)
    print("🔍 DETECTED COLUMNS:")
    for i, col in enumerate(all_unique_cols):
        print(f"[{i + 1}] {col}")
    print("="*50)
    
    rules = {
        "numeric": ask_user_for_columns("Which columns must contain ONLY NUMBERS? (e.g. phone, ID)", all_unique_cols),
        "alpha": ask_user_for_columns("Which columns must contain ONLY LETTERS? (e.g. first_name, last_name)", all_unique_cols),
        "email": ask_user_for_columns("Which columns contain EMAILS?", all_unique_cols),
        "date": ask_user_for_columns("Which columns contain DATES?", all_unique_cols),
        "datetime": ask_user_for_columns("Which columns contain DATE & TIME? (Will add 00:00:00 if time is missing)", all_unique_cols),
    }

    date_format_choice = '3'
    if rules["date"] or rules["datetime"]:
        date_format_choice = ask_date_format()
        
    print("\n" + "="*50 + "\n")
    # ---------------------------------------------------------

    # پردازش و تولید فایل نهایی
    for table_name, data in tables.items():
        unified_columns = data["columns"]
        seen_hashes = data["seen_hashes"]
        output_file = f"{table_name}_processed.csv"

        if os.path.exists(output_file):
            os.remove(output_file)

        header_written = False
        has_error = False
        rows = data["rows"]

        print(f"Processing data: {table_name}")

        for i in tqdm(range(0, len(rows), CONFIG["CHUNK_SIZE"])):
            chunk = rows[i:i + CONFIG["CHUNK_SIZE"]]
            aligned_chunk = []
            error_stage_col = []
            error_col_name_col = []
            error_msg_col = []

            for cols, row, row_num in chunk:
                aligned = align_row(cols, row, unified_columns)
                
                errors, null_logs, new_row_hash = validate_and_transform_row(
                    aligned, unified_columns, seen_hashes, rules, date_format_choice
                )
                seen_hashes.add(new_row_hash)

                if errors:
                    has_error = True

                stages = "; ".join([e[0] for e in errors])
                error_cols = "; ".join([e[1] for e in errors])
                messages = "; ".join([e[2] for e in errors])

                aligned_chunk.append(aligned)
                error_stage_col.append(stages)
                error_col_name_col.append(error_cols)
                error_msg_col.append(messages)
                
                if errors:
                    write_log(f"ERROR | Table: {table_name} | Row: {row_num} | Details: {messages}")
                    
                if null_logs:
                    for n_log in null_logs:
                        write_log(f"NULL LOG | Table: {table_name} | Row: {row_num} | Details: {n_log}")

            df = pd.DataFrame(aligned_chunk, columns=unified_columns)
            df["error_stage"] = error_stage_col
            df["error_column"] = error_col_name_col
            df["error_message"] = error_msg_col

            df.to_csv(
                output_file, mode="a", index=False, header=not header_written, encoding="utf-8-sig"
            )
            header_written = True

        print(f"\n📊 STATISTICS FOR {table_name}:")
        print(f"   📥 Input Rows: {len(rows)}")
        print(f"   📤 Output Rows: {len(rows)}")
        print(f"   ⚖️ Difference (Deleted Rows): 0")
        print(f"Saved: {output_file}\n")
        
    print("Process Finished! 🎉")
    print("- Check the generated CSVs for your clean data.")
    print("- Check 'errors.log' for structural/typing errors.")
    if os.path.exists("skipped_lines_log.txt"):
        print("- ⚠️ NOTE: Some completely broken rows were skipped. Check 'skipped_lines_log.txt'.")

# ==========================================
# ENTRY POINT
# ==========================================
if __name__ == "__main__":
    root = Tk()
    root.withdraw()
    root.attributes('-topmost', True)
    
    print("="*50)
    print("📂 SELECT FILE FORMAT")
    print("="*50)
    print("[1] CSV (.csv)")
    print("[2] TXT (.txt)")
    print("[3] SQL (.sql)")
    print("[4] XLSX (.xlsx)")
    print("[5] JSON (.json)")
    
    file_type_map = {'1': 'csv', '2': 'txt', '3': 'sql', '4': 'xlsx', '5': 'json'}
    file_type = None
    
    while True:
        choice = input("\nEnter the number of the file format (1-5): ").strip()
        if choice in file_type_map:
            file_type = file_type_map[choice]
            break
        print("❌ Invalid choice! Please enter a number between 1 and 5.")

    delimiter = ','
    if file_type in ['txt', 'csv']:
        print("\n" + "="*50)
        print("🔀 SELECT DELIMITER")
        print("="*50)
        print("[1] Comma (,)")
        print("[2] Tab (\\t)")
        print("[3] Pipe (|)")
        print("[4] Semicolon (;)")
        print("[5] Custom...")
        
        while True:
            delim_choice = input("\nEnter the number for the delimiter (1-5): ").strip()
            if delim_choice == '1':
                delimiter = ','
                break
            elif delim_choice == '2':
                delimiter = '\t'
                break
            elif delim_choice == '3':
                delimiter = '|'
                break
            elif delim_choice == '4':
                delimiter = ';'
                break
            elif delim_choice == '5':
                delimiter = input("Enter your custom delimiter character: ")
                break
            else:
                print("❌ Invalid choice! Please enter a number between 1 and 5.")

    file_path = askopenfilename(
        parent=root,
        title=f"Select {file_type.upper()} File",
        filetypes=[(f"{file_type.upper()} Files", f"*.{file_type}"), ("All Files", "*.*")]
    )

    root.destroy()

    if not file_path:
        print("No file selected. Exiting...")
        exit(1)

    main(file_path, file_type, delimiter)