import os
import sys
import subprocess
import shutil
from tkinter import Tk
from tkinter.filedialog import askopenfilename

def run_pipeline():
    Tk().withdraw()
    
    print("="*60)
    print("🚀 STARTING THE AUTOMATED DATA ETL PIPELINE")
    print("="*60)
    
    # ==========================================
    # مرحله ۰: دریافت نام فایل اولیه
    # ==========================================
    print("\n[INIT] To ensure the final filename and folder match exactly, please select your ORIGINAL raw data file first:")
    original_file_path = askopenfilename(
        title="Select ORIGINAL Raw Data File",
        filetypes=[("All Files", "*.*")]
    )
    
    if not original_file_path:
        print("❌ No file selected. Pipeline stopped.")
        return
        
    original_dir = os.path.dirname(original_file_path)
    original_name = os.path.basename(original_file_path)
    
    # استخراج نام فایل بدون پسوند برای ساخت پوشه
    original_name_no_ext = os.path.splitext(original_name)[0]
    
    print(f"\n✅ Target recorded - Folder & File will be named: '{original_name_no_ext}'")

    # ==========================================
    # مرحله ۱: شناسایی مشکلات و پارس اولیه
    # ==========================================
    print("\n" + "-"*60)
    print("\n[STEP 1] Running 0define_problems.py...")
    print(f"💡 Hint: Please select '{original_name}' again in the file dialog.")
    
    step1 = subprocess.run([sys.executable, "0define_problems.py"])
    if step1.returncode != 0:
        print("\n❌ Step 1 failed or was interrupted. Pipeline stopped.")
        return
    print("\n✅ Step 1 Completed.")

    # ==========================================
    # مرحله ۲: اصلاح شماره موبایل
    # ==========================================
    print("\n" + "-"*60)
    print("\n[STEP 2] Running 1phone_fixer.py...")
    print("💡 Hint: Select the '*_processed.csv' generated in Step 1.")
    print("🔥 BEST PRACTICE: Choose '[1] Overwrite' at the end to keep things clean.")
    
    step2 = subprocess.run([sys.executable, "1phone_fixer.py"])
    if step2.returncode != 0:
        print("\n❌ Step 2 failed or was interrupted. Pipeline stopped.")
        return
    print("\n✅ Step 2 Completed.")

    # ==========================================
    # مرحله ۳: اصلاح ایمیل
    # ==========================================
    print("\n" + "-"*60)
    print("\n[STEP 3] Running 2email_fixer.py...")
    print("💡 Hint: Select the same file you just updated in Step 2.")
    print("🔥 BEST PRACTICE: Choose '[1] Overwrite' at the end.")
    
    step3 = subprocess.run([sys.executable, "2email_fixer.py"])
    if step3.returncode != 0:
        print("\n❌ Step 3 failed or was interrupted. Pipeline stopped.")
        return
    print("\n✅ Step 3 Completed.")

    # ==========================================
    # مرحله ۴: اصلاح تاریخ
    # ==========================================
    print("\n" + "-"*60)
    print("\n[STEP 4] Running 3date_fixer.py...")
    print("💡 Hint: Select the same file you just updated in Step 3.")
    print("🔥 BEST PRACTICE: Choose '[1] Overwrite' at the end.")
    
    step4 = subprocess.run([sys.executable, "3date_fixer.py"])
    if step4.returncode != 0:
        print("\n❌ Step 4 failed or was interrupted. Pipeline stopped.")
        return
    print("\n✅ Step 4 Completed.")

    # ==========================================
    # خروجی نهایی با نام دقیق پوشه و فایل ورودی
    # ==========================================
    print("\n" + "="*60)
    print("🎯 FINALIZING FOLDER AND FILE NAME")
    print("="*60)
    print("Please select the FINAL processed CSV file (the output from Step 4):")
    
    final_processed_path = askopenfilename(
        title="Select FINAL Processed File from Step 4",
        filetypes=[("CSV Files", "*.csv"), ("All Files", "*.*")]
    )
    
    if not final_processed_path:
        print("❌ No final file selected. You can rename your processed file manually later.")
        return
        
    output_dir = os.path.join(original_dir, original_name_no_ext)
    os.makedirs(output_dir, exist_ok=True)
    
    final_destination = os.path.join(output_dir, original_name_no_ext + ".csv")
    
    try:
        shutil.copy2(final_processed_path, final_destination)
        
        print("\n🎉 PIPELINE FULLY EXECUTED SUCCESSFULLY! 🎉")
        print(f"📂 Created Folder: {output_dir}")
        print(f"💾 Saved File:     {final_destination}")
        print("\n(Note: Your original raw file remains safe and untouched).")
        print("="*60)
    except Exception as e:
        print(f"\n❌ Could not move the final file automatically: {e}")

if __name__ == "__main__":
    run_pipeline()